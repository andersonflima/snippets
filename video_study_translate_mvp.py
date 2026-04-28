#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

OPENAI_API_BASE = "https://api.openai.com/v1"
ELEVENLABS_API_BASE = "https://api.elevenlabs.io/v1"
ELEVENLABS_CLONE_SAFE_MODEL = "eleven_multilingual_v2"
ELEVENLABS_CLONE_SAFE_STABILITY = 0.78
ELEVENLABS_CLONE_SAFE_SIMILARITY = 0.9
ELEVENLABS_CLONE_SAFE_STYLE = 0.0
ELEVENLABS_CLONE_SAFE_SPEAKER_BOOST = False
ELEVENLABS_DUBBING_READY_STATUS = "dubbed"
ELEVENLABS_DUBBING_FAILED_STATUS = "failed"
TIMECODE_PATTERN = re.compile(
    r"^(?P<start>\d{2}:\d{2}:\d{2},\d{3})\s-->\s(?P<end>\d{2}:\d{2}:\d{2},\d{3})$"
)
WORD_PATTERN = re.compile(r"\w+", flags=re.UNICODE)
ZERO_CROSSINGS_RATE_PATTERN = re.compile(
    r"Zero crossings rate:\s*(?P<rate>\d+(?:\.\d+)?)"
)
SILENCE_START_PATTERN = re.compile(r"silence_start:\s*(?P<start>\d+(?:\.\d+)?)")
SILENCE_END_PATTERN = re.compile(r"silence_end:\s*(?P<end>\d+(?:\.\d+)?)")


@dataclass(frozen=True)
class SrtCue:
    index: int
    start: str
    end: str
    text: str


def parse_boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(
        f"Valor booleano invalido: {value!r}. Use true/false."
    )


def clamp_float(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def run_cmd(cmd: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
    )


def ensure_binary(binary_name: str) -> None:
    if shutil.which(binary_name) is None:
        raise RuntimeError(f"Binary '{binary_name}' nao encontrado no PATH.")


def extract_audio(video_path: Path, audio_path: Path, timeout_seconds: int) -> None:
    ensure_binary("ffmpeg")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(audio_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao extrair audio com ffmpeg.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def transcribe_audio_to_srt(
    audio_path: Path,
    api_key: str,
    source_language: str,
    transcribe_model: str,
    timeout_seconds: int,
) -> str:
    ensure_binary("curl")
    def parse_openai_error_body(raw_body: str) -> tuple[str | None, str | None]:
        if not raw_body.strip():
            return (None, None)
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            return (None, None)

        if not isinstance(payload, dict):
            return (None, None)

        error_payload = payload.get("error")
        if not isinstance(error_payload, dict):
            return (None, None)

        code = error_payload.get("code")
        message = error_payload.get("message")
        return (
            code if isinstance(code, str) else None,
            message if isinstance(message, str) else None,
        )

    def build_transcribe_cmd(model_name: str) -> list[str]:
        return [
            "curl",
            "-sS",
            "--fail-with-body",
            "-X",
            "POST",
            f"{OPENAI_API_BASE}/audio/transcriptions",
            "-H",
            f"Authorization: Bearer {api_key}",
            "-F",
            f"file=@{audio_path}",
            "-F",
            f"model={model_name}",
            "-F",
            "response_format=srt",
            "-F",
            f"language={source_language}",
        ]

    active_model = transcribe_model
    result = run_cmd(build_transcribe_cmd(active_model), timeout_seconds)

    if result.returncode != 0:
        first_body = result.stdout.strip()
        first_code, first_message = parse_openai_error_body(first_body)
        should_fallback_to_whisper = (
            active_model != "whisper-1"
            and first_code == "unsupported_value"
            and isinstance(first_message, str)
            and "response_format" in first_message
        )

        if should_fallback_to_whisper:
            print(
                (
                    "[pipeline] modelo de transcricao atual nao suporta SRT. "
                    "Aplicando fallback automatico para whisper-1."
                ),
                file=sys.stderr,
            )
            active_model = "whisper-1"
            result = run_cmd(build_transcribe_cmd(active_model), timeout_seconds)

        if result.returncode != 0:
            response_body = result.stdout.strip()
            body_excerpt = response_body[:2000] if response_body else ""
            raise RuntimeError(
                "Falha na transcricao via OpenAI.\n"
                f"model: {active_model}\n"
                f"response_format: srt\n"
                f"stderr: {result.stderr.strip()}\n"
                f"body: {body_excerpt}"
            )

    raw_output = result.stdout.strip()

    if not raw_output:
        raise RuntimeError("Transcricao retornou vazia.")

    if raw_output.startswith("{"):
        try:
            payload = json.loads(raw_output)
            error_payload = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error_payload, dict):
                error_message = error_payload.get("message")
                if isinstance(error_message, str) and error_message.strip():
                    raise RuntimeError(f"Erro retornado pela API de transcricao: {error_message}")
        except json.JSONDecodeError:
            pass

    return result.stdout


def normalize_srt_text(raw: str) -> str:
    return raw.replace("\r\n", "\n").replace("\r", "\n").strip()


def parse_srt(raw_srt: str) -> list[SrtCue]:
    normalized = normalize_srt_text(raw_srt)
    if not normalized:
        return []

    blocks = re.split(r"\n\s*\n", normalized)
    cues: list[SrtCue] = []

    for block in blocks:
        lines = [line for line in block.split("\n") if line.strip() != ""]
        if len(lines) < 3:
            continue

        index_line = lines[0].strip()
        timecode_line = lines[1].strip()
        text_lines = lines[2:]

        if not index_line.isdigit():
            continue

        time_match = TIMECODE_PATTERN.match(timecode_line)
        if not time_match:
            continue

        cue = SrtCue(
            index=int(index_line),
            start=time_match.group("start"),
            end=time_match.group("end"),
            text="\n".join(text_lines).strip(),
        )
        cues.append(cue)

    return cues


def render_srt(cues: Iterable[SrtCue]) -> str:
    blocks = [
        f"{cue.index}\n{cue.start} --> {cue.end}\n{cue.text}".strip()
        for cue in cues
    ]
    return "\n\n".join(blocks).strip() + "\n"


def chunk_cues(cues: list[SrtCue], max_items: int, max_chars: int) -> list[list[SrtCue]]:
    if max_items < 1 or max_chars < 1:
        raise ValueError("max_items e max_chars devem ser > 0.")

    chunks: list[list[SrtCue]] = []
    current_chunk: list[SrtCue] = []
    current_chars = 0

    for cue in cues:
        cue_size = len(cue.text)
        exceeds_item_limit = len(current_chunk) >= max_items
        exceeds_char_limit = (current_chars + cue_size) > max_chars and len(current_chunk) > 0

        if exceeds_item_limit or exceeds_char_limit:
            chunks.append(current_chunk)
            current_chunk = []
            current_chars = 0

        current_chunk.append(cue)
        current_chars += cue_size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def http_post_json(
    url: str,
    api_key: str,
    payload: dict[str, object],
    timeout_seconds: int,
    retry_attempts: int,
) -> dict[str, object]:
    encoded = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    for attempt in range(1, retry_attempts + 1):
        req = urllib.request.Request(url=url, data=encoded, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                response_text = response.read().decode("utf-8")
                return json.loads(response_text)
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            retriable = err.code in {408, 409, 429, 500, 502, 503, 504}
            if attempt < retry_attempts and retriable:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(
                f"Erro HTTP {err.code} na chamada OpenAI: {body}"
            ) from err
        except urllib.error.URLError as err:
            if attempt < retry_attempts:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"Erro de rede na chamada OpenAI: {err}") from err

    raise RuntimeError("Falha inesperada na chamada HTTP.")


def http_post_binary(
    url: str,
    api_key: str,
    payload: dict[str, object],
    timeout_seconds: int,
    retry_attempts: int,
) -> bytes:
    encoded = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    for attempt in range(1, retry_attempts + 1):
        req = urllib.request.Request(url=url, data=encoded, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                return response.read()
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            retriable = err.code in {408, 409, 429, 500, 502, 503, 504}
            if attempt < retry_attempts and retriable:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(
                f"Erro HTTP {err.code} na chamada OpenAI (binary): {body}"
            ) from err
        except urllib.error.URLError as err:
            if attempt < retry_attempts:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"Erro de rede na chamada OpenAI (binary): {err}") from err

    raise RuntimeError("Falha inesperada na chamada HTTP binary.")


def extract_chat_content(response_payload: dict[str, object]) -> str:
    choices = response_payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Resposta de chat sem 'choices'.")

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise RuntimeError("Resposta de chat invalida (choice).")

    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise RuntimeError("Resposta de chat invalida (message).")

    content = message.get("content")
    if not isinstance(content, str):
        raise RuntimeError("Resposta de chat invalida (content).")

    return content


def normalize_language_tag(language: str) -> str:
    return language.strip().lower().replace("_", "-")


def to_elevenlabs_language_code(language: str) -> str:
    normalized = normalize_language_tag(language)
    if normalized in {"", "auto", "source"}:
        return normalized or "auto"
    if normalized.startswith("pt"):
        return "pt"

    language_parts = [part for part in normalized.split("-") if part]
    if not language_parts:
        return "auto"
    code = language_parts[0]
    if re.fullmatch(r"[a-z]{2,3}", code):
        return code
    return "auto"


def elevenlabs_target_accent_from_language(language: str) -> str | None:
    normalized = normalize_language_tag(language)
    if normalized.startswith("pt-br"):
        return "brazilian"
    return None


def guess_media_content_type(media_path: Path) -> str:
    extension = media_path.suffix.strip().lower()
    mapping: dict[str, str] = {
        ".mp4": "video/mp4",
        ".m4v": "video/mp4",
        ".mov": "video/quicktime",
        ".mkv": "video/x-matroska",
        ".avi": "video/x-msvideo",
        ".webm": "video/webm",
        ".mpeg": "video/mpeg",
        ".mpg": "video/mpeg",
        ".wmv": "video/x-ms-wmv",
        ".aac": "audio/aac",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".flac": "audio/flac",
        ".ogg": "audio/ogg",
        ".m4a": "audio/mp4",
    }
    return mapping.get(extension, "video/mp4")


def elevenlabs_request_json(
    url: str,
    api_key: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> dict[str, object]:
    headers = {
        "Accept": "application/json",
        "xi-api-key": api_key,
    }
    for attempt in range(1, retry_attempts + 1):
        request = urllib.request.Request(url=url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(body)
                if not isinstance(parsed, dict):
                    raise RuntimeError(
                        f"Resposta JSON invalida da ElevenLabs (esperado objeto): {body[:800]}"
                    )
                return parsed
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            retriable = err.code in {408, 409, 425, 429, 500, 502, 503, 504}
            if attempt < retry_attempts and retriable:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(
                "Falha na chamada JSON da ElevenLabs.\n"
                f"url: {url}\n"
                f"http_status: {err.code}\n"
                f"body: {body[:2000]}"
            ) from err
        except urllib.error.URLError as err:
            if attempt < retry_attempts:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"Erro de rede na chamada JSON da ElevenLabs: {err}") from err

    raise RuntimeError("Falha inesperada em elevenlabs_request_json.")


def create_elevenlabs_dubbing_job(
    api_key: str,
    input_media_path: Path,
    source_language: str,
    target_language: str,
    timeout_seconds: int,
) -> str:
    ensure_binary("curl")

    target_code = to_elevenlabs_language_code(target_language)
    source_code = to_elevenlabs_language_code(source_language)
    target_accent = elevenlabs_target_accent_from_language(target_language)
    media_content_type = guess_media_content_type(input_media_path)
    media_filename = input_media_path.name

    cmd = [
        "curl",
        "-sS",
        "--fail-with-body",
        "-X",
        "POST",
        f"{ELEVENLABS_API_BASE}/dubbing",
        "-H",
        f"xi-api-key: {api_key}",
        "-H",
        "Accept: application/json",
        "-F",
        f"file=@{input_media_path};filename={media_filename};type={media_content_type}",
        "-F",
        f"source_lang={source_code}",
        "-F",
        f"target_lang={target_code}",
        "-F",
        "num_speakers=0",
        "-F",
        "watermark=false",
        "-F",
        "highest_resolution=true",
        "-F",
        "drop_background_audio=false",
    ]
    if target_accent is not None:
        cmd.extend(["-F", f"target_accent={target_accent}"])

    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao criar job de dubbing na ElevenLabs.\n"
            f"media_content_type: {media_content_type}\n"
            f"stderr: {result.stderr.strip()}\n"
            f"stdout: {result.stdout.strip()}"
        )

    try:
        parsed = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as err:
        raise RuntimeError(
            "Resposta invalida ao criar job de dubbing na ElevenLabs.\n"
            f"stdout: {result.stdout.strip()}"
        ) from err

    dubbing_id = parsed.get("dubbing_id")
    if not isinstance(dubbing_id, str) or not dubbing_id.strip():
        raise RuntimeError(f"ElevenLabs nao retornou dubbing_id: {parsed}")
    return dubbing_id.strip()


def get_elevenlabs_dubbing_metadata(
    api_key: str,
    dubbing_id: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> dict[str, object]:
    encoded_dubbing_id = urllib.parse.quote(dubbing_id.strip(), safe="")
    return elevenlabs_request_json(
        url=f"{ELEVENLABS_API_BASE}/dubbing/{encoded_dubbing_id}",
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )


def wait_for_elevenlabs_dubbing_completion(
    api_key: str,
    dubbing_id: str,
    timeout_seconds: int,
    retry_attempts: int,
    max_wait_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, object]:
    started_at = time.monotonic()
    wait_limit = max(30, max_wait_seconds)
    poll_step = max(1.0, poll_interval_seconds)
    attempts = 0

    while True:
        attempts += 1
        metadata = get_elevenlabs_dubbing_metadata(
            api_key=api_key,
            dubbing_id=dubbing_id,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
        )
        status_raw = metadata.get("status")
        status = (
            status_raw.strip().lower()
            if isinstance(status_raw, str) and status_raw.strip()
            else "unknown"
        )

        if status == ELEVENLABS_DUBBING_READY_STATUS:
            return metadata
        if status == ELEVENLABS_DUBBING_FAILED_STATUS:
            error_text = metadata.get("error")
            error_message = error_text.strip() if isinstance(error_text, str) else "sem detalhe"
            raise RuntimeError(
                "Dubbing na ElevenLabs falhou.\n"
                f"dubbing_id: {dubbing_id}\n"
                f"status: {status}\n"
                f"error: {error_message}"
            )

        elapsed = time.monotonic() - started_at
        if elapsed > wait_limit:
            raise RuntimeError(
                "Timeout aguardando job de dubbing na ElevenLabs.\n"
                f"dubbing_id: {dubbing_id}\n"
                f"status_atual: {status}\n"
                f"tempo_espera_segundos: {int(elapsed)}"
            )

        print(
            f"[elevenlabs] aguardando dubbing {dubbing_id} (status={status}, tentativa={attempts})",
            file=sys.stderr,
        )
        time.sleep(poll_step)


def download_elevenlabs_dubbed_media(
    api_key: str,
    dubbing_id: str,
    language_code: str,
    output_path: Path,
    timeout_seconds: int,
    retry_attempts: int,
) -> None:
    encoded_dubbing_id = urllib.parse.quote(dubbing_id.strip(), safe="")
    encoded_language = urllib.parse.quote(language_code.strip(), safe="")
    request_url = (
        f"{ELEVENLABS_API_BASE}/dubbing/{encoded_dubbing_id}/audio/{encoded_language}"
    )
    headers = {
        "xi-api-key": api_key,
        "Accept": "*/*",
    }

    for attempt in range(1, retry_attempts + 1):
        request = urllib.request.Request(url=request_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                output_path.write_bytes(response.read())
                return
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            retriable = err.code in {408, 409, 425, 429, 500, 502, 503, 504}
            if attempt < retry_attempts and retriable:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(
                "Falha ao baixar video dublado da ElevenLabs.\n"
                f"url: {request_url}\n"
                f"http_status: {err.code}\n"
                f"body: {body[:2000]}"
            ) from err
        except urllib.error.URLError as err:
            if attempt < retry_attempts:
                time.sleep(min(2**attempt, 8))
                continue
            raise RuntimeError(f"Erro de rede ao baixar dub da ElevenLabs: {err}") from err

    raise RuntimeError("Falha inesperada ao baixar dub da ElevenLabs.")


def fetch_elevenlabs_transcript_srt(
    api_key: str,
    dubbing_id: str,
    language_code: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> str:
    encoded_dubbing_id = urllib.parse.quote(dubbing_id.strip(), safe="")
    encoded_language = urllib.parse.quote(language_code.strip(), safe="")
    response_payload = elevenlabs_request_json(
        url=(
            f"{ELEVENLABS_API_BASE}/dubbing/"
            f"{encoded_dubbing_id}/transcripts/{encoded_language}/format/srt"
        ),
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )
    srt_text = response_payload.get("srt")
    if not isinstance(srt_text, str) or not srt_text.strip():
        raise RuntimeError(
            "Resposta da ElevenLabs sem campo 'srt' ao buscar transcript.\n"
            f"payload: {response_payload}"
        )
    return normalize_srt_text(srt_text) + "\n"


def run_elevenlabs_managed_dubbing(
    input_video: Path,
    output_dir: Path,
    base_name: str,
    source_language: str,
    target_language: str,
    api_key: str,
    timeout_seconds: int,
    retry_attempts: int,
    burn_subtitles_enabled: bool,
    max_wait_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, str]:
    source_srt_path = output_dir / f"{base_name}.{source_language}.srt"
    target_srt_path = output_dir / f"{base_name}.{target_language}.srt"
    subtitled_video_path = output_dir / f"{base_name}.{target_language}.hardsub.mp4"
    dubbed_video_path = output_dir / f"{base_name}.{target_language}.dub.mp4"
    raw_dubbed_video_path = output_dir / f"{base_name}.{target_language}.dub.raw.mp4"

    print("[pipeline] enviando video para dubbing gerenciado na ElevenLabs", file=sys.stderr)
    dubbing_id = create_elevenlabs_dubbing_job(
        api_key=api_key,
        input_media_path=input_video,
        source_language=source_language,
        target_language=target_language,
        timeout_seconds=timeout_seconds,
    )

    print(f"[pipeline] dubbing_id ElevenLabs: {dubbing_id}", file=sys.stderr)
    _ = wait_for_elevenlabs_dubbing_completion(
        api_key=api_key,
        dubbing_id=dubbing_id,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
        max_wait_seconds=max_wait_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )

    target_code = to_elevenlabs_language_code(target_language)
    print("[pipeline] baixando video dublado da ElevenLabs", file=sys.stderr)
    download_elevenlabs_dubbed_media(
        api_key=api_key,
        dubbing_id=dubbing_id,
        language_code=target_code,
        output_path=raw_dubbed_video_path,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )

    shutil.copyfile(raw_dubbed_video_path, dubbed_video_path)

    try:
        source_srt_path.write_text(
            fetch_elevenlabs_transcript_srt(
                api_key=api_key,
                dubbing_id=dubbing_id,
                language_code="source",
                timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts,
            ),
            encoding="utf-8",
        )
    except Exception as err:
        print(f"[pipeline] aviso: falha ao baixar SRT de origem ElevenLabs ({err})", file=sys.stderr)

    try:
        target_srt_path.write_text(
            fetch_elevenlabs_transcript_srt(
                api_key=api_key,
                dubbing_id=dubbing_id,
                language_code=target_code,
                timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts,
            ),
            encoding="utf-8",
        )
    except Exception as err:
        print(f"[pipeline] aviso: falha ao baixar SRT de destino ElevenLabs ({err})", file=sys.stderr)

    final_dubbed_video_path = dubbed_video_path
    if burn_subtitles_enabled and target_srt_path.exists():
        print("[pipeline] aplicando hardsub no video dublado ElevenLabs", file=sys.stderr)
        burn_subtitles(
            video_path=dubbed_video_path,
            subtitle_path=target_srt_path,
            output_video_path=subtitled_video_path,
            timeout_seconds=timeout_seconds,
        )
        final_dubbed_video_path = subtitled_video_path

    outputs: dict[str, str] = {
        "tts_provider": "elevenlabs",
        "tts_model": "elevenlabs_dubbing_api",
        "tts_voice": "managed_by_elevenlabs",
        "elevenlabs_dubbing_id": dubbing_id,
        "dubbed_video": str(final_dubbed_video_path.resolve()),
    }

    if source_srt_path.exists():
        outputs["source_srt"] = str(source_srt_path.resolve())
    if target_srt_path.exists():
        outputs["target_srt"] = str(target_srt_path.resolve())
    if burn_subtitles_enabled and target_srt_path.exists():
        outputs["subtitled_video"] = str(subtitled_video_path.resolve())

    return outputs


def is_target_pt_br(language: str) -> bool:
    normalized = normalize_language_tag(language)
    return normalized in {"pt-br", "ptbr", "portuguese-brazil", "portuguese (brazil)"}


def build_locale_translation_rules(target_language: str) -> list[str]:
    if not is_target_pt_br(target_language):
        return []

    return [
        "use apenas portugues brasileiro (pt-BR), nunca portugues europeu",
        "prefira pronome voce em vez de tu",
        "evite termos de Portugal como autocarro, telemovel, ficheiro, ecrã, comboio e fixe",
        "use termos do Brasil como onibus, celular, arquivo, tela, trem e legal",
    ]


def normalize_pt_br_text(text: str) -> str:
    normalized = text
    replacements: list[tuple[str, str]] = [
        (r"\bautocarros\b", "ônibus"),
        (r"\bautocarro\b", "ônibus"),
        (r"\btelem[oó]veis\b", "celulares"),
        (r"\btelem[oó]vel\b", "celular"),
        (r"\bficheiros\b", "arquivos"),
        (r"\bficheiro\b", "arquivo"),
        (r"\becr[aã]s\b", "telas"),
        (r"\becr[aã]\b", "tela"),
        (r"\bcomboios\b", "trens"),
        (r"\bcomboio\b", "trem"),
        (r"\bfixes\b", "legais"),
        (r"\bfixe\b", "legal"),
        (r"\bpequeno-alm[oó]ço\b", "café da manhã"),
        (r"\bcami[aã]o\b", "caminhão"),
        (r"\bcami[aã]oes\b", "caminhões"),
    ]
    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    return normalized


def request_translation_batch(
    batch: list[SrtCue],
    source_language: str,
    target_language: str,
    api_key: str,
    translate_model: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> dict[int, str]:
    input_rows = [{"id": cue.index, "text": cue.text} for cue in batch]

    locale_rules = build_locale_translation_rules(target_language)

    system_prompt = (
        "Voce e um tradutor tecnico profissional de conteudo educacional. "
        "Traduza com fidelidade sem resumir ideias. Preserve termos tecnicos, nomes proprios, "
        "siglas e blocos de codigo quando necessario. "
        "Otimize a fluidez para dublagem: frases naturais, objetivas e com duracao proxima ao original."
    )

    user_prompt = {
        "task": "translate_srt_batch",
        "source_language": source_language,
        "target_language": target_language,
        "rules": [
            "nao altere ids",
            "retorne JSON valido",
            "mantenha quebras de linha quando fizer sentido",
            "nao inclua explicacoes extras",
            "evite expandir desnecessariamente o tamanho das frases",
            "prefira redacao compacta para sincronismo de dublagem",
            *locale_rules,
        ],
        "rows": input_rows,
        "output_schema": {
            "translations": [{"id": 1, "text": "texto traduzido"}],
        },
    }

    payload = {
        "model": translate_model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ],
    }

    response_payload = http_post_json(
        url=f"{OPENAI_API_BASE}/chat/completions",
        api_key=api_key,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )

    content = extract_chat_content(response_payload)
    parsed = json.loads(content)

    translations = parsed.get("translations")
    if not isinstance(translations, list):
        raise RuntimeError("Resposta sem campo 'translations'.")

    translated_map: dict[int, str] = {}
    for item in translations:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        item_text = item.get("text")
        if isinstance(item_id, int) and isinstance(item_text, str):
            normalized_text = item_text.strip()
            if is_target_pt_br(target_language):
                normalized_text = normalize_pt_br_text(normalized_text)
            translated_map[item_id] = normalized_text

    expected_ids = {cue.index for cue in batch}
    got_ids = set(translated_map.keys())

    if expected_ids != got_ids:
        missing = sorted(expected_ids - got_ids)
        extra = sorted(got_ids - expected_ids)
        raise RuntimeError(
            "Batch de traducao inconsistente. "
            f"missing_ids={missing} extra_ids={extra}"
        )

    return translated_map


def request_pt_br_localization_batch(
    batch: list[SrtCue],
    api_key: str,
    translate_model: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> dict[int, str]:
    input_rows = [{"id": cue.index, "text": cue.text} for cue in batch]

    system_prompt = (
        "Voce e um revisor linguistico de dublagem. "
        "Converta o texto para portugues brasileiro (pt-BR), mantendo sentido tecnico, clareza e fluidez. "
        "Nao use portugues europeu."
    )

    user_prompt = {
        "task": "localize_to_pt_br",
        "rules": [
            "nao altere ids",
            "retorne JSON valido",
            "nao inclua explicacoes extras",
            "mantenha termos tecnicos corretos",
            "use somente portugues brasileiro",
            "evite termos de Portugal e prefira termos do Brasil",
            "mantenha o texto compacto para dublagem",
        ],
        "rows": input_rows,
        "output_schema": {
            "translations": [{"id": 1, "text": "texto em pt-BR"}],
        },
    }

    payload = {
        "model": translate_model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ],
    }

    response_payload = http_post_json(
        url=f"{OPENAI_API_BASE}/chat/completions",
        api_key=api_key,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )

    content = extract_chat_content(response_payload)
    parsed = json.loads(content)
    translations = parsed.get("translations")
    if not isinstance(translations, list):
        raise RuntimeError("Resposta sem campo 'translations' na localizacao pt-BR.")

    localized_map: dict[int, str] = {}
    for item in translations:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        item_text = item.get("text")
        if isinstance(item_id, int) and isinstance(item_text, str):
            localized_map[item_id] = normalize_pt_br_text(item_text.strip())

    expected_ids = {cue.index for cue in batch}
    got_ids = set(localized_map.keys())
    if expected_ids != got_ids:
        missing = sorted(expected_ids - got_ids)
        extra = sorted(got_ids - expected_ids)
        raise RuntimeError(
            "Batch de localizacao pt-BR inconsistente. "
            f"missing_ids={missing} extra_ids={extra}"
        )

    return localized_map


def localize_cues_to_pt_br(
    cues: list[SrtCue],
    api_key: str,
    translate_model: str,
    timeout_seconds: int,
    retry_attempts: int,
    max_batch_items: int,
    max_batch_chars: int,
) -> list[SrtCue]:
    if not cues:
        return cues

    cue_chunks = chunk_cues(cues, max_items=max_batch_items, max_chars=max_batch_chars)
    localized_cues: list[SrtCue] = []

    for chunk_index, chunk in enumerate(cue_chunks, start=1):
        print(
            f"[translate] revisao pt-BR lote {chunk_index}/{len(cue_chunks)} ({len(chunk)} cues)",
            file=sys.stderr,
        )
        try:
            localized_map = request_pt_br_localization_batch(
                batch=chunk,
                api_key=api_key,
                translate_model=translate_model,
                timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts,
            )
            localized_cues.extend(
                [
                    SrtCue(
                        index=cue.index,
                        start=cue.start,
                        end=cue.end,
                        text=localized_map[cue.index],
                    )
                    for cue in chunk
                ]
            )
        except Exception as err:
            print(
                f"[translate] revisao pt-BR com falha ({err}). Mantendo texto atual do lote.",
                file=sys.stderr,
            )
            localized_cues.extend(chunk)

    return localized_cues


def request_translation_single(
    cue: SrtCue,
    source_language: str,
    target_language: str,
    api_key: str,
    translate_model: str,
    timeout_seconds: int,
    retry_attempts: int,
) -> str:
    translated = request_translation_batch(
        batch=[cue],
        source_language=source_language,
        target_language=target_language,
        api_key=api_key,
        translate_model=translate_model,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )
    return translated[cue.index]


def translate_cues(
    cues: list[SrtCue],
    source_language: str,
    target_language: str,
    api_key: str,
    translate_model: str,
    timeout_seconds: int,
    retry_attempts: int,
    max_batch_items: int,
    max_batch_chars: int,
) -> list[SrtCue]:
    cue_chunks = chunk_cues(cues, max_items=max_batch_items, max_chars=max_batch_chars)
    translated_cues: list[SrtCue] = []

    for chunk_index, chunk in enumerate(cue_chunks, start=1):
        print(
            f"[translate] lote {chunk_index}/{len(cue_chunks)} ({len(chunk)} cues)",
            file=sys.stderr,
        )
        try:
            translated_map = request_translation_batch(
                batch=chunk,
                source_language=source_language,
                target_language=target_language,
                api_key=api_key,
                translate_model=translate_model,
                timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts,
            )
            translated_cues.extend(
                [
                    SrtCue(
                        index=cue.index,
                        start=cue.start,
                        end=cue.end,
                        text=translated_map[cue.index],
                    )
                    for cue in chunk
                ]
            )
        except Exception as err:
            print(
                f"[translate] lote com falha ({err}). Aplicando fallback cue a cue.",
                file=sys.stderr,
            )
            translated_cues.extend(
                [
                    SrtCue(
                        index=cue.index,
                        start=cue.start,
                        end=cue.end,
                        text=request_translation_single(
                            cue=cue,
                            source_language=source_language,
                            target_language=target_language,
                            api_key=api_key,
                            translate_model=translate_model,
                            timeout_seconds=timeout_seconds,
                            retry_attempts=retry_attempts,
                        ),
                    )
                    for cue in chunk
                ]
            )

    if is_target_pt_br(target_language):
        translated_cues = localize_cues_to_pt_br(
            cues=translated_cues,
            api_key=api_key,
            translate_model=translate_model,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            max_batch_items=max_batch_items,
            max_batch_chars=max_batch_chars,
        )

    return translated_cues


def srt_timestamp_to_seconds(timestamp: str) -> float:
    hours_str, minutes_str, rest = timestamp.split(":")
    seconds_str, millis_str = rest.split(",")
    return (
        int(hours_str) * 3600
        + int(minutes_str) * 60
        + int(seconds_str)
        + (int(millis_str) / 1000)
    )


def count_words(text: str) -> int:
    return len(WORD_PATTERN.findall(text))


def estimate_speech_rate_wpm(cues: list[SrtCue]) -> float | None:
    if not cues:
        return None

    total_duration = sum(
        max(0.0, srt_timestamp_to_seconds(cue.end) - srt_timestamp_to_seconds(cue.start))
        for cue in cues
    )
    if total_duration <= 0:
        return None

    total_words = sum(count_words(cue.text) for cue in cues)
    if total_words <= 0:
        return None

    words_per_minute = total_words / (total_duration / 60)
    return max(90.0, min(words_per_minute, 190.0))


def ffprobe_duration(media_path: Path, timeout_seconds: int) -> float:
    ensure_binary("ffprobe")
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(media_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao obter duracao com ffprobe.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )
    try:
        return float(result.stdout.strip())
    except ValueError as err:
        raise RuntimeError(f"Duracao invalida retornada por ffprobe: {result.stdout!r}") from err


def build_atempo_filters(speed_factor: float) -> list[str]:
    if speed_factor <= 0:
        raise ValueError("speed_factor deve ser > 0")

    factors: list[float] = []
    remaining = speed_factor

    while remaining > 2.0:
        factors.append(2.0)
        remaining /= 2.0

    while remaining < 0.5:
        factors.append(0.5)
        remaining /= 0.5

    factors.append(remaining)

    return [f"atempo={factor:.6f}" for factor in factors if abs(factor - 1.0) > 0.01]


def synthesize_tts_audio_openai(
    text: str,
    api_key: str,
    tts_model: str,
    tts_voice: str,
    tts_instructions: str,
    output_path: Path,
    timeout_seconds: int,
    retry_attempts: int,
) -> None:
    payload: dict[str, object] = {
        "model": tts_model,
        "voice": tts_voice,
        "input": text,
        "response_format": "mp3",
    }

    if tts_instructions.strip():
        payload["instructions"] = tts_instructions

    binary_audio = http_post_binary(
        url=f"{OPENAI_API_BASE}/audio/speech",
        api_key=api_key,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )

    output_path.write_bytes(binary_audio)


def synthesize_tts_audio_elevenlabs(
    text: str,
    api_key: str,
    voice_id: str,
    model_id: str,
    stability: float,
    similarity_boost: float,
    style: float,
    use_speaker_boost: bool,
    output_path: Path,
    timeout_seconds: int,
    retry_attempts: int,
    allow_model_fallback: bool = True,
) -> str:
    escaped_voice_id = urllib.parse.quote(voice_id.strip(), safe="")
    request_url = (
        f"{ELEVENLABS_API_BASE}/text-to-speech/{escaped_voice_id}"
        "?output_format=mp3_44100_128"
    )
    requested_model = model_id.strip() if model_id.strip() else "eleven_multilingual_v2"
    fallback_models = (
        [
            requested_model,
            "eleven_v3",
            "eleven_multilingual_v2",
            "eleven_turbo_v2_5",
        ]
        if allow_model_fallback
        else [requested_model]
    )
    candidate_models: list[str] = []
    for candidate in fallback_models:
        if candidate not in candidate_models:
            candidate_models.append(candidate)

    headers = {
        "Content-Type": "application/json",
        "xi-api-key": api_key,
    }

    def should_try_model_fallback(http_status: int, response_body: str) -> bool:
        if http_status in {401, 403, 404}:
            return True
        if http_status not in {400, 422}:
            return False
        normalized_body = response_body.lower()
        return (
            "model" in normalized_body
            or "subscription" in normalized_body
            or "not available" in normalized_body
            or "unsupported" in normalized_body
        )

    last_error: RuntimeError | None = None

    for model_index, active_model in enumerate(candidate_models):
        payload: dict[str, object] = {
            "text": text,
            "model_id": active_model,
            "voice_settings": {
                "stability": clamp_float(stability, 0.0, 1.0),
                "similarity_boost": clamp_float(similarity_boost, 0.0, 1.0),
                "style": clamp_float(style, 0.0, 1.0),
                "use_speaker_boost": bool(use_speaker_boost),
                "speed": 1.0,
            },
        }
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        for attempt in range(1, retry_attempts + 1):
            request = urllib.request.Request(
                url=request_url,
                data=encoded_payload,
                headers=headers,
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                    output_path.write_bytes(response.read())
                    return active_model
            except urllib.error.HTTPError as err:
                body = err.read().decode("utf-8", errors="replace")
                retriable = err.code in {408, 409, 429, 500, 502, 503, 504}
                if attempt < retry_attempts and retriable:
                    time.sleep(min(2**attempt, 8))
                    continue

                can_fallback_model = (
                    model_index < len(candidate_models) - 1
                    and should_try_model_fallback(err.code, body)
                )
                if can_fallback_model:
                    break

                last_error = RuntimeError(
                    "Falha no TTS da ElevenLabs.\n"
                    f"model_id: {active_model}\n"
                    f"http_status: {err.code}\n"
                    f"body: {body[:2000]}"
                )
                break
            except urllib.error.URLError as err:
                if attempt < retry_attempts:
                    time.sleep(min(2**attempt, 8))
                    continue
                last_error = RuntimeError(
                    f"Falha de rede no TTS da ElevenLabs (model_id={active_model}): {err}"
                )
                break

        if last_error is not None and model_index == len(candidate_models) - 1:
            raise last_error

    if last_error is not None:
        raise last_error
    raise RuntimeError("Falha inesperada no TTS da ElevenLabs.")


def resolve_elevenlabs_clone_profile(
    requested_model: str,
    requested_stability: float,
    requested_similarity_boost: float,
    requested_style: float,
    requested_use_speaker_boost: bool,
) -> tuple[str, float, float, float, bool]:
    normalized_model = requested_model.strip() if requested_model.strip() else ELEVENLABS_CLONE_SAFE_MODEL
    if normalized_model.lower() == "eleven_v3":
        normalized_model = ELEVENLABS_CLONE_SAFE_MODEL

    normalized_stability = clamp_float(
        max(requested_stability, ELEVENLABS_CLONE_SAFE_STABILITY),
        0.0,
        1.0,
    )
    normalized_similarity = clamp_float(
        max(requested_similarity_boost, ELEVENLABS_CLONE_SAFE_SIMILARITY),
        0.0,
        1.0,
    )
    normalized_style = clamp_float(min(requested_style, ELEVENLABS_CLONE_SAFE_STYLE), 0.0, 1.0)
    normalized_speaker_boost = bool(requested_use_speaker_boost and ELEVENLABS_CLONE_SAFE_SPEAKER_BOOST)

    return (
        normalized_model,
        normalized_stability,
        normalized_similarity,
        normalized_style,
        normalized_speaker_boost,
    )


def extract_voice_clone_sample_from_video(
    video_path: Path,
    output_audio_path: Path,
    duration_seconds: int,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    safe_duration = max(15, min(duration_seconds, 120))
    preferred_start_seconds = 18.0
    safe_start_seconds = 0.0
    try:
        total_duration_seconds = ffprobe_duration(video_path, timeout_seconds)
        if total_duration_seconds > safe_duration + 1:
            max_start_seconds = max(0.0, total_duration_seconds - safe_duration - 0.25)
            safe_start_seconds = min(preferred_start_seconds, max_start_seconds)
    except Exception:
        safe_start_seconds = 0.0

    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{safe_start_seconds:.3f}",
        "-i",
        str(video_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "44100",
        "-t",
        f"{safe_duration}",
        "-af",
        (
            "highpass=f=85,"
            "lowpass=f=6500,"
            "afftdn=nf=-20,"
            "acompressor=threshold=-18dB:ratio=2.5:attack=15:release=120,"
            "alimiter=limit=0.95"
        ),
        "-c:a",
        "libmp3lame",
        "-b:a",
        "192k",
        str(output_audio_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao extrair amostra de voz para clonagem.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def create_elevenlabs_ivc_voice(
    api_key: str,
    sample_audio_path: Path,
    voice_name: str,
    remove_background_noise: bool,
    timeout_seconds: int,
) -> str:
    ensure_binary("curl")
    cmd = [
        "curl",
        "-sS",
        "--fail-with-body",
        "-X",
        "POST",
        f"{ELEVENLABS_API_BASE}/voices/add",
        "-H",
        f"xi-api-key: {api_key}",
        "-H",
        "Accept: application/json",
        "-F",
        f"name={voice_name}",
        "-F",
        f"remove_background_noise={'true' if remove_background_noise else 'false'}",
        "-F",
        f"files=@{sample_audio_path}",
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao criar voz IVC na ElevenLabs.\n"
            f"stderr: {result.stderr.strip()}\n"
            f"body: {result.stdout.strip()[:2000]}"
        )

    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as err:
        raise RuntimeError(
            f"Resposta invalida ao criar voz IVC na ElevenLabs: {result.stdout[:1000]!r}"
        ) from err

    voice_id = parsed.get("voice_id")
    if not isinstance(voice_id, str) or not voice_id.strip():
        raise RuntimeError(f"ElevenLabs nao retornou voice_id: {parsed}")

    return voice_id.strip()


def delete_elevenlabs_voice(api_key: str, voice_id: str, timeout_seconds: int) -> None:
    encoded_voice_id = urllib.parse.quote(voice_id, safe="")
    request = urllib.request.Request(
        url=f"{ELEVENLABS_API_BASE}/voices/{encoded_voice_id}",
        headers={"xi-api-key": api_key},
        method="DELETE",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        _ = response.read()


def create_silence_segment(output_path: Path, duration_seconds: float, timeout_seconds: int) -> None:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds deve ser > 0 para silencio.")

    ensure_binary("ffmpeg")
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=r=16000:cl=mono",
        "-t",
        f"{duration_seconds:.6f}",
        "-acodec",
        "pcm_s16le",
        str(output_path),
    ]

    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao gerar segmento de silencio.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def normalize_cue_audio_duration(
    input_audio_path: Path,
    output_audio_path: Path,
    target_duration_seconds: float,
    max_speedup_factor: float,
    min_slowdown_factor: float,
    timeout_seconds: int,
) -> None:
    if target_duration_seconds <= 0:
        raise ValueError("target_duration_seconds deve ser > 0")
    if max_speedup_factor < 1.0:
        raise ValueError("max_speedup_factor deve ser >= 1.0")
    if not (0 < min_slowdown_factor <= 1.0):
        raise ValueError("min_slowdown_factor deve estar no intervalo (0, 1].")

    generated_duration = ffprobe_duration(input_audio_path, timeout_seconds)
    speed_factor = generated_duration / target_duration_seconds if target_duration_seconds > 0 else 1.0

    trim_filters = [
        "silenceremove=start_periods=1:start_duration=0.03:start_threshold=-46dB",
        "areverse",
        "silenceremove=start_periods=1:start_duration=0.03:start_threshold=-50dB",
        "areverse",
    ]

    tempo_factor = 1.0
    if speed_factor > 1.01:
        tempo_factor = min(speed_factor, max_speedup_factor)
    elif speed_factor < 0.97:
        tempo_factor = max(speed_factor, min_slowdown_factor)

    tempo_filters: list[str] = []
    if abs(tempo_factor - 1.0) > 0.01:
        tempo_filters.extend(build_atempo_filters(tempo_factor))

    trailing_filters = [
        f"apad=pad_dur={target_duration_seconds:.6f}",
        f"atrim=0:{target_duration_seconds:.6f}",
    ]

    candidate_filter_chains = [
        ",".join(trim_filters + tempo_filters + trailing_filters),
        ",".join(tempo_filters + trailing_filters),
    ]

    candidate_commands = [
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_audio_path),
            "-ac",
            "1",
            "-ar",
            "16000",
            "-af",
            filter_chain,
            "-c:a",
            "pcm_s16le",
            str(output_audio_path),
        ]
        for filter_chain in candidate_filter_chains
    ]

    failures: list[str] = []
    for cmd in candidate_commands:
        result = run_cmd(cmd, timeout_seconds)
        if result.returncode == 0:
            return
        failures.append(
            "Falha ao normalizar duracao do audio TTS.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )

    raise RuntimeError("\n\n".join(failures))


def resample_audio_for_sync(
    input_audio_path: Path,
    output_audio_path: Path,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_audio_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(output_audio_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao padronizar audio para analise de lip sync.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def detect_first_voice_onset_seconds(audio_path: Path, timeout_seconds: int) -> float:
    ensure_binary("ffmpeg")
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-i",
        str(audio_path),
        "-af",
        "silencedetect=noise=-42dB:d=0.02",
        "-f",
        "null",
        "-",
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao detectar inicio de fala para lip sync.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )

    waiting_initial_silence_end = False

    for line in result.stderr.splitlines():
        start_match = SILENCE_START_PATTERN.search(line)
        if start_match:
            silence_start = float(start_match.group("start"))
            if silence_start <= 0.05:
                waiting_initial_silence_end = True
            elif not waiting_initial_silence_end:
                return 0.0

        end_match = SILENCE_END_PATTERN.search(line)
        if end_match and waiting_initial_silence_end:
            silence_end = float(end_match.group("end"))
            return max(0.0, silence_end)

    return 0.0


def apply_global_audio_shift(
    input_audio_path: Path,
    output_audio_path: Path,
    shift_seconds: float,
    target_duration_seconds: float,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    absolute_shift_seconds = abs(shift_seconds)

    if absolute_shift_seconds < 0.005:
        shutil.copyfile(input_audio_path, output_audio_path)
        return

    if shift_seconds > 0:
        filter_chain = (
            f"atrim=start={shift_seconds:.6f},"
            f"apad=pad_dur={target_duration_seconds:.6f},"
            f"atrim=0:{target_duration_seconds:.6f}"
        )
    else:
        delay_ms = int(round(absolute_shift_seconds * 1000))
        filter_chain = (
            f"adelay={delay_ms}:all=1,"
            f"apad=pad_dur={target_duration_seconds:.6f},"
            f"atrim=0:{target_duration_seconds:.6f}"
        )

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_audio_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-af",
        filter_chain,
        "-c:a",
        "pcm_s16le",
        str(output_audio_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao aplicar deslocamento global de audio para lip sync.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def extract_audio_slice_from_video(
    video_path: Path,
    output_audio_path: Path,
    start_seconds: float,
    duration_seconds: float,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    safe_start = max(0.0, start_seconds)
    safe_duration = max(0.05, duration_seconds)
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{safe_start:.6f}",
        "-i",
        str(video_path),
        "-t",
        f"{safe_duration:.6f}",
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(output_audio_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao extrair trecho de audio de referencia.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def align_segment_onset_to_reference(
    reference_segment_path: Path,
    dubbed_segment_path: Path,
    output_segment_path: Path,
    target_duration_seconds: float,
    timeout_seconds: int,
    max_adjust_seconds: float = 0.22,
    desired_lead_seconds: float = 0.04,
) -> float:
    reference_onset = detect_first_voice_onset_seconds(reference_segment_path, timeout_seconds)
    dubbed_onset = detect_first_voice_onset_seconds(dubbed_segment_path, timeout_seconds)
    measured_delay = dubbed_onset - reference_onset
    requested_shift = measured_delay + desired_lead_seconds
    shift_seconds = clamp_float(requested_shift, -max_adjust_seconds, max_adjust_seconds)

    if abs(shift_seconds) < 0.008:
        shutil.copyfile(dubbed_segment_path, output_segment_path)
        return 0.0

    apply_global_audio_shift(
        input_audio_path=dubbed_segment_path,
        output_audio_path=output_segment_path,
        shift_seconds=shift_seconds,
        target_duration_seconds=target_duration_seconds,
        timeout_seconds=timeout_seconds,
    )
    return shift_seconds


def auto_calibrate_global_lipsync(
    reference_video_path: Path,
    dubbed_audio_path: Path,
    target_duration_seconds: float,
    timeout_seconds: int,
    max_adjust_seconds: float = 0.5,
    desired_lead_seconds: float = 0.09,
) -> float:
    with TemporaryDirectory(prefix="lipsync_auto_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        reference_audio_path = temp_dir / "reference.wav"
        dubbed_resampled_path = temp_dir / "dubbed.wav"
        shifted_audio_path = temp_dir / "dubbed_shifted.wav"

        extract_audio(reference_video_path, reference_audio_path, timeout_seconds)
        resample_audio_for_sync(dubbed_audio_path, dubbed_resampled_path, timeout_seconds)

        reference_onset = detect_first_voice_onset_seconds(reference_audio_path, timeout_seconds)
        dubbed_onset = detect_first_voice_onset_seconds(dubbed_resampled_path, timeout_seconds)

        measured_delay = dubbed_onset - reference_onset
        requested_shift = measured_delay + desired_lead_seconds
        clamped_shift = clamp_float(requested_shift, -max_adjust_seconds, max_adjust_seconds)

        if abs(clamped_shift) < 0.015:
            return 0.0

        apply_global_audio_shift(
            input_audio_path=dubbed_resampled_path,
            output_audio_path=shifted_audio_path,
            shift_seconds=clamped_shift,
            target_duration_seconds=target_duration_seconds,
            timeout_seconds=timeout_seconds,
        )
        shutil.copyfile(shifted_audio_path, dubbed_audio_path)
        return clamped_shift


def classify_voice_gender_from_zero_crossing_rate(rate: float) -> tuple[str, float]:
    threshold = 0.108
    detected_gender = "female" if rate >= threshold else "male"
    confidence = min(1.0, abs(rate - threshold) / 0.03)
    return detected_gender, confidence


def detect_voice_gender_from_video(
    video_path: Path,
    timeout_seconds: int,
) -> tuple[str | None, float | None]:
    if not video_path.exists():
        return None, None

    with TemporaryDirectory(prefix="voice_profile_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        sample_audio_path = temp_dir / "voice_sample.wav"
        extract_cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(video_path),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-t",
            "45",
            "-af",
            "highpass=f=70,lowpass=f=3000",
            "-c:a",
            "pcm_s16le",
            str(sample_audio_path),
        ]

        extract_result = run_cmd(extract_cmd, timeout_seconds)
        if extract_result.returncode != 0 or not sample_audio_path.exists():
            return None, None

        stats_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-nostats",
            "-i",
            str(sample_audio_path),
            "-af",
            "astats=metadata=1:reset=1",
            "-f",
            "null",
            "-",
        ]
        stats_result = run_cmd(stats_cmd, timeout_seconds)
        if stats_result.returncode != 0:
            return None, None

        rates = [
            float(match.group("rate"))
            for match in ZERO_CROSSINGS_RATE_PATTERN.finditer(stats_result.stderr)
        ]
        if not rates:
            return None, None

        mean_rate = statistics.fmean(rates)
        detected_gender, confidence = classify_voice_gender_from_zero_crossing_rate(mean_rate)
        if confidence < 0.35:
            return None, confidence
        return detected_gender, confidence


def resolve_tts_voice(
    requested_voice: str,
    input_video: Path | None,
    timeout_seconds: int,
    male_voice: str,
    female_voice: str,
) -> tuple[str, str | None, float | None]:
    normalized_requested = requested_voice.strip().lower()
    if normalized_requested not in {"auto", "detect", "gender-auto"}:
        return requested_voice, None, None

    if input_video is None:
        return male_voice, None, None

    detected_gender, confidence = detect_voice_gender_from_video(
        video_path=input_video,
        timeout_seconds=timeout_seconds,
    )

    if detected_gender == "male":
        return male_voice, detected_gender, confidence

    if detected_gender == "female":
        return female_voice, detected_gender, confidence

    return male_voice, None, confidence


def build_tts_instructions(
    base_instructions: str,
    detected_gender: str | None,
    source_wpm: float | None,
) -> str:
    dynamic_parts: list[str] = []

    if detected_gender == "male":
        dynamic_parts.append("Mantenha voz masculina adulta, natural e clara.")
    elif detected_gender == "female":
        dynamic_parts.append("Mantenha voz feminina adulta, natural e clara.")

    if source_wpm is not None:
        dynamic_parts.append(
            f"Mantenha ritmo de fala proximo de {int(round(source_wpm))} palavras por minuto."
        )

    dynamic_parts.append(
        "Evite acelerar artificialmente; priorize cadencia humana com pausas curtas."
    )

    base_text = base_instructions.strip()
    if not base_text:
        return " ".join(dynamic_parts)
    return f"{base_text} {' '.join(dynamic_parts)}".strip()


def adapt_text_for_timing(text: str, target_duration_seconds: float, max_chars_per_cue: int) -> str:
    collapsed = re.sub(r"\s+", " ", text.strip())
    if not collapsed:
        return ""

    if max_chars_per_cue > 0 and len(collapsed) > max_chars_per_cue:
        collapsed = collapsed[:max_chars_per_cue].rstrip()

    max_words = max(3, int(target_duration_seconds * 2.75))
    words = collapsed.split(" ")
    if len(words) <= max_words:
        return collapsed

    shortened = " ".join(words[:max_words]).rstrip(" ,;:-")
    return shortened


def concat_audio_segments(segment_paths: list[Path], output_path: Path, timeout_seconds: int) -> None:
    if not segment_paths:
        raise RuntimeError("Nenhum segmento de audio para concatenar.")

    ensure_binary("ffmpeg")

    list_file = output_path.with_suffix(".segments.txt")
    list_file.write_text(
        "\n".join([f"file '{path.as_posix()}'" for path in segment_paths]) + "\n",
        encoding="utf-8",
    )

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(list_file),
        "-c",
        "copy",
        str(output_path),
    ]

    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao concatenar segmentos de audio.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def replace_video_audio(
    video_path: Path,
    dubbed_audio_path: Path,
    output_video_path: Path,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(dubbed_audio_path),
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-ac",
        "1",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-sn",
        "-dn",
        "-shortest",
        str(output_video_path),
    ]

    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao substituir audio do video.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def synthesize_dubbed_audio_timeline(
    cues: list[SrtCue],
    video_path: Path,
    output_audio_path: Path,
    openai_api_key: str,
    elevenlabs_api_key: str | None,
    tts_provider: str,
    tts_model: str,
    tts_voice: str,
    tts_instructions: str,
    elevenlabs_stability: float,
    elevenlabs_similarity_boost: float,
    elevenlabs_style: float,
    elevenlabs_use_speaker_boost: bool,
    elevenlabs_clone_from_video: bool,
    elevenlabs_clone_duration_seconds: int,
    elevenlabs_clone_remove_background_noise: bool,
    timeout_seconds: int,
    retry_attempts: int,
    max_tts_chars_per_cue: int,
    max_speedup_factor: float,
    min_slowdown_factor: float,
    global_audio_lead_seconds: float,
) -> None:
    if not cues:
        raise RuntimeError("Nao ha cues traduzidas para dublagem.")

    with TemporaryDirectory(prefix="dub_segments_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        segment_paths: list[Path] = []
        cursor = 0.0
        effective_tts_voice = tts_voice
        effective_tts_model = tts_model
        effective_eleven_stability = elevenlabs_stability
        effective_eleven_similarity_boost = elevenlabs_similarity_boost
        effective_eleven_style = elevenlabs_style
        effective_eleven_use_speaker_boost = elevenlabs_use_speaker_boost
        ephemeral_elevenlabs_voice_id: str | None = None

        ordered_cues = sorted(cues, key=lambda cue: srt_timestamp_to_seconds(cue.start))

        try:
            if tts_provider == "elevenlabs" and elevenlabs_clone_from_video:
                if not elevenlabs_api_key:
                    raise RuntimeError(
                        "ELEVENLABS_API_KEY ausente para clonar voz a partir do video."
                    )

                clone_sample_path = temp_dir / "elevenlabs_clone_sample.mp3"
                print("[dub] extraindo amostra para clonagem ElevenLabs", file=sys.stderr)
                extract_voice_clone_sample_from_video(
                    video_path=video_path,
                    output_audio_path=clone_sample_path,
                    duration_seconds=elevenlabs_clone_duration_seconds,
                    timeout_seconds=timeout_seconds,
                )
                clone_voice_name = f"auto-clone-{video_path.stem[:24]}-{int(time.time())}"
                print("[dub] criando voz temporaria na ElevenLabs", file=sys.stderr)
                ephemeral_elevenlabs_voice_id = create_elevenlabs_ivc_voice(
                    api_key=elevenlabs_api_key,
                    sample_audio_path=clone_sample_path,
                    voice_name=clone_voice_name,
                    remove_background_noise=elevenlabs_clone_remove_background_noise,
                    timeout_seconds=timeout_seconds,
                )
                effective_tts_voice = ephemeral_elevenlabs_voice_id
                print(
                    (
                        "[dub] voz temporaria criada: "
                        f"{ephemeral_elevenlabs_voice_id[:6]}...{ephemeral_elevenlabs_voice_id[-4:]}"
                    ),
                    file=sys.stderr,
                )

            if tts_provider == "elevenlabs" and elevenlabs_clone_from_video:
                (
                    effective_tts_model,
                    effective_eleven_stability,
                    effective_eleven_similarity_boost,
                    effective_eleven_style,
                    effective_eleven_use_speaker_boost,
                ) = resolve_elevenlabs_clone_profile(
                    requested_model=effective_tts_model,
                    requested_stability=effective_eleven_stability,
                    requested_similarity_boost=effective_eleven_similarity_boost,
                    requested_style=effective_eleven_style,
                    requested_use_speaker_boost=effective_eleven_use_speaker_boost,
                )
                print(
                    (
                        "[dub] perfil ElevenLabs para clonagem aplicado: "
                        f"model={effective_tts_model} stability={effective_eleven_stability:.2f} "
                        f"similarity={effective_eleven_similarity_boost:.2f} "
                        f"style={effective_eleven_style:.2f} "
                        f"speaker_boost={effective_eleven_use_speaker_boost}"
                    ),
                    file=sys.stderr,
                )

            for cue_index, cue in enumerate(ordered_cues, start=1):
                raw_cue_start = srt_timestamp_to_seconds(cue.start)
                raw_cue_end = srt_timestamp_to_seconds(cue.end)
                cue_start = max(0.0, raw_cue_start - global_audio_lead_seconds)
                cue_end = max(cue_start + 0.05, raw_cue_end - global_audio_lead_seconds)
                cue_duration = max(0.05, cue_end - cue_start)

                if cue_start > cursor + 0.01:
                    silence_path = temp_dir / f"segment_{cue_index:05d}_silence.wav"
                    create_silence_segment(silence_path, cue_start - cursor, timeout_seconds)
                    segment_paths.append(silence_path)
                    cursor = cue_start

                raw_tts_path = temp_dir / f"segment_{cue_index:05d}_raw.mp3"
                normalized_path = temp_dir / f"segment_{cue_index:05d}_norm.wav"

                text_for_tts = adapt_text_for_timing(
                    text=cue.text,
                    target_duration_seconds=cue_duration,
                    max_chars_per_cue=max_tts_chars_per_cue,
                )
                if not text_for_tts:
                    create_silence_segment(normalized_path, cue_duration, timeout_seconds)
                    segment_paths.append(normalized_path)
                    cursor = cue_end
                    continue

                print(
                    f"[dub] sintetizando cue {cue_index}/{len(ordered_cues)}",
                    file=sys.stderr,
                )
                if tts_provider == "openai":
                    synthesize_tts_audio_openai(
                        text=text_for_tts,
                        api_key=openai_api_key,
                        tts_model=tts_model,
                        tts_voice=effective_tts_voice,
                        tts_instructions=tts_instructions,
                        output_path=raw_tts_path,
                        timeout_seconds=timeout_seconds,
                        retry_attempts=retry_attempts,
                    )
                elif tts_provider == "elevenlabs":
                    if not elevenlabs_api_key:
                        raise RuntimeError(
                            "ELEVENLABS_API_KEY ausente para sintetizar audio com ElevenLabs."
                        )
                    if not effective_tts_voice.strip():
                        raise RuntimeError(
                            "Voice ID da ElevenLabs ausente para sintetizar a dublagem."
                        )
                    active_eleven_model = synthesize_tts_audio_elevenlabs(
                        text=text_for_tts,
                        api_key=elevenlabs_api_key,
                        voice_id=effective_tts_voice,
                        model_id=effective_tts_model,
                        stability=effective_eleven_stability,
                        similarity_boost=effective_eleven_similarity_boost,
                        style=effective_eleven_style,
                        use_speaker_boost=effective_eleven_use_speaker_boost,
                        output_path=raw_tts_path,
                        timeout_seconds=timeout_seconds,
                        retry_attempts=retry_attempts,
                        allow_model_fallback=False,
                    )
                    if active_eleven_model != effective_tts_model:
                        print(
                            (
                                "[dub] fallback de modelo ElevenLabs aplicado: "
                                f"{effective_tts_model} -> {active_eleven_model}"
                            ),
                            file=sys.stderr,
                        )
                        effective_tts_model = active_eleven_model
                else:
                    raise RuntimeError(f"TTS provider nao suportado: {tts_provider}")

                normalize_cue_audio_duration(
                    input_audio_path=raw_tts_path,
                    output_audio_path=normalized_path,
                    target_duration_seconds=cue_duration,
                    max_speedup_factor=max_speedup_factor,
                    min_slowdown_factor=min_slowdown_factor,
                    timeout_seconds=timeout_seconds,
                )

                aligned_path = temp_dir / f"segment_{cue_index:05d}_aligned.wav"
                reference_path = temp_dir / f"segment_{cue_index:05d}_ref.wav"

                try:
                    extract_audio_slice_from_video(
                        video_path=video_path,
                        output_audio_path=reference_path,
                        start_seconds=raw_cue_start,
                        duration_seconds=raw_cue_end - raw_cue_start,
                        timeout_seconds=timeout_seconds,
                    )
                    applied_segment_shift = align_segment_onset_to_reference(
                        reference_segment_path=reference_path,
                        dubbed_segment_path=normalized_path,
                        output_segment_path=aligned_path,
                        target_duration_seconds=cue_duration,
                        timeout_seconds=timeout_seconds,
                    )
                    if abs(applied_segment_shift) >= 0.02:
                        print(
                            (
                                f"[dub] cue {cue_index} alinhado: "
                                f"{applied_segment_shift:+.3f}s"
                            ),
                            file=sys.stderr,
                        )
                    segment_paths.append(aligned_path)
                except Exception as cue_align_error:
                    print(
                        f"[dub] aviso: alinhamento por cue ignorado ({cue_align_error})",
                        file=sys.stderr,
                    )
                    fallback_shifted_path = temp_dir / f"segment_{cue_index:05d}_fallback_lead.wav"
                    try:
                        apply_global_audio_shift(
                            input_audio_path=normalized_path,
                            output_audio_path=fallback_shifted_path,
                            shift_seconds=0.04,
                            target_duration_seconds=cue_duration,
                            timeout_seconds=timeout_seconds,
                        )
                        segment_paths.append(fallback_shifted_path)
                    except Exception:
                        segment_paths.append(normalized_path)
                cursor = cue_end

            total_video_duration = ffprobe_duration(video_path, timeout_seconds)
            if total_video_duration > cursor + 0.01:
                tail_silence_path = temp_dir / "segment_zzzz_tail.wav"
                create_silence_segment(tail_silence_path, total_video_duration - cursor, timeout_seconds)
                segment_paths.append(tail_silence_path)

            concat_audio_segments(segment_paths, output_audio_path, timeout_seconds)
            try:
                applied_shift_seconds = auto_calibrate_global_lipsync(
                    reference_video_path=video_path,
                    dubbed_audio_path=output_audio_path,
                    target_duration_seconds=total_video_duration,
                    timeout_seconds=timeout_seconds,
                )
                if abs(applied_shift_seconds) >= 0.03:
                    print(
                        (
                            "[dub] ajuste global de lip sync aplicado: "
                            f"{applied_shift_seconds:+.3f}s"
                        ),
                        file=sys.stderr,
                    )
            except Exception as lipsync_error:
                print(
                    f"[dub] aviso: ajuste automatico de lip sync ignorado ({lipsync_error})",
                    file=sys.stderr,
                )
        finally:
            if (
                ephemeral_elevenlabs_voice_id is not None
                and elevenlabs_api_key is not None
                and elevenlabs_clone_from_video
            ):
                try:
                    print(
                        (
                            "[dub] removendo voz temporaria na ElevenLabs: "
                            f"{ephemeral_elevenlabs_voice_id[:6]}...{ephemeral_elevenlabs_voice_id[-4:]}"
                        ),
                        file=sys.stderr,
                    )
                    delete_elevenlabs_voice(
                        api_key=elevenlabs_api_key,
                        voice_id=ephemeral_elevenlabs_voice_id,
                        timeout_seconds=timeout_seconds,
                    )
                except Exception as cleanup_error:
                    print(
                        f"[dub] aviso: falha ao remover voz temporaria: {cleanup_error}",
                        file=sys.stderr,
                    )


def escape_subtitles_filter_path(path: Path) -> str:
    return (
        str(path.resolve())
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
    )


def burn_subtitles(
    video_path: Path,
    subtitle_path: Path,
    output_video_path: Path,
    timeout_seconds: int,
) -> None:
    ensure_binary("ffmpeg")
    subtitle_filter = f"subtitles='{escape_subtitles_filter_path(subtitle_path)}'"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-vf",
        subtitle_filter,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "copy",
        str(output_video_path),
    ]
    result = run_cmd(cmd, timeout_seconds)
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao embutir legenda no video.\n"
            f"Comando: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )


def resolve_api_key(explicit_api_key: str | None, env_var_name: str) -> str:
    if explicit_api_key:
        return explicit_api_key.strip()
    from_env = os.getenv(env_var_name, "").strip()
    if from_env:
        return from_env
    raise RuntimeError(
        f"API key ausente. Informe --api-key ou configure a env {env_var_name}."
    )


def copy_if_needed(source: Path, target: Path) -> None:
    if source.resolve() == target.resolve():
        return
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "MVP: traduz e dubla video de estudo EN -> idioma alvo "
            "(transcricao + traducao + legenda + dublagem opcional)."
        )
    )
    parser.add_argument("--input-video", type=Path, help="Arquivo de video de entrada")
    parser.add_argument("--source-srt", type=Path, help="SRT de origem (pula transcricao)")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("video_translation_output"),
        help="Diretorio de saida",
    )
    parser.add_argument("--source-lang", default="en", help="Idioma de origem")
    parser.add_argument("--target-lang", default="pt-BR", help="Idioma de destino")
    parser.add_argument(
        "--transcribe-model",
        default="whisper-1",
        help="Modelo de transcricao",
    )
    parser.add_argument(
        "--translate-model",
        default="gpt-4o-mini",
        help="Modelo de traducao",
    )
    parser.add_argument(
        "--tts-provider",
        default="openai",
        choices=["openai", "elevenlabs"],
        help="Provedor de TTS: openai ou elevenlabs",
    )
    parser.add_argument(
        "--tts-model",
        default="gpt-4o-mini-tts",
        help="Modelo de text-to-speech",
    )
    parser.add_argument(
        "--tts-voice",
        default="auto",
        help="Voice para TTS (ex: auto, coral, alloy, verse, ash)",
    )
    parser.add_argument(
        "--tts-male-voice",
        default="ash",
        help="Voice usada quando deteccao automatica indicar voz masculina",
    )
    parser.add_argument(
        "--tts-female-voice",
        default="coral",
        help="Voice usada quando deteccao automatica indicar voz feminina",
    )
    parser.add_argument(
        "--tts-instructions",
        default="Fale de forma natural, clara e didatica para conteudo educacional.",
        help="Instrucoes de estilo para o TTS",
    )
    parser.add_argument(
        "--elevenlabs-api-key",
        default=None,
        help="API key da ElevenLabs (opcional; usa env por padrao)",
    )
    parser.add_argument(
        "--elevenlabs-api-key-env",
        default="ELEVENLABS_API_KEY",
        help="Nome da variavel de ambiente da API key da ElevenLabs",
    )
    parser.add_argument(
        "--elevenlabs-stability",
        type=float,
        default=0.45,
        help="Voice setting stability da ElevenLabs (0..1)",
    )
    parser.add_argument(
        "--elevenlabs-similarity-boost",
        type=float,
        default=0.85,
        help="Voice setting similarity_boost da ElevenLabs (0..1)",
    )
    parser.add_argument(
        "--elevenlabs-style",
        type=float,
        default=0.15,
        help="Voice setting style da ElevenLabs (0..1)",
    )
    parser.add_argument(
        "--elevenlabs-use-speaker-boost",
        type=parse_boolean,
        default=True,
        help="Voice setting use_speaker_boost da ElevenLabs (true/false)",
    )
    parser.add_argument(
        "--elevenlabs-clone-from-video",
        type=parse_boolean,
        default=False,
        help="Quando true, cria automaticamente uma voz IVC temporaria a partir do video",
    )
    parser.add_argument(
        "--elevenlabs-clone-duration-seconds",
        type=int,
        default=90,
        help="Duracao maxima da amostra de audio extraida do video para clonagem",
    )
    parser.add_argument(
        "--elevenlabs-clone-remove-background-noise",
        type=parse_boolean,
        default=True,
        help="Remove ruido de fundo ao criar a voz IVC na ElevenLabs",
    )
    parser.add_argument(
        "--elevenlabs-dubbing-max-wait-seconds",
        type=int,
        default=3600,
        help="Tempo maximo para aguardar a conclusao do dubbing gerenciado da ElevenLabs",
    )
    parser.add_argument(
        "--elevenlabs-dubbing-poll-interval-seconds",
        type=float,
        default=5.0,
        help="Intervalo de polling para verificar status do dubbing gerenciado da ElevenLabs",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key da OpenAI (opcional; usa env por padrao)",
    )
    parser.add_argument(
        "--api-key-env",
        default="OPENAI_API_KEY",
        help="Nome da variavel de ambiente da API key",
    )
    parser.add_argument(
        "--force-transcribe",
        action="store_true",
        help="Forca nova transcricao mesmo se o SRT ingles ja existir",
    )
    parser.add_argument(
        "--force-translate",
        action="store_true",
        help="Forca nova traducao mesmo se o SRT destino ja existir",
    )
    parser.add_argument(
        "--burn-subtitles",
        action="store_true",
        help="Gera video final com legenda embutida (hard sub)",
    )
    parser.add_argument(
        "--dub-audio",
        action="store_true",
        help="Gera audio dublado no idioma alvo via TTS",
    )
    parser.add_argument(
        "--replace-audio",
        action="store_true",
        help="Substitui o audio do video pelo audio dublado",
    )
    parser.add_argument(
        "--keep-audio",
        action="store_true",
        help="Mantem o arquivo de audio extraido",
    )
    parser.add_argument(
        "--max-batch-items",
        type=int,
        default=30,
        help="Maximo de cues por lote de traducao",
    )
    parser.add_argument(
        "--max-batch-chars",
        type=int,
        default=2200,
        help="Maximo de caracteres por lote de traducao",
    )
    parser.add_argument(
        "--max-tts-chars-per-cue",
        type=int,
        default=450,
        help="Limite de caracteres por cue para sintetizar no TTS",
    )
    parser.add_argument(
        "--max-speedup-factor",
        type=float,
        default=1.18,
        help="Limite maximo de aceleracao por cue para ajuste de sincronismo",
    )
    parser.add_argument(
        "--min-slowdown-factor",
        type=float,
        default=0.90,
        help="Limite minimo de desaceleracao por cue para ajuste de sincronismo",
    )
    parser.add_argument(
        "--global-audio-lead-seconds",
        type=float,
        default=0.22,
        help="Adianta globalmente o audio final em segundos para melhorar lip sync",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=600,
        help="Timeout por chamada externa",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=3,
        help="Tentativas de retry para chamadas HTTP",
    )
    return parser.parse_args(argv)


def derive_base_name(input_video: Path | None, source_srt: Path | None) -> str:
    if input_video:
        return input_video.stem
    if source_srt:
        return source_srt.stem
    return "video"


def run_pipeline(args: argparse.Namespace) -> dict[str, str]:
    if args.input_video is None and args.source_srt is None:
        raise RuntimeError("Informe --input-video ou --source-srt.")

    if args.input_video and not args.input_video.exists():
        raise RuntimeError(f"Video nao encontrado: {args.input_video}")

    if args.source_srt and not args.source_srt.exists():
        raise RuntimeError(f"SRT nao encontrado: {args.source_srt}")

    if (args.burn_subtitles or args.dub_audio or args.replace_audio) and args.input_video is None:
        raise RuntimeError("--burn-subtitles/--dub-audio/--replace-audio exigem --input-video.")

    tts_provider = str(args.tts_provider).strip().lower()

    if tts_provider not in {"openai", "elevenlabs"}:
        raise RuntimeError(f"TTS provider invalido: {args.tts_provider}")

    resolved_tts_model = str(args.tts_model).strip()
    if tts_provider == "elevenlabs" and resolved_tts_model == "gpt-4o-mini-tts":
        resolved_tts_model = "eleven_multilingual_v2"

    elevenlabs_api_key: str | None = None
    if args.dub_audio and tts_provider == "elevenlabs":
        elevenlabs_api_key = resolve_api_key(
            args.elevenlabs_api_key, args.elevenlabs_api_key_env
        )

    requested_target_lang = str(args.target_lang).strip()
    effective_target_lang = (
        "pt-BR"
        if normalize_language_tag(requested_target_lang).startswith("pt")
        else requested_target_lang
    )
    if effective_target_lang != requested_target_lang:
        print(
            (
                "[pipeline] target-lang ajustado para pt-BR para garantir "
                "localizacao brasileira consistente."
            ),
            file=sys.stderr,
        )

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    base_name = derive_base_name(args.input_video, args.source_srt)
    audio_path = output_dir / f"{base_name}.{args.source_lang}.wav"
    source_srt_path = output_dir / f"{base_name}.{args.source_lang}.srt"
    target_srt_path = output_dir / f"{base_name}.{effective_target_lang}.srt"
    subtitled_video_path = output_dir / f"{base_name}.{effective_target_lang}.hardsub.mp4"
    dubbed_audio_path = output_dir / f"{base_name}.{effective_target_lang}.dub.wav"
    dubbed_video_path = output_dir / f"{base_name}.{effective_target_lang}.dub.mp4"

    use_elevenlabs_managed_dubbing = (
        tts_provider == "elevenlabs"
        and args.dub_audio
        and args.replace_audio
        and args.input_video is not None
    )
    if use_elevenlabs_managed_dubbing:
        elevenlabs_api_key = resolve_api_key(
            args.elevenlabs_api_key, args.elevenlabs_api_key_env
        )
        return run_elevenlabs_managed_dubbing(
            input_video=args.input_video,
            output_dir=output_dir,
            base_name=base_name,
            source_language=args.source_lang,
            target_language=effective_target_lang,
            api_key=elevenlabs_api_key,
            timeout_seconds=args.timeout_seconds,
            retry_attempts=args.retry_attempts,
            burn_subtitles_enabled=bool(args.burn_subtitles),
            max_wait_seconds=int(args.elevenlabs_dubbing_max_wait_seconds),
            poll_interval_seconds=float(args.elevenlabs_dubbing_poll_interval_seconds),
        )

    openai_api_key = resolve_api_key(args.api_key, args.api_key_env)

    if args.source_srt:
        print("[pipeline] usando SRT de origem informado", file=sys.stderr)
        copy_if_needed(args.source_srt, source_srt_path)
    else:
        should_transcribe = args.force_transcribe or not source_srt_path.exists()
        if should_transcribe:
            print("[pipeline] extraindo audio", file=sys.stderr)
            extract_audio(
                video_path=args.input_video,
                audio_path=audio_path,
                timeout_seconds=args.timeout_seconds,
            )
            print("[pipeline] transcrevendo audio para SRT", file=sys.stderr)
            transcribed_srt = transcribe_audio_to_srt(
                audio_path=audio_path,
                api_key=openai_api_key,
                source_language=args.source_lang,
                transcribe_model=args.transcribe_model,
                timeout_seconds=args.timeout_seconds,
            )
            source_srt_path.write_text(
                normalize_srt_text(transcribed_srt) + "\n",
                encoding="utf-8",
            )
        else:
            print("[pipeline] reutilizando SRT de origem existente", file=sys.stderr)

    source_srt_raw = source_srt_path.read_text(encoding="utf-8")
    source_cues = parse_srt(source_srt_raw)
    if not source_cues:
        raise RuntimeError("Nao foi possivel parsear cues do SRT de origem.")

    should_translate = args.force_translate or not target_srt_path.exists()
    if should_translate:
        print("[pipeline] traduzindo cues para idioma alvo", file=sys.stderr)
        target_cues = translate_cues(
            cues=source_cues,
            source_language=args.source_lang,
            target_language=effective_target_lang,
            api_key=openai_api_key,
            translate_model=args.translate_model,
            timeout_seconds=args.timeout_seconds,
            retry_attempts=args.retry_attempts,
            max_batch_items=args.max_batch_items,
            max_batch_chars=args.max_batch_chars,
        )
        target_srt_path.write_text(render_srt(target_cues), encoding="utf-8")
    else:
        print("[pipeline] reutilizando SRT traduzido existente", file=sys.stderr)

    target_cues = parse_srt(target_srt_path.read_text(encoding="utf-8"))

    working_video_path = args.input_video
    source_wpm = estimate_speech_rate_wpm(source_cues)
    detected_voice_gender: str | None = None
    voice_confidence: float | None = None
    resolved_tts_voice = str(args.tts_voice).strip()
    resolved_tts_instructions = args.tts_instructions

    elevenlabs_clone_from_video = bool(args.elevenlabs_clone_from_video)

    if tts_provider == "openai":
        resolved_tts_voice, detected_voice_gender, voice_confidence = resolve_tts_voice(
            requested_voice=args.tts_voice,
            input_video=args.input_video,
            timeout_seconds=args.timeout_seconds,
            male_voice=args.tts_male_voice,
            female_voice=args.tts_female_voice,
        )
        resolved_tts_instructions = build_tts_instructions(
            base_instructions=args.tts_instructions,
            detected_gender=detected_voice_gender,
            source_wpm=source_wpm,
        )

        if resolved_tts_voice != args.tts_voice:
            print(
                (
                    "[pipeline] voice automatica selecionada: "
                    f"{resolved_tts_voice} (gender={detected_voice_gender or 'unknown'} "
                    f"confidence={voice_confidence if voice_confidence is not None else 'n/a'})"
                ),
                file=sys.stderr,
            )
    else:
        requires_voice_id = not elevenlabs_clone_from_video
        if requires_voice_id and resolved_tts_voice.lower() in {"", "auto", "detect", "gender-auto"}:
            raise RuntimeError(
                (
                    "Para ElevenLabs sem clonagem automatica, informe --tts-voice "
                    "com o Voice ID da voz clonada."
                )
            )

    if args.burn_subtitles:
        print("[pipeline] gerando video com legenda embutida", file=sys.stderr)
        burn_subtitles(
            video_path=args.input_video,
            subtitle_path=target_srt_path,
            output_video_path=subtitled_video_path,
            timeout_seconds=args.timeout_seconds,
        )
        working_video_path = subtitled_video_path

    if args.dub_audio:
        print("[pipeline] sintetizando audio dublado", file=sys.stderr)
        global_audio_lead_seconds = clamp_float(
            float(args.global_audio_lead_seconds),
            min_value=0.0,
            max_value=0.5,
        )
        synthesize_dubbed_audio_timeline(
            cues=target_cues,
            video_path=working_video_path,
            output_audio_path=dubbed_audio_path,
            openai_api_key=openai_api_key,
            elevenlabs_api_key=elevenlabs_api_key,
            tts_provider=tts_provider,
            tts_model=resolved_tts_model,
            tts_voice=resolved_tts_voice,
            tts_instructions=resolved_tts_instructions,
            elevenlabs_stability=args.elevenlabs_stability,
            elevenlabs_similarity_boost=args.elevenlabs_similarity_boost,
            elevenlabs_style=args.elevenlabs_style,
            elevenlabs_use_speaker_boost=args.elevenlabs_use_speaker_boost,
            elevenlabs_clone_from_video=elevenlabs_clone_from_video,
            elevenlabs_clone_duration_seconds=args.elevenlabs_clone_duration_seconds,
            elevenlabs_clone_remove_background_noise=args.elevenlabs_clone_remove_background_noise,
            timeout_seconds=args.timeout_seconds,
            retry_attempts=args.retry_attempts,
            max_tts_chars_per_cue=args.max_tts_chars_per_cue,
            max_speedup_factor=args.max_speedup_factor,
            min_slowdown_factor=args.min_slowdown_factor,
            global_audio_lead_seconds=global_audio_lead_seconds,
        )

    if args.replace_audio:
        if not args.dub_audio:
            raise RuntimeError("--replace-audio exige --dub-audio.")
        print("[pipeline] substituindo trilha de audio do video", file=sys.stderr)
        replace_video_audio(
            video_path=working_video_path,
            dubbed_audio_path=dubbed_audio_path,
            output_video_path=dubbed_video_path,
            timeout_seconds=args.timeout_seconds,
        )

    if (not args.keep_audio) and audio_path.exists():
        audio_path.unlink()

    outputs: dict[str, str] = {
        "source_srt": str(source_srt_path.resolve()),
        "target_srt": str(target_srt_path.resolve()),
        "tts_provider": tts_provider,
        "tts_model": resolved_tts_model,
        "tts_voice": resolved_tts_voice,
    }

    if detected_voice_gender is not None:
        outputs["detected_voice_gender"] = detected_voice_gender
    if source_wpm is not None:
        outputs["estimated_source_wpm"] = f"{source_wpm:.1f}"

    if args.burn_subtitles:
        outputs["subtitled_video"] = str(subtitled_video_path.resolve())

    if args.dub_audio:
        outputs["dubbed_audio"] = str(dubbed_audio_path.resolve())

    if args.replace_audio:
        outputs["dubbed_video"] = str(dubbed_video_path.resolve())

    return outputs


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        outputs = run_pipeline(args)
        print(json.dumps({"ok": True, "outputs": outputs}, ensure_ascii=False, indent=2))
        return 0
    except Exception as err:
        print(json.dumps({"ok": False, "error": str(err)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
