# Video Translation MVP (EN -> PT-BR)

MVP para traduzir videos de estudo de ingles para portugues com pipeline:

1. extrai audio do video (`ffmpeg`)
2. transcreve audio para SRT em ingles (OpenAI Audio Transcriptions)
3. traduz SRT para pt-BR em lotes (OpenAI Chat Completions com JSON)
4. opcional: gera video com legenda embutida (`ffmpeg`)
5. opcional: gera dublagem no idioma alvo via OpenAI TTS e substitui a trilha do video

## Pre-requisitos

- Python 3.10+
- `ffmpeg` no PATH
- `ffprobe` no PATH
- `curl` no PATH
- `OPENAI_API_KEY` configurada

## Execucao rapida

```bash
export OPENAI_API_KEY="sua-chave"
python3 video_study_translate_mvp.py \
  --input-video /caminho/video.mp4 \
  --source-lang en \
  --target-lang pt-BR \
  --dub-audio \
  --replace-audio
```

Saidas padrao em `video_translation_output/`:

- `video.en.srt`
- `video.pt-BR.srt`
- `video.pt-BR.dub.wav` (quando usar `--dub-audio`)
- `video.pt-BR.dub.mp4` (quando usar `--replace-audio`)
- `video.pt-BR.hardsub.mp4` (quando usar `--burn-subtitles`)

## Modos uteis

Usar SRT existente e pular transcricao:

```bash
python3 video_study_translate_mvp.py \
  --source-srt /caminho/video.en.srt \
  --target-lang pt-BR
```

Forcar nova traducao:

```bash
python3 video_study_translate_mvp.py \
  --source-srt /caminho/video.en.srt \
  --force-translate
```

## Modelos e ajuste de lotes

- `--transcribe-model` (padrao: `whisper-1`)
- `--translate-model` (padrao: `gpt-4o-mini`)
- `--tts-model` (padrao: `gpt-4o-mini-tts`)
- `--tts-voice` (padrao: `coral`)
- `--max-batch-items` (padrao: `30`)
- `--max-batch-chars` (padrao: `2200`)
- `--max-tts-chars-per-cue` (padrao: `450`)

## Observacoes de qualidade

- O tradutor preserva IDs e timestamps do SRT.
- Se um lote falhar validacao (faltando IDs), cai para fallback cue a cue.
- A dublagem usa timeline por cue: gera TTS por legenda, ajusta duracao por janela temporal e concatena.

## Escopo atual

Este MVP esta versionado como CLI standalone. Integracoes web, upload multipart,
fila persistente e armazenamento de jobs devem ser adicionados em uma camada de
aplicacao separada, chamando `video_study_translate_mvp.py` como pipeline.
