#!/usr/bin/env python3
"""Download and extract a GitHub branch ZIP with proxy-aware retries."""

from __future__ import annotations

import shutil
import sys
import tempfile
import zipfile
import os
import socket
import time
import ssl
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def die(message: str) -> None:
    print(f"[github-zip-download-extract] erro: {message}", file=sys.stderr)
    raise SystemExit(1)


RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 1.0


def assert_zip_file(zip_path: Path, url: str) -> None:
    if zipfile.is_zipfile(zip_path):
        return

    preview = zip_path.read_bytes()[:200]
    text_preview = preview.decode("utf-8", errors="replace").replace("\n", "\\n")
    die(
        "download nao retornou um ZIP valido: "
        f"{url}. Inicio da resposta: {text_preview!r}"
    )


def download_zip(url: str, zip_path: Path, token: str) -> None:
    headers = {"User-Agent": "nvim-zip-bootstrap/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    ssl_context = ssl._create_unverified_context()
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = Request(url=url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=180, context=ssl_context) as response:
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise HTTPError(url, status, f"status {status}", response.headers, None)
                with zip_path.open("wb") as handle:
                    shutil.copyfileobj(response, handle, length=1024 * 64)
                assert_zip_file(zip_path, url)
                return
        except HTTPError as error:
            last_error = error
            if error.code not in RETRYABLE_HTTP_STATUS or attempt == MAX_ATTEMPTS:
                break
        except (URLError, TimeoutError, socket.timeout, ConnectionError) as error:
            last_error = error
            if attempt == MAX_ATTEMPTS:
                break
        time.sleep(BACKOFF_SECONDS * attempt)

    die(
        "falha no download por ZIP: "
        f"{last_error}. Verifique HTTPS_PROXY/HTTP_PROXY/NO_PROXY, certificados e o host em --github-base."
    )


def extract_and_move(zip_path: Path, destination: Path, url: str) -> None:
    # Extrai num diretorio temporario no MESMO filesystem do destino (irmao de
    # destination.parent) e faz a troca com os.replace (rename atomico). So
    # remove a copia antiga apos a nova estar no lugar.
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".gh-zip-", dir=str(destination.parent)))
    try:
        with zipfile.ZipFile(zip_path, "r") as zipped:
            zipped.extractall(staging)

        entries = [path for path in staging.iterdir() if not path.name.startswith(".__")]
        directories = [entry for entry in entries if entry.is_dir()]
        if len(directories) != 1:
            die(f"arquivo zip com formato inesperado: {url}")

        source = directories[0]
        new_path = destination.parent / f"{destination.name}.new-{os.getpid()}"
        if new_path.exists() or new_path.is_symlink():
            if new_path.is_dir() and not new_path.is_symlink():
                shutil.rmtree(new_path)
            else:
                new_path.unlink()
        os.replace(str(source), str(new_path))  # mesmo fs -> rename atomico

        old_path: Path | None = None
        if destination.exists() or destination.is_symlink():
            old_path = destination.parent / f"{destination.name}.old-{os.getpid()}"
            if old_path.exists() or old_path.is_symlink():
                if old_path.is_dir() and not old_path.is_symlink():
                    shutil.rmtree(old_path)
                else:
                    old_path.unlink()
            os.replace(str(destination), str(old_path))

        os.replace(str(new_path), str(destination))

        if old_path is not None:
            if old_path.is_dir() and not old_path.is_symlink():
                shutil.rmtree(old_path, ignore_errors=True)
            else:
                old_path.unlink()
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main() -> None:
    if len(sys.argv) != 5:
        die(
            "uso: python scripts/github_zip_download_extract.py "
            "<url> <zipPath> <extractRoot> <destination>"
        )

    url = sys.argv[1]
    zip_path = Path(sys.argv[2])
    # sys.argv[3] (extractRoot) mantido no contrato de CLI por compatibilidade,
    # mas nao e mais usado: a extracao agora usa um staging no mesmo filesystem
    # do destino (ver extract_and_move).
    destination = Path(sys.argv[4])

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    token = os.environ.get("GITHUB_TOKEN", "").strip()

    download_zip(url, zip_path, token)
    extract_and_move(zip_path, destination, url)


if __name__ == "__main__":
    main()
