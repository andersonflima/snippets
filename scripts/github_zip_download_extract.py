#!/usr/bin/env python3
"""Download and extract a GitHub branch ZIP with proxy-aware retries."""

from __future__ import annotations

import shutil
import sys
import zipfile
import os
import socket
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener


def die(message: str) -> None:
    print(f"[github-zip-download-extract] erro: {message}", file=sys.stderr)
    raise SystemExit(1)


RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 1.0


def download_zip(url: str, zip_path: Path, token: str) -> None:
    headers = {"User-Agent": "nvim-zip-bootstrap/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    opener = build_opener()
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = Request(url=url, headers=headers, method="GET")
        try:
            with opener.open(request, timeout=180) as response:
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise HTTPError(url, status, f"status {status}", response.headers, None)
                with zip_path.open("wb") as handle:
                    shutil.copyfileobj(response, handle, length=1024 * 64)
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


def extract_and_move(zip_path: Path, extract_root: Path, destination: Path, url: str) -> None:
    with zipfile.ZipFile(zip_path, "r") as zipped:
        zipped.extractall(extract_root)

    entries = [path for path in extract_root.iterdir() if not path.name.startswith(".__")]
    directories = [entry for entry in entries if entry.is_dir()]
    if len(directories) != 1:
        die(f"arquivo zip com formato inesperado: {url}")

    source = directories[0]
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    shutil.move(str(source), str(destination))


def main() -> None:
    if len(sys.argv) != 5:
        die(
            "uso: python scripts/github_zip_download_extract.py "
            "<url> <zipPath> <extractRoot> <destination>"
        )

    url = sys.argv[1]
    zip_path = Path(sys.argv[2])
    extract_root = Path(sys.argv[3])
    destination = Path(sys.argv[4])

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    extract_root.mkdir(parents=True, exist_ok=True)

    token = os.environ.get("GITHUB_TOKEN", "").strip()

    download_zip(url, zip_path, token)
    extract_and_move(zip_path, extract_root, destination, url)


if __name__ == "__main__":
    main()
