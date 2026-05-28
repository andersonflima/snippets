#!/usr/bin/env python3
"""Download and extract a GitHub branch ZIP with proxy-aware retries."""

from __future__ import annotations

import shutil
import sys
import zipfile
import os
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def die(message: str) -> None:
    print(f"[github-zip-download-extract] erro: {message}", file=sys.stderr)
    raise SystemExit(1)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_zip(url: str, zip_path: Path, token: str) -> None:
    headers = {"User-Agent": "nvim-zip-bootstrap/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    session = build_session()
    try:
        response = session.get(
            url,
            headers=headers,
            allow_redirects=True,
            timeout=(15, 180),
            stream=True,
        )
        if response.status_code < 200 or response.status_code >= 300:
            die(f"falha no download ({response.status_code}) para {url}")

        with zip_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    handle.write(chunk)
    except requests.RequestException as error:
        die(
            "falha no download por ZIP: "
            f"{error}. Verifique HTTPS_PROXY/HTTP_PROXY/NO_PROXY e o host em --github-base."
        )
    finally:
        session.close()


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
