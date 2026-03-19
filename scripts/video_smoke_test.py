#!/usr/bin/env python3

import hashlib
import os
import tempfile
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", "video-bucket")
KEY = os.environ.get("KEY", "video/test.mp4")
VIDEO_PATH = os.environ.get("VIDEO_PATH")


def request(method: str, url: str, data: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def assert_status(actual: int, expected: tuple[int, ...], action: str) -> None:
    if actual not in expected:
        raise SystemExit(f"{action} failed, expected {expected}, got {actual}")


def file_sha256(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def download_to_file(url: str, dst_path: str) -> int:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req) as response, open(dst_path, "wb") as out:
            status = response.status
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
            return status
    except urllib.error.HTTPError as exc:
        return exc.code


def main() -> None:
    if not VIDEO_PATH:
        raise SystemExit("VIDEO_PATH is required, example: VIDEO_PATH=/path/to/video.mp4")
    if not os.path.isfile(VIDEO_PATH):
        raise SystemExit(f"VIDEO_PATH does not exist or is not a file: {VIDEO_PATH}")

    object_url = f"{API_URL}/{BUCKET}/{KEY}"
    source_size = os.path.getsize(VIDEO_PATH)
    source_hash = file_sha256(VIDEO_PATH)

    print(f"Uploading video {VIDEO_PATH} ({source_size} bytes) to {object_url}")
    with open(VIDEO_PATH, "rb") as handle:
        status, _ = request("PUT", object_url, handle.read())
    assert_status(status, (200, 201), "video upload")

    with tempfile.TemporaryDirectory(prefix="sstorage-video-") as workdir:
        downloaded_path = os.path.join(workdir, "downloaded_video.bin")

        print("Downloading video")
        status = download_to_file(object_url, downloaded_path)
        assert_status(status, (200,), "video download")

        downloaded_size = os.path.getsize(downloaded_path)
        downloaded_hash = file_sha256(downloaded_path)
        if downloaded_hash != source_hash:
            raise SystemExit(
                "video content mismatch after download "
                f"(src sha256={source_hash}, dst sha256={downloaded_hash})"
            )

        print(
            "Video integrity OK "
            f"(size={downloaded_size} bytes, sha256={downloaded_hash})"
        )

    print("Deleting video")
    status, _ = request("DELETE", object_url)
    assert_status(status, (204,), "video delete")

    print("Verifying video is deleted")
    status, _ = request("GET", object_url)
    assert_status(status, (404,), "video verify delete")

    print("Video smoke test passed")


if __name__ == "__main__":
    main()
