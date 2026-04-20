#!/usr/bin/env python3

import base64
import os
import random
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", f"multi-bucket-{random.randint(100000, 999999)}")

PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wn8q9sAAAAASUVORK5CYII="
)


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


def put_and_check(key: str, payload: bytes, action: str) -> None:
    url = f"{API_URL}/{BUCKET}/{key}"
    status, _ = request("PUT", url, payload)
    assert_status(status, (200, 201), f"{action} upload")

    status, body = request("GET", url)
    assert_status(status, (200,), f"{action} download")
    if body != payload:
        raise SystemExit(f"{action} payload mismatch")


def main() -> None:
    text_key = "objects/readme.txt"
    image_key = "objects/pixel.png"
    blob_key = "objects/blob.bin"

    text_payload = b"multi object integration test\n"
    blob_payload = os.urandom(2 * 1024 * 1024 + 123)

    put_and_check(text_key, text_payload, "text")
    put_and_check(image_key, PNG_BYTES, "image")
    put_and_check(blob_key, blob_payload, "blob")

    updated_text = b"multi object integration test updated\n"
    put_and_check(text_key, updated_text, "text overwrite")

    for key in (text_key, image_key, blob_key):
        url = f"{API_URL}/{BUCKET}/{key}"
        status, _ = request("DELETE", url)
        assert_status(status, (204,), f"delete {key}")
        status, _ = request("GET", url)
        assert_status(status, (404,), f"verify delete {key}")

    print("Multi-object integration test passed")


if __name__ == "__main__":
    main()
