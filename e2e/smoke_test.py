#!/usr/bin/env python3

import base64
import os
import random
import time
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", "smoke-bucket")

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


def object_url(key: str) -> str:
    return f"{API_URL}/{BUCKET}/{key}"


def roundtrip_object(key: str, payload: bytes, label: str) -> None:
    url = object_url(key)

    print(f"Uploading {label} to {url}")
    status, _ = request("PUT", url, payload)
    assert_status(status, (200, 201), f"{label} upload")

    print(f"Downloading {label}")
    status, downloaded = request("GET", url)
    assert_status(status, (200,), f"{label} download")
    if downloaded != payload:
        raise SystemExit(f"{label} content mismatch after download")

    print(f"Deleting {label}")
    status, _ = request("DELETE", url)
    assert_status(status, (204,), f"{label} delete")

    print(f"Verifying {label} is deleted")
    status, _ = request("GET", url)
    assert_status(status, (404,), f"{label} verify delete")


def main() -> None:
    text_payload = (
        f"smoke test\n"
        f"timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n"
        f"random: {random.randint(1000, 999999)}-{random.randint(1000, 999999)}\n"
    ).encode("utf-8")

    roundtrip_object("smoke/test.txt", text_payload, "text object")
    roundtrip_object("smoke/image.png", PNG_BYTES, "image object")

    print("Smoke test passed")


if __name__ == "__main__":
    main()
