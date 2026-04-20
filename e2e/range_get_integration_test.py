#!/usr/bin/env python3

import os
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", "range-bucket")
KEY = os.environ.get("KEY", "range/object.bin")


def request(
    method: str,
    url: str,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes, dict[str, str]]:
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, response.read(), dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers.items())


def assert_status(actual: int, expected: tuple[int, ...], action: str) -> None:
    if actual not in expected:
        raise SystemExit(f"{action} failed, expected {expected}, got {actual}")


def assert_bytes(actual: bytes, expected: bytes, action: str) -> None:
    if actual != expected:
        raise SystemExit(
            f"{action} content mismatch, expected {len(expected)} bytes, got {len(actual)} bytes"
        )


def object_url() -> str:
    return f"{API_URL}/{BUCKET}/{KEY}"


def main() -> None:
    url = object_url()
    payload = bytes((i % 251 for i in range(2 * 1024 * 1024 + 12345)))
    size = len(payload)

    print(f"Uploading payload ({size} bytes) to {url}")
    status, _, _ = request("PUT", url, payload)
    assert_status(status, (200, 201), "upload")

    print("Checking full GET")
    status, body, _ = request("GET", url)
    assert_status(status, (200,), "full get")
    assert_bytes(body, payload, "full get")

    print("Checking Range bytes=0-1023")
    status, body, headers = request("GET", url, headers={"Range": "bytes=0-1023"})
    assert_status(status, (206,), "range 0-1023")
    assert_bytes(body, payload[0:1024], "range 0-1023")
    if headers.get("Content-Range") != f"bytes 0-1023/{size}":
        raise SystemExit(f"unexpected Content-Range for bytes=0-1023: {headers.get('Content-Range')}")

    print("Checking Range bytes=1048500-1048700 (cross-chunk)")
    start = 1_048_500
    end = 1_048_700
    status, body, headers = request("GET", url, headers={"Range": f"bytes={start}-{end}"})
    assert_status(status, (206,), "cross-chunk range")
    assert_bytes(body, payload[start : end + 1], "cross-chunk range")
    if headers.get("Content-Range") != f"bytes {start}-{end}/{size}":
        raise SystemExit(f"unexpected Content-Range for cross-chunk range: {headers.get('Content-Range')}")

    print("Checking Range bytes=1048576- (open-ended)")
    start = 1_048_576
    status, body, headers = request("GET", url, headers={"Range": f"bytes={start}-"})
    assert_status(status, (206,), "open-ended range")
    assert_bytes(body, payload[start:], "open-ended range")
    if headers.get("Content-Range") != f"bytes {start}-{size - 1}/{size}":
        raise SystemExit(f"unexpected Content-Range for open-ended range: {headers.get('Content-Range')}")

    print("Checking Range bytes=-4096 (suffix)")
    suffix = 4096
    status, body, headers = request("GET", url, headers={"Range": f"bytes=-{suffix}"})
    assert_status(status, (206,), "suffix range")
    assert_bytes(body, payload[-suffix:], "suffix range")
    if headers.get("Content-Range") != f"bytes {size - suffix}-{size - 1}/{size}":
        raise SystemExit(f"unexpected Content-Range for suffix range: {headers.get('Content-Range')}")

    print("Checking unsatisfiable Range bytes=size-")
    status, _, headers = request("GET", url, headers={"Range": f"bytes={size}-"})
    assert_status(status, (416,), "unsatisfiable range")
    if headers.get("Content-Range") != f"bytes */{size}":
        raise SystemExit(
            f"unexpected Content-Range for unsatisfiable range: {headers.get('Content-Range')}"
        )

    print("Cleaning up")
    status, _, _ = request("DELETE", url)
    assert_status(status, (204,), "delete")

    print("Range GET integration test passed")


if __name__ == "__main__":
    main()
