#!/usr/bin/env python3

import concurrent.futures
import hashlib
import os
import random
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", f"concurrent-bucket-{random.randint(100000, 999999)}")
KEY = os.environ.get("KEY", "race/object.bin")
WRITERS = int(os.environ.get("WRITERS", "12"))
PAYLOAD_SIZE = int(os.environ.get("PAYLOAD_SIZE", str(512 * 1024)))


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


def header_get(headers: dict[str, str], name: str) -> str | None:
    target = name.lower()
    for k, v in headers.items():
        if k.lower() == target:
            return v
    return None


def main() -> None:
    url = f"{API_URL}/{BUCKET}/{KEY}"
    payloads: list[bytes] = []
    for i in range(WRITERS):
        prefix = f"writer-{i:02d}|".encode("utf-8")
        body = os.urandom(max(PAYLOAD_SIZE - len(prefix), 1))
        payloads.append(prefix + body)

    def put_payload(payload: bytes) -> tuple[int, str | None]:
        status, _, headers = request("PUT", url, payload)
        return status, header_get(headers, "ETag")

    with concurrent.futures.ThreadPoolExecutor(max_workers=WRITERS) as ex:
        futures = [ex.submit(put_payload, p) for p in payloads]
        results = [f.result() for f in futures]

    for idx, (status, etag) in enumerate(results):
        assert_status(status, (200, 201), f"concurrent put #{idx}")
        if not etag:
            raise SystemExit(f"concurrent put #{idx} missing ETag")

    status, body, headers = request("GET", url)
    assert_status(status, (200,), "concurrent final get")
    if body not in payloads:
        raise SystemExit("concurrent final object is not equal to any writer payload")

    expected_etag = '"' + hashlib.sha256(body).hexdigest() + '"'
    actual_etag = header_get(headers, "ETag")
    if actual_etag != expected_etag:
        raise SystemExit(f"concurrent final ETag mismatch expected={expected_etag} got={actual_etag}")

    status, _, _ = request("DELETE", url)
    assert_status(status, (204,), "concurrent cleanup delete")
    status, _, _ = request("GET", url)
    assert_status(status, (404,), "concurrent cleanup verify")


if __name__ == "__main__":
    main()
