#!/usr/bin/env python3

import json
import os
import random
import urllib.error
import urllib.parse
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", f"pagination-bucket-{random.randint(100000, 999999)}")
OBJECT_COUNT = int(os.environ.get("OBJECT_COUNT", "7"))
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "3"))
PREFIX = os.environ.get("KEY_PREFIX", "paging")


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


def put_object(key: str, payload: bytes) -> None:
    status, _ = request("PUT", f"{API_URL}/{BUCKET}/{key}", payload)
    assert_status(status, (200, 201), f"upload {key}")


def delete_object(key: str) -> None:
    status, _ = request("DELETE", f"{API_URL}/{BUCKET}/{key}")
    assert_status(status, (204,), f"delete {key}")


def main() -> None:
    keys = [f"{PREFIX}/obj-{i:03d}.txt" for i in range(OBJECT_COUNT)]
    for key in keys:
        put_object(key, f"payload::{key}\n".encode("utf-8"))

    listed: list[str] = []
    cursor: str | None = None

    while True:
        url = f"{API_URL}/{BUCKET}?limit={PAGE_LIMIT}"
        if cursor is not None:
            url = f"{url}&cursor={urllib.parse.quote(cursor, safe='')}"

        status, body = request("GET", url)
        assert_status(status, (200,), "list bucket page")
        parsed = json.loads(body.decode("utf-8"))
        page_keys = parsed.get("keys", [])
        next_cursor = parsed.get("next_cursor")
        if not isinstance(page_keys, list):
            raise SystemExit("invalid list response: keys is not a list")
        listed.extend(page_keys)

        if next_cursor is None:
            break
        if not isinstance(next_cursor, str) or not next_cursor:
            raise SystemExit("invalid list response: next_cursor is invalid")
        cursor = next_cursor

    expected = sorted(keys)
    actual = sorted(listed)
    if actual != expected:
        raise SystemExit(f"pagination mismatch: expected={expected}, got={actual}")

    for key in keys:
        delete_object(key)

    print("Pagination integration test passed")


if __name__ == "__main__":
    main()
