#!/usr/bin/env python3

import json
import os
import random
import urllib.error
import urllib.parse
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
BUCKET = os.environ.get("BUCKET", f"keys-bucket-{random.randint(100000, 999999)}")


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


def list_bucket_keys(limit: int = 1000) -> list[str]:
    status, body = request("GET", f"{API_URL}/{BUCKET}?limit={limit}")
    assert_status(status, (200,), "list bucket")
    parsed = json.loads(body.decode("utf-8"))
    keys = parsed.get("keys")
    if not isinstance(keys, list):
        raise SystemExit("list bucket returned non-list keys")
    return keys


def main() -> None:
    keys = [
        "enc/space key.txt",
        "enc/plus+key.txt",
        "enc/percent%key.txt",
        "enc/question?key.txt",
        "enc/hash#key.txt",
        "enc/brackets[key].txt",
    ]

    for idx, key in enumerate(keys):
        payload = f"payload-{idx}-{key}\n".encode("utf-8")
        url = f"{API_URL}/{BUCKET}/{urllib.parse.quote(key, safe='/')}"
        status, _ = request("PUT", url, payload)
        assert_status(status, (200, 201), f"put {key}")

        status, body = request("GET", url)
        assert_status(status, (200,), f"get {key}")
        if body != payload:
            raise SystemExit(f"payload mismatch for key={key}")

    listed_before_delete = set(list_bucket_keys())
    missing = [key for key in keys if key not in listed_before_delete]
    if missing:
        raise SystemExit(f"list bucket missing keys: {missing}")

    target = keys[0]
    target_url = f"{API_URL}/{BUCKET}/{urllib.parse.quote(target, safe='/')}"
    status, _ = request("DELETE", target_url)
    assert_status(status, (204,), "first delete")
    status, _ = request("DELETE", target_url)
    assert_status(status, (404,), "second delete")
    status, _ = request("GET", target_url)
    assert_status(status, (404,), "get after delete")

    listed_after_delete = set(list_bucket_keys())
    if target in listed_after_delete:
        raise SystemExit("deleted key still present in list")

    for key in keys[1:]:
        url = f"{API_URL}/{BUCKET}/{urllib.parse.quote(key, safe='/')}"
        status, _ = request("DELETE", url)
        assert_status(status, (204,), f"cleanup delete {key}")


if __name__ == "__main__":
    main()
