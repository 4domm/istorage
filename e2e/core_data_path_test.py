#!/usr/bin/env python3

import base64
import hashlib
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
METADATA_URL = os.environ.get("METADATA_URL", "http://127.0.0.1:3001").rstrip("/")
BUCKET = os.environ.get("BUCKET", f"core-bucket-{random.randint(100000, 999999)}")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(256 * 1024)))
WINDOW = int(os.environ.get("GET_BATCH_WINDOW", "16"))
REPEAT_COUNT = int(os.environ.get("REPEAT_COUNT", str(WINDOW + 4)))

PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wn8q9sAAAAASUVORK5CYII="
)


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
        raise SystemExit(f"{action} content mismatch, expected {len(expected)} bytes, got {len(actual)} bytes")


def encode_key_path(key: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="") for part in key.split("/"))


def header_get(headers: dict[str, str], name: str) -> str | None:
    target = name.lower()
    for k, v in headers.items():
        if k.lower() == target:
            return v
    return None


def object_url(key: str) -> str:
    return f"{API_URL}/{BUCKET}/{key}"


def metadata_object_url(key: str) -> str:
    return f"{METADATA_URL}/objects/{BUCKET}/{encode_key_path(key)}"


def roundtrip(key: str, payload: bytes, action: str) -> None:
    url = object_url(key)
    status, _, _ = request("PUT", url, payload)
    assert_status(status, (200, 201), f"{action} put")
    status, body, _ = request("GET", url)
    assert_status(status, (200,), f"{action} get")
    assert_bytes(body, payload, f"{action} get")
    status, _, _ = request("DELETE", url)
    assert_status(status, (204,), f"{action} delete")
    status, _, _ = request("GET", url)
    assert_status(status, (404,), f"{action} verify delete")


def test_smoke_roundtrip() -> None:
    text_payload = (
        f"smoke test\n"
        f"timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n"
        f"random: {random.randint(1000, 999999)}-{random.randint(1000, 999999)}\n"
    ).encode("utf-8")
    roundtrip("smoke/test.txt", text_payload, "smoke text")
    roundtrip("smoke/image.png", PNG_BYTES, "smoke image")


def test_overwrite_and_conditional_get() -> None:
    key = "conditional/object.txt"
    url = object_url(key)
    payload_v1 = b"conditional-get-v1\n"
    payload_v2 = b"conditional-get-v2\n"

    status, _, headers = request("PUT", url, payload_v1)
    assert_status(status, (200, 201), "conditional put v1")
    etag_v1 = header_get(headers, "ETag")
    if not etag_v1:
        raise SystemExit("conditional put v1 missing ETag")

    status, body, _ = request("GET", url, headers={"If-None-Match": etag_v1})
    assert_status(status, (304,), "conditional get match v1")
    if body:
        raise SystemExit("conditional get match v1 expected empty body")

    status, body, _ = request("GET", url, headers={"If-None-Match": "\"not-matching\""})
    assert_status(status, (200,), "conditional get miss v1")
    assert_bytes(body, payload_v1, "conditional get miss v1")

    status, _, headers = request("PUT", url, payload_v2)
    assert_status(status, (200, 201), "conditional put v2")
    etag_v2 = header_get(headers, "ETag")
    if not etag_v2:
        raise SystemExit("conditional put v2 missing ETag")
    if etag_v2 == etag_v1:
        raise SystemExit("conditional overwrite did not change ETag")

    status, body, _ = request("GET", url, headers={"If-None-Match": etag_v1})
    assert_status(status, (200,), "conditional old etag after overwrite")
    assert_bytes(body, payload_v2, "conditional old etag after overwrite")

    status, body, _ = request("GET", url, headers={"If-None-Match": etag_v2})
    assert_status(status, (304,), "conditional get match v2")
    if body:
        raise SystemExit("conditional get match v2 expected empty body")

    status, _, _ = request("DELETE", url)
    assert_status(status, (204,), "conditional delete")
    status, _, _ = request("GET", url)
    assert_status(status, (404,), "conditional verify delete")


def test_large_object_integrity_with_manifest() -> None:
    key = "regression/repeated.bin"
    url = object_url(key)
    metadata_url = metadata_object_url(key)
    block = (b"ABCD1234" * (CHUNK_SIZE // 8 + 1))[:CHUNK_SIZE]
    payload = block * REPEAT_COUNT

    status, _, _ = request("PUT", url, payload)
    assert_status(status, (200, 201), "large put")

    status, body, _ = request("GET", metadata_url)
    assert_status(status, (200,), "large metadata get")
    parsed = json.loads(body.decode("utf-8"))
    manifest = parsed.get("manifest", [])
    if len(manifest) < 2:
        raise SystemExit(f"large metadata manifest too short: {len(manifest)}")

    status, downloaded, _ = request("GET", url)
    assert_status(status, (200,), "large get")
    if downloaded != payload:
        src = hashlib.sha256(payload).hexdigest()
        dst = hashlib.sha256(downloaded).hexdigest()
        raise SystemExit(
            f"large get mismatch src_sha256={src} dst_sha256={dst} src_len={len(payload)} dst_len={len(downloaded)}"
        )

    status, _, _ = request("DELETE", url)
    assert_status(status, (204,), "large delete")
    status, _, _ = request("GET", url)
    assert_status(status, (404,), "large verify delete")


def main() -> None:
    test_smoke_roundtrip()
    test_overwrite_and_conditional_get()
    test_large_object_integrity_with_manifest()


if __name__ == "__main__":
    main()
