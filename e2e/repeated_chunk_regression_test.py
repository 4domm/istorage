#!/usr/bin/env python3

import hashlib
import json
import os
import urllib.parse
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
METADATA_URL = os.environ.get("METADATA_URL", "http://127.0.0.1:3001").rstrip("/")
BUCKET = os.environ.get("BUCKET", "regression-bucket")
KEY = os.environ.get("KEY", "regression/repeated.bin")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(256 * 1024)))
WINDOW = int(os.environ.get("GET_BATCH_WINDOW", "16"))
REPEAT_COUNT = int(os.environ.get("REPEAT_COUNT", str(WINDOW + 4)))


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


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> None:
    block = (b"ABCD1234" * (CHUNK_SIZE // 8 + 1))[:CHUNK_SIZE]
    payload = block * REPEAT_COUNT

    object_url = f"{API_URL}/{BUCKET}/{KEY}"
    encoded_key = urllib.parse.quote(KEY, safe="")
    metadata_url = f"{METADATA_URL}/objects/{BUCKET}/{encoded_key}"

    print(
        f"Uploading repeated-chunk payload: chunk_size={CHUNK_SIZE}, repeats={REPEAT_COUNT}, bytes={len(payload)}"
    )
    status, _ = request("PUT", object_url, payload)
    assert_status(status, (200, 201), "upload")

    print("Checking manifest is present for large object")
    status, body = request("GET", metadata_url)
    assert_status(status, (200,), "metadata fetch")
    meta = json.loads(body.decode("utf-8"))
    manifest = meta.get("manifest", [])
    if len(manifest) < 2:
        raise SystemExit(f"unexpected manifest length for large object: {len(manifest)}")

    print("Downloading and comparing bytes")
    status, downloaded = request("GET", object_url)
    assert_status(status, (200,), "download")
    src_hash = sha256_bytes(payload)
    dst_hash = sha256_bytes(downloaded)
    if downloaded != payload:
        raise SystemExit(
            "regression failed: downloaded payload differs "
            f"(src_sha256={src_hash}, dst_sha256={dst_hash}, src_len={len(payload)}, dst_len={len(downloaded)})"
        )

    print("Deleting test object")
    status, _ = request("DELETE", object_url)
    assert_status(status, (204,), "delete")

    print("Verifying deletion")
    status, _ = request("GET", object_url)
    assert_status(status, (404,), "verify delete")

    print("Repeated-chunk regression test passed")


if __name__ == "__main__":
    main()
