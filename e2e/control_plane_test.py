#!/usr/bin/env python3

import json
import os
import random
import urllib.error
import urllib.parse
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
METADATA_URL = os.environ.get("METADATA_URL", "http://127.0.0.1:3001").rstrip("/")
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "3"))
OBJECT_COUNT = int(os.environ.get("OBJECT_COUNT", "7"))
SHARD_BYTES = int(os.environ.get("FILE_SIZE_BYTES", str(4 * 1024 * 1024)))


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


def encode_key_path(key: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="") for part in key.split("/"))


def test_sharding_distribution() -> None:
    bucket = f"sharding-bucket-{random.randint(100000, 999999)}"
    key = "sharding/random.bin"
    payload = os.urandom(SHARD_BYTES)
    object_url = f"{API_URL}/{bucket}/{key}"
    metadata_url = f"{METADATA_URL}/objects/{bucket}/{encode_key_path(key)}"

    status, _ = request("PUT", object_url, payload)
    assert_status(status, (200, 201), "sharding put")

    status, body = request("GET", metadata_url)
    assert_status(status, (200,), "sharding metadata get")
    parsed = json.loads(body.decode("utf-8"))
    manifest = parsed.get("manifest", [])
    if not manifest:
        raise SystemExit("sharding metadata manifest empty")

    counts: dict[str, int] = {}
    for chunk_ref in manifest:
        shard_refs = chunk_ref.get("shards") or []
        if shard_refs:
            for shard in shard_refs:
                node_id = shard.get("node_id")
                if not node_id:
                    raise SystemExit(f"sharding shard missing node_id: {shard}")
                counts[node_id] = counts.get(node_id, 0) + 1
            continue
        node_id = chunk_ref.get("node_id")
        if not node_id:
            raise SystemExit(f"sharding chunk missing node_id: {chunk_ref}")
        counts[node_id] = counts.get(node_id, 0) + 1

    used_nodes = {node_id for node_id, c in counts.items() if c > 0}
    if len(used_nodes) < 2:
        raise SystemExit(f"sharding expected >=2 nodes, got {sorted(used_nodes)}")

    status, downloaded = request("GET", object_url)
    assert_status(status, (200,), "sharding get")
    if downloaded != payload:
        raise SystemExit("sharding get payload mismatch")

    status, _ = request("DELETE", object_url)
    assert_status(status, (204,), "sharding delete")
    status, _ = request("GET", object_url)
    assert_status(status, (404,), "sharding verify delete")


def test_bucket_pagination() -> None:
    bucket = f"pagination-bucket-{random.randint(100000, 999999)}"
    prefix = "paging"
    keys = [f"{prefix}/obj-{i:03d}.txt" for i in range(OBJECT_COUNT)]

    for key in keys:
        status, _ = request("PUT", f"{API_URL}/{bucket}/{key}", f"payload::{key}\n".encode("utf-8"))
        assert_status(status, (200, 201), f"pagination put {key}")

    listed: list[str] = []
    cursor: str | None = None
    while True:
        url = f"{API_URL}/{bucket}?limit={PAGE_LIMIT}"
        if cursor is not None:
            url = f"{url}&cursor={urllib.parse.quote(cursor, safe='')}"
        status, body = request("GET", url)
        assert_status(status, (200,), "pagination list")
        parsed = json.loads(body.decode("utf-8"))
        page_keys = parsed.get("keys", [])
        next_cursor = parsed.get("next_cursor")
        if not isinstance(page_keys, list):
            raise SystemExit("pagination keys is not list")
        listed.extend(page_keys)
        if next_cursor is None:
            break
        if not isinstance(next_cursor, str) or not next_cursor:
            raise SystemExit("pagination next_cursor invalid")
        cursor = next_cursor

    if sorted(listed) != sorted(keys):
        raise SystemExit(f"pagination mismatch expected={sorted(keys)} got={sorted(listed)}")

    for key in keys:
        status, _ = request("DELETE", f"{API_URL}/{bucket}/{key}")
        assert_status(status, (204,), f"pagination delete {key}")


def main() -> None:
    test_sharding_distribution()
    test_bucket_pagination()


if __name__ == "__main__":
    main()
