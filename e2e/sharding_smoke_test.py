#!/usr/bin/env python3

import json
import os
import urllib.parse
import urllib.error
import urllib.request


API_URL = os.environ.get("API_URL", "http://127.0.0.1:3000").rstrip("/")
METADATA_URL = os.environ.get("METADATA_URL", "http://127.0.0.1:3001").rstrip("/")
BUCKET = os.environ.get("BUCKET", "sharding-bucket")
KEY = os.environ.get("KEY", "sharding/random.bin")
FILE_SIZE_BYTES = int(os.environ.get("FILE_SIZE_BYTES", str(4 * 1024 * 1024)))
EXPECTED_NODES = ("chunker-a", "chunker-b")


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


def main() -> None:
    source_payload = os.urandom(FILE_SIZE_BYTES)
    object_url = f"{API_URL}/{BUCKET}/{KEY}"
    encoded_key = urllib.parse.quote(KEY, safe="")
    metadata_url = f"{METADATA_URL}/objects/{BUCKET}/{encoded_key}"

    print(f"Uploading {FILE_SIZE_BYTES} bytes to {object_url}")
    status, _ = request("PUT", object_url, source_payload)
    assert_status(status, (200, 201), "sharding upload")

    print("Fetching metadata manifest")
    status, body = request("GET", metadata_url)
    assert_status(status, (200,), "metadata fetch")
    metadata_obj = json.loads(body.decode("utf-8"))
    manifest = metadata_obj.get("manifest", [])
    if not manifest:
        raise SystemExit("Manifest is empty")

    counts: dict[str, int] = {}
    for chunk_ref in manifest:
        shard_refs = chunk_ref.get("shards") or []
        if shard_refs:
            for shard in shard_refs:
                node_id = shard.get("node_id")
                if not node_id:
                    raise SystemExit(f"Shard entry missing node_id: {shard}")
                counts[node_id] = counts.get(node_id, 0) + 1
            continue

        node_id = chunk_ref.get("node_id")
        if not node_id:
            raise SystemExit(f"Manifest entry missing node_id: {chunk_ref}")
        counts[node_id] = counts.get(node_id, 0) + 1

    print("Node distribution:")
    for node in EXPECTED_NODES:
        print(f"  {node}: {counts.get(node, 0)}")

    used_nodes = {node_id for node_id, count in counts.items() if count > 0}
    if len(used_nodes) < 2:
        raise SystemExit(
            f"Expected chunks to be distributed across at least 2 nodes, got {sorted(used_nodes)}"
        )

    print("Downloading object for integrity check")
    status, downloaded = request("GET", object_url)
    assert_status(status, (200,), "sharding download")
    if downloaded != source_payload:
        raise SystemExit("Downloaded sharded object does not match uploaded payload")

    print("Deleting object")
    status, _ = request("DELETE", object_url)
    assert_status(status, (204,), "sharding delete")

    print("Verifying object is deleted")
    status, _ = request("GET", object_url)
    assert_status(status, (404,), "sharding verify delete")

    print("Sharding smoke test passed")


if __name__ == "__main__":
    main()
