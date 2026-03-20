#!/usr/bin/env python3

import argparse
import hashlib
import os
import random
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Protocol


API_URL = "http://127.0.0.1:3000"
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_REGION = "us-east-1"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

BUCKET = f"bench-bucket-{random.randint(100000, 999999)}"
OBJECT_COUNT = 100
OBJECT_SIZE_BYTES = 262144*40
PUT_CONCURRENCY = 6
GET_CONCURRENCY = 6
HTTP_TIMEOUT_SEC = float(os.environ.get("HTTP_TIMEOUT_SEC", "30"))


class StorageBench(Protocol):
    def prepare(self) -> None: ...

    def put(self, key: str, payload: bytes) -> bool: ...

    def get(self, key: str) -> tuple[bool, bytes]: ...

    def delete(self, key: str) -> None: ...


class CustomStorageBench:
    def __init__(self, api_url: str, bucket: str):
        self.api_url = api_url.rstrip("/")
        self.bucket = bucket

    def _request(self, method: str, url: str, data: bytes | None = None) -> tuple[int, bytes]:
        req = urllib.request.Request(url, data=data, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def prepare(self) -> None:
        pass

    def put(self, key: str, payload: bytes) -> bool:
        status, _ = self._request("PUT", f"{self.api_url}/{self.bucket}/{key}", payload)
        return status in (200, 201)

    def get(self, key: str) -> tuple[bool, bytes]:
        status, body = self._request("GET", f"{self.api_url}/{self.bucket}/{key}")
        return status == 200, body

    def delete(self, key: str) -> None:
        self._request("DELETE", f"{self.api_url}/{self.bucket}/{key}")


class MinioStorageBench:
    def __init__(
        self,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str,
    ):
        try:
            import boto3
            from botocore.config import Config
        except Exception as exc:  # pragma: no cover
            raise SystemExit(
                "minio mode requires boto3 and botocore. Install: pip install boto3"
            ) from exc

        self.bucket = bucket
        self._region = region
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint.rstrip("/"),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def prepare(self) -> None:
        try:
            if self._region == "us-east-1":
                self._s3.create_bucket(Bucket=self.bucket)
            else:
                self._s3.create_bucket(
                    Bucket=self.bucket,
                    CreateBucketConfiguration={"LocationConstraint": self._region},
                )
        except self._s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        except self._s3.exceptions.BucketAlreadyExists:
            pass

    def put(self, key: str, payload: bytes) -> bool:
        try:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=payload)
            return True
        except Exception:
            return False

    def get(self, key: str) -> tuple[bool, bytes]:
        try:
            response = self._s3.get_object(Bucket=self.bucket, Key=key)
            return True, response["Body"].read()
        except Exception:
            return False, b""

    def delete(self, key: str) -> None:
        try:
            self._s3.delete_object(Bucket=self.bucket, Key=key)
        except Exception:
            pass


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = max(0, min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1)))))
    return sorted(values)[idx]


def fmt_ms(v: float) -> str:
    return f"{v * 1000:.2f} ms"


def print_stats(name: str, latencies: list[float], ok: int, err: int, bytes_total: int, wall: float) -> None:
    ops = ok + err
    avg = statistics.mean(latencies) if latencies else 0.0
    print(f"\n{name}")
    print(f"  ops_total: {ops}")
    print(f"  ok: {ok}")
    print(f"  err: {err}")
    print(f"  wall_time: {wall:.3f} s")
    print(f"  throughput: {ok / wall:.2f} ops/s" if wall > 0 else "  throughput: 0 ops/s")
    mib = bytes_total / (1024 * 1024)
    print(f"  data: {mib:.2f} MiB")
    print(f"  bandwidth: {mib / wall:.2f} MiB/s" if wall > 0 else "  bandwidth: 0 MiB/s")
    print(f"  latency_min: {fmt_ms(min(latencies) if latencies else 0.0)}")
    print(f"  latency_avg: {fmt_ms(avg)}")
    print(f"  latency_p50: {fmt_ms(pct(latencies, 50))}")
    print(f"  latency_p95: {fmt_ms(pct(latencies, 95))}")
    print(f"  latency_p99: {fmt_ms(pct(latencies, 99))}")
    print(f"  latency_max: {fmt_ms(max(latencies) if latencies else 0.0)}")


def make_payload(seed: int) -> bytes:
    rnd = random.Random(seed)
    return bytes(rnd.getrandbits(8) for _ in range(OBJECT_SIZE_BYTES))


def key_for(i: int) -> str:
    return f"bench/obj-{i:08d}.bin"


def run_phase(name: str, concurrency: int, fn, count: int) -> tuple[int, int, list[str], float]:
    latencies: list[float] = []
    failed: list[str] = []
    ok = 0
    err = 0
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(fn, i) for i in range(count)]
        for future in as_completed(futures):
            success, latency, key = future.result()
            latencies.append(latency)
            if success:
                ok += 1
            else:
                err += 1
                failed.append(key)
    wall = time.perf_counter() - started
    print_stats(name, latencies, ok, err, ok * OBJECT_SIZE_BYTES, wall)
    return ok, err, failed, wall


def cleanup(storage: StorageBench) -> None:
    for i in range(OBJECT_COUNT):
        storage.delete(key_for(i))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["custom", "minio"],
        default=os.environ.get("BENCH_MODE", "custom"),
        help="Benchmark target mode",
    )
    parser.add_argument("--api-url", default=API_URL, help="Custom storage API URL")
    parser.add_argument("--minio-endpoint", default=MINIO_ENDPOINT, help="MinIO S3 endpoint")
    args = parser.parse_args()

    if args.mode == "custom":
        storage: StorageBench = CustomStorageBench(args.api_url, BUCKET)
        target = args.api_url
    else:
        storage = MinioStorageBench(
            endpoint=args.minio_endpoint,
            bucket=BUCKET,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            region=MINIO_REGION,
        )
        target = args.minio_endpoint

    storage.prepare()

    print("Benchmark configuration")
    print(f"  MODE: {args.mode}")
    print(f"  TARGET: {target}")
    print(f"  BUCKET: {BUCKET}")
    print(f"  OBJECT_COUNT: {OBJECT_COUNT}")
    print(f"  OBJECT_SIZE_BYTES: {OBJECT_SIZE_BYTES}")
    print(f"  PUT_CONCURRENCY: {PUT_CONCURRENCY}")
    print(f"  GET_CONCURRENCY: {GET_CONCURRENCY}")

    def put_one(i: int) -> tuple[bool, float, str]:
        key = key_for(i)
        payload = make_payload(i)
        started = time.perf_counter()
        ok = storage.put(key, payload)
        return ok, time.perf_counter() - started, key

    def get_one(i: int) -> tuple[bool, float, str]:
        key = key_for(i)
        expected_hash = hashlib.sha256(make_payload(i)).hexdigest()
        started = time.perf_counter()
        ok, body = storage.get(key)
        latency = time.perf_counter() - started
        if not ok:
            return False, latency, key
        return hashlib.sha256(body).hexdigest() == expected_hash, latency, key

    put_ok, put_err, put_failed, _ = run_phase("PUT phase", PUT_CONCURRENCY, put_one, OBJECT_COUNT)
    if put_err > 0:
        print(f"\nPUT failed keys sample: {put_failed[:10]}")
        cleanup(storage)
        raise SystemExit(1)

    get_ok, get_err, get_failed, _ = run_phase("GET phase", GET_CONCURRENCY, get_one, OBJECT_COUNT)
    if get_err > 0:
        print(f"\nGET failed keys sample: {get_failed[:10]}")
        cleanup(storage)
        raise SystemExit(1)

    cleanup(storage)
    print("\nBenchmark completed successfully")
    print(f"  uploaded: {put_ok}")
    print(f"  downloaded: {get_ok}")


if __name__ == "__main__":
    main()
