import argparse
import hashlib
import random
import re
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


API_URL = "http://127.0.0.1:3000"
HTTP_TIMEOUT_SEC = 30.0
PUT_CONCURRENCY = 6
GET_CONCURRENCY = 6


def parse_size(value: str) -> int:
    raw = value.strip().lower()
    m = re.fullmatch(r"(\d+)\s*([a-z]*)", raw)
    if not m:
        raise argparse.ArgumentTypeError(
            "invalid size format; examples: 1024, 1kb, 256kb, 1mb, 2mib, 1gb"
        )
    amount = int(m.group(1))
    unit = m.group(2)
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1000,
        "kb": 1000,
        "m": 1000**2,
        "mb": 1000**2,
        "g": 1000**3,
        "gb": 1000**3,
        "ki": 1024,
        "kib": 1024,
        "mi": 1024**2,
        "mib": 1024**2,
        "gi": 1024**3,
        "gib": 1024**3,
    }
    mul = multipliers.get(unit)
    if mul is None:
        raise argparse.ArgumentTypeError(
            "unknown size unit; use b/kb/mb/gb or kib/mib/gib"
        )
    return amount * mul


def request(method: str, url: str, data: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, method=method)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


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


def make_payload(seed: int, object_size_bytes: int) -> bytes:
    rnd = random.Random(seed)
    return bytes(rnd.getrandbits(8) for _ in range(object_size_bytes))


def key_for(i: int) -> str:
    return f"bench/obj-{i:08d}.bin"


def run_phase(name: str, concurrency: int, fn, count: int, object_size_bytes: int) -> tuple[int, int, list[str]]:
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
    print_stats(name, latencies, ok, err, ok * object_size_bytes, wall)
    return ok, err, failed


def cleanup(api_url: str, bucket: str, object_count: int) -> None:
    for i in range(object_count):
        request("DELETE", f"{api_url}/{bucket}/{key_for(i)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--object-count", type=int, required=True)
    parser.add_argument("--object-size-bytes", type=parse_size, required=True)
    args = parser.parse_args()

    object_count = args.object_count
    object_size_bytes = args.object_size_bytes
    if object_count <= 0 or object_size_bytes <= 0:
        raise SystemExit("object-count and object-size-bytes must be > 0")

    bucket = f"bench-bucket-{random.randint(100000, 999999)}"

    print("Benchmark configuration")
    print("  MODE: custom")
    print(f"  TARGET: {API_URL}")
    print(f"  BUCKET: {bucket}")
    print(f"  OBJECT_COUNT: {object_count}")
    print(f"  OBJECT_SIZE_BYTES: {object_size_bytes}")
    print(f"  PUT_CONCURRENCY: {PUT_CONCURRENCY}")
    print(f"  GET_CONCURRENCY: {GET_CONCURRENCY}")

    def put_one(i: int) -> tuple[bool, float, str]:
        key = key_for(i)
        payload = make_payload(i, object_size_bytes)
        started = time.perf_counter()
        status, _ = request("PUT", f"{API_URL}/{bucket}/{key}", payload)
        return status in (200, 201), time.perf_counter() - started, key

    def get_one(i: int) -> tuple[bool, float, str]:
        key = key_for(i)
        expected_hash = hashlib.sha256(make_payload(i, object_size_bytes)).hexdigest()
        started = time.perf_counter()
        status, body = request("GET", f"{API_URL}/{bucket}/{key}")
        latency = time.perf_counter() - started
        if status != 200:
            return False, latency, key
        return hashlib.sha256(body).hexdigest() == expected_hash, latency, key

    put_ok, put_err, put_failed = run_phase("PUT phase", PUT_CONCURRENCY, put_one, object_count, object_size_bytes)
    if put_err > 0:
        print(f"\nPUT failed keys sample: {put_failed[:10]}")
        cleanup(API_URL, bucket, object_count)
        raise SystemExit(1)

    get_ok, get_err, get_failed = run_phase("GET phase", GET_CONCURRENCY, get_one, object_count, object_size_bytes)
    if get_err > 0:
        print(f"\nGET failed keys sample: {get_failed[:10]}")
        cleanup(API_URL, bucket, object_count)
        raise SystemExit(1)

    cleanup(API_URL, bucket, object_count)
    print("\nBenchmark completed successfully")
    print(f"  uploaded: {put_ok}")
    print(f"  downloaded: {get_ok}")


if __name__ == "__main__":
    main()
