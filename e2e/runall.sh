#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

./smoke_test.py
./sharding_smoke_test.py
./repeated_chunk_regression_test.py
./pagination_integration_test.py
./multi_object_integration_test.py
./range_get_integration_test.py
