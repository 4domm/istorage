#!/usr/bin/env bash
set -euo pipefail

scripts/smoke_test.py
scripts/sharding_smoke_test.py
scripts/repeated_chunk_regression_test.py
scripts/pagination_integration_test.py
scripts/multi_object_integration_test.py
