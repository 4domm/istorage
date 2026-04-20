#!/usr/bin/env bash
set -euo pipefail

e2e/smoke_test.py
e2e/sharding_smoke_test.py
e2e/repeated_chunk_regression_test.py
e2e/pagination_integration_test.py
e2e/multi_object_integration_test.py
e2e/range_get_integration_test.py
