#!/usr/bin/env bash
set -euo pipefail
SQL_FILE="$1"
if ! grep -q "datacenter = 'DC_ONPREM'" $SQL_FILE && ! grep -q "datacenter = 'DC_AZURE'" $SQL_FILE; then
  echo "ERROR: No datacenter pin found in $SQL_FILE" >&2
  exit 2
fi
if grep -q "cassandra_onprem_sink" $SQL_FILE && grep -q "cassandra_cloud_sink" $SQL_FILE; then
  echo "ERROR: SQL file contains both onprem and cloud sinks - split jobs required" >&2
  exit 3
fi
echo "CI validation passed for $SQL_FILE"
