#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/env"

SERVER=${PREGRADE_SERVER:-http://68.181.216.50:8080}

echo "Using secret: ${SECRET:0:8}..."

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -F "secret_key=${SECRET}" \
    "${SERVER}/check")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" -eq 200 ]; then
    echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
else
    echo "ERROR (HTTP ${HTTP_CODE}): $BODY"
fi
