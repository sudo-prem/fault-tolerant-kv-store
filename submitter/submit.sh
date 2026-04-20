#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/env"

SERVER=${PREGRADE_SERVER:-http://68.181.216.50:8081}

FILE_PATH=$("${SCRIPT_DIR}/packer.sh" "${PROJ_ROOT}" | tail -1)

echo "Uploading submission..."
echo "Group: ${GROUP_NAME:-not set}"
echo ""

curl -s -N --no-buffer \
    -F "secret_key=${SECRET}" \
    -F "group_name=${GROUP_NAME}" \
    -F "file=@${FILE_PATH}" \
    --max-time 1800 \
    "${SERVER}/grade" | while IFS= read -r line; do
    if [ "$line" = "===RESULT_JSON===" ]; then
        read -r json_line
        echo ""
        echo "=== Final Result ==="
        echo "$json_line" | python3 -m json.tool 2>/dev/null || echo "$json_line"
        break
    fi
    echo "$line"
done

rm -f "${FILE_PATH}" 2>/dev/null || true
