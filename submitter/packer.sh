#!/bin/bash
set -e

ROOT_DIR=${1:-.}

cd "$ROOT_DIR"
tar -czvf pack.tar.gz src/ inc/ proto/ 2>/dev/null
echo "$PWD/pack.tar.gz"
