#!/bin/bash
set -e

ROOT_DIR=${1:-.}

cd "$ROOT_DIR"
EXCLUDE="CMakeLists.txt"
tar --exclude=$EXCLUDE -czvf pack.tar.gz src/ inc/ 2>/dev/null
echo "$PWD/pack.tar.gz"
