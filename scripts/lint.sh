#!/bin/bash
set -euxo pipefail

black .
pflake8 .
mypy --install-types .

# disable tracing -- too verbose
set +x
# find all .h, .hpp, and .cpp files excepting third party files
files=$(find ./scaler -path './scaler/io/ymq/third_party' -prune -o \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) -print0 | xargs -0)
set -x

echo "running clang format on $(wc -w <<< $files) files"
clang-format -i -style file -- $files
