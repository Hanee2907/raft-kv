#!/usr/bin/env bash
# Compiles with plain javac (no dependencies) and runs the whole suite.
# Usage: ./run-tests.sh                 run everything
#        ./run-tests.sh test-name ...   run only the named tests
set -euo pipefail
cd "$(dirname "$0")"

rm -rf out
mkdir -p out/main out/test
javac -Xlint:all -d out/main $(find src/main -name '*.java')
javac -Xlint:all -cp out/main -d out/test $(find src/test -name '*.java')
exec java -cp out/main:out/test raftkv.AllTests "$@"
