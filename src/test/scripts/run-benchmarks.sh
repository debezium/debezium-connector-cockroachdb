#!/usr/bin/env bash
#
# Run JMH microbenchmarks for the Debezium CockroachDB connector.
#
# Usage:
#   ./run-benchmarks.sh                     # all benchmarks, all payload sizes
#   ./run-benchmarks.sh medium              # all benchmarks, medium payload only
#   ./run-benchmarks.sh medium json         # JSON output for sharing
#
# Prerequisites:
#   - Java 21+ and Maven (or use the included mvnw wrapper)
#
# The script builds the benchmark uber-jar (if needed) and runs JMH.
# Results are printed to stdout; pass "json" as the second argument to
# also write machine-readable output to benchmark-results.json.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BENCHMARK_JAR="$PROJECT_DIR/target/debezium-connector-cockroachdb-benchmarks.jar"

PAYLOAD_SIZE="${1:-small,medium,large}"
OUTPUT_FORMAT="${2:-}"

# Build the benchmark jar if it doesn't exist or source is newer
if [ ! -f "$BENCHMARK_JAR" ] || \
   [ "$(find "$PROJECT_DIR/src" -name '*.java' -newer "$BENCHMARK_JAR" 2>/dev/null | head -1)" ]; then
    echo "Building benchmark jar..."
    cd "$PROJECT_DIR"
    ./mvnw clean package -Pbenchmark -DskipTests -DskipITs -q
    echo "Build complete."
fi

echo ""
echo "Available benchmarks:"
java -jar "$BENCHMARK_JAR" -l
echo ""
echo "Running benchmarks with payloadSize=$PAYLOAD_SIZE ..."
echo ""

JMH_ARGS=(
    -p "payloadSize=$PAYLOAD_SIZE"
    -f 2
    -wi 5
    -i 5
    -t 1
)

if [ "$OUTPUT_FORMAT" = "json" ]; then
    RESULTS_FILE="$PROJECT_DIR/benchmark-results.json"
    JMH_ARGS+=(-rf json -rff "$RESULTS_FILE")
    echo "JSON results will be written to: $RESULTS_FILE"
    echo ""
fi

java -jar "$BENCHMARK_JAR" "${JMH_ARGS[@]}"

if [ "$OUTPUT_FORMAT" = "json" ] && [ -f "$RESULTS_FILE" ]; then
    echo ""
    echo "Results saved to $RESULTS_FILE"
fi
