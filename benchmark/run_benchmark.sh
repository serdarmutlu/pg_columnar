#!/usr/bin/env bash
# =============================================================================
# run_benchmark.sh – orchestrates the full pg_columnar benchmark
#
# Usage:
#   ./benchmark/run_benchmark.sh [OPTIONS]
#
# Options:
#   -h HOST        PostgreSQL host          (default: localhost)
#   -p PORT        PostgreSQL port          (default: 5432)
#   -U USER        PostgreSQL user          (default: $USER)
#   -d DATABASE    Database name            (default: postgres)
#   -n ROWS        Number of rows to insert (default: 1000000)
#   -s STEP        Run only one step: setup | load | bench | explain | all
#                  (default: all)
#   --skip-setup   Skip table creation (use existing tables)
#   --skip-load    Skip data loading (use existing data)
#   --help         Show this help message
# =============================================================================

set -euo pipefail

# ---------- Defaults ---------------------------------------------------------
PG_HOST="${PGHOST:-localhost}"
PG_PORT="${PGPORT:-5432}"
PG_USER="${PGUSER:-$USER}"
PG_DATABASE="${PGDATABASE:-postgres}"
ROW_COUNT=1000000
STEP="all"
SKIP_SETUP=0
SKIP_LOAD=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$RESULTS_DIR/benchmark_${TIMESTAMP}.log"

# ---------- Argument parsing -------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h) PG_HOST="$2";     shift 2 ;;
        -p) PG_PORT="$2";     shift 2 ;;
        -U) PG_USER="$2";     shift 2 ;;
        -d) PG_DATABASE="$2"; shift 2 ;;
        -n) ROW_COUNT="$2";   shift 2 ;;
        -s) STEP="$2";        shift 2 ;;
        --skip-setup) SKIP_SETUP=1; shift ;;
        --skip-load)  SKIP_LOAD=1;  shift ;;
        --help)
            sed -n '2,/^# ===/p' "$0" | sed 's/^# \{0,3\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

# ---------- Helper -----------------------------------------------------------
psql_run() {
    local label="$1"; shift
    echo ""
    echo "================================================================"
    echo " $label"
    echo "================================================================"
    psql \
        -h "$PG_HOST" \
        -p "$PG_PORT" \
        -U "$PG_USER" \
        -d "$PG_DATABASE" \
        -v ROW_COUNT="$ROW_COUNT" \
        -v ON_ERROR_STOP=1 \
        "$@"
}

print_header() {
    echo "================================================================"
    echo " pg_columnar Performance Benchmark"
    echo " $(date)"
    echo " Host:     $PG_HOST:$PG_PORT"
    echo " Database: $PG_DATABASE"
    echo " User:     $PG_USER"
    echo " Rows:     $ROW_COUNT"
    echo " Log:      $LOG_FILE"
    echo "================================================================"
}

# ---------- Main -------------------------------------------------------------
print_header | tee "$LOG_FILE"

run_all() {
    if [[ $SKIP_SETUP -eq 0 ]]; then
        psql_run "Step 1 – Setup (extension + tables)" \
            -f "$SCRIPT_DIR/01_setup.sql" | tee -a "$LOG_FILE"
    else
        echo "(Skipping setup)"
    fi

    if [[ $SKIP_LOAD -eq 0 ]]; then
        psql_run "Step 2 – Load $ROW_COUNT rows" \
            -f "$SCRIPT_DIR/02_load_data.sql" | tee -a "$LOG_FILE"
    else
        echo "(Skipping data load)"
    fi

    psql_run "Step 3 – Benchmark queries" \
        -f "$SCRIPT_DIR/03_benchmark.sql" | tee -a "$LOG_FILE"

    psql_run "Step 4 – Query plans (EXPLAIN ANALYZE)" \
        -f "$SCRIPT_DIR/04_explain.sql" | tee -a "$LOG_FILE"
}

case "$STEP" in
    all)
        run_all
        ;;
    setup)
        psql_run "Setup" -f "$SCRIPT_DIR/01_setup.sql" | tee -a "$LOG_FILE"
        ;;
    load)
        psql_run "Load data ($ROW_COUNT rows)" \
            -f "$SCRIPT_DIR/02_load_data.sql" | tee -a "$LOG_FILE"
        ;;
    bench)
        psql_run "Benchmark" -f "$SCRIPT_DIR/03_benchmark.sql" | tee -a "$LOG_FILE"
        ;;
    explain)
        psql_run "Explain" -f "$SCRIPT_DIR/04_explain.sql" | tee -a "$LOG_FILE"
        ;;
    *)
        echo "Unknown step: $STEP  (valid: setup | load | bench | explain | all)" >&2
        exit 1
        ;;
esac

echo ""
echo "================================================================"
echo " Done. Full log saved to: $LOG_FILE"
echo "================================================================"
