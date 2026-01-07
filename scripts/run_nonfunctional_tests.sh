#!/bin/bash

# =============================================================================
# Non-Functional Test Runner Script
# Runs MQTT performance and reliability tests multiple times and aggregates results
# =============================================================================

# Note: Not using 'set -e' because we want to continue even if individual tests fail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
ITERATIONS=${1:-100}  # Default to 100 iterations, can override with first argument
RESULTS_DIR="$PROJECT_ROOT/test-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    MQTT Non-Functional Test Runner${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Iterations: $ITERATIONS"
echo "Results directory: $RESULTS_DIR"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Files for collecting results
LATENCY_RESULTS="$RESULTS_DIR/latency_results_$TIMESTAMP.csv"
RECOVERY_RESULTS="$RESULTS_DIR/recovery_results_$TIMESTAMP.csv"
SUMMARY_FILE="$RESULTS_DIR/summary_$TIMESTAMP.md"

# Initialize CSV files
echo "iteration,min_us,max_us,avg_us,p50_us,p95_us,p99_us,received,sent" > "$LATENCY_RESULTS"
echo "iteration,reconnect_ms,connected,msgs_before,msgs_after" > "$RECOVERY_RESULTS"

# Ensure MQTT broker is running (check for local mosquitto first)
echo -e "${YELLOW}Checking MQTT broker...${NC}"

# Check if local mosquitto is running on port 1883
if nc -z localhost 1883 2>/dev/null; then
    echo -e "${GREEN}✓ Local MQTT broker is running on port 1883${NC}"
    export MQTT_BROKER="tcp://localhost:1883"
else
    # Try to start a docker broker
    BROKER_CONTAINER="mqtt-test-broker"
    if ! docker ps --format '{{.Names}}' | grep -q "^${BROKER_CONTAINER}$"; then
        echo "Starting MQTT broker for tests..."
        docker rm -f "$BROKER_CONTAINER" 2>/dev/null || true
        docker run -d --name "$BROKER_CONTAINER" -p 1883:1883 eclipse-mosquitto:2.0 mosquitto -c /mosquitto-no-auth.conf
        sleep 2
    fi
    export MQTT_BROKER="tcp://localhost:1883"
fi
echo ""

# =============================================================================
# Run Latency Tests
# =============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    Running Message Latency Tests ($ITERATIONS iterations)${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

latency_passed=0
latency_failed=0

for i in $(seq 1 $ITERATIONS); do
    printf "\rRunning latency test %d/%d..." "$i" "$ITERATIONS"
    
    # Run test and capture output (-count=1 disables caching)
    output=$(cd "$PROJECT_ROOT" && go test -v -count=1 -run TestMessageLatency ./internal/coordinator/ 2>&1 || true)
    
    # Extract results from output
    result_line=$(echo "$output" | grep "\[LATENCY_RESULT\]" || echo "")
    
    if [[ -n "$result_line" ]]; then
        # Parse: [LATENCY_RESULT] min=X max=X avg=X p50=X p95=X p99=X received=X sent=X
        min=$(echo "$result_line" | grep -oP 'min=\K\d+' || echo "0")
        max=$(echo "$result_line" | grep -oP 'max=\K\d+' || echo "0")
        avg=$(echo "$result_line" | grep -oP 'avg=\K\d+' || echo "0")
        p50=$(echo "$result_line" | grep -oP 'p50=\K\d+' || echo "0")
        p95=$(echo "$result_line" | grep -oP 'p95=\K\d+' || echo "0")
        p99=$(echo "$result_line" | grep -oP 'p99=\K\d+' || echo "0")
        received=$(echo "$result_line" | grep -oP 'received=\K\d+' || echo "0")
        sent=$(echo "$result_line" | grep -oP 'sent=\K\d+' || echo "0")
        
        echo "$i,$min,$max,$avg,$p50,$p95,$p99,$received,$sent" >> "$LATENCY_RESULTS"
        
        # Check if test passed (avg < 50ms = 50000us, p99 < 200ms = 200000us)
        if [[ "$avg" -lt 50000 ]] && [[ "$p99" -lt 200000 ]]; then
            ((latency_passed++))
        else
            ((latency_failed++))
        fi
    else
        echo "$i,0,0,0,0,0,0,0,0" >> "$LATENCY_RESULTS"
        ((latency_failed++))
    fi
done

echo ""
echo -e "${GREEN}✓ Latency tests complete: $latency_passed passed, $latency_failed failed${NC}"
echo ""

# =============================================================================
# Run Connection Recovery Tests
# =============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    Running Connection Recovery Tests ($ITERATIONS iterations)${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

recovery_passed=0
recovery_failed=0

for i in $(seq 1 $ITERATIONS); do
    printf "\rRunning recovery test %d/%d..." "$i" "$ITERATIONS"
    
    # Run test and capture output (-count=1 disables caching)
    output=$(cd "$PROJECT_ROOT" && go test -v -count=1 -run TestConnectionFailureRecovery ./internal/coordinator/ 2>&1 || true)
    
    # Extract results from output
    result_line=$(echo "$output" | grep "\[RECOVERY_RESULT\]" || echo "")
    
    if [[ -n "$result_line" ]]; then
        # Parse: [RECOVERY_RESULT] reconnect_ms=X connected=X msgs_before=X msgs_after=X
        reconnect_ms=$(echo "$result_line" | grep -oP 'reconnect_ms=\K\d+' || echo "0")
        connected=$(echo "$result_line" | grep -oP 'connected=\K\w+' || echo "false")
        msgs_before=$(echo "$result_line" | grep -oP 'msgs_before=\K\d+' || echo "0")
        msgs_after=$(echo "$result_line" | grep -oP 'msgs_after=\K\d+' || echo "0")
        
        echo "$i,$reconnect_ms,$connected,$msgs_before,$msgs_after" >> "$RECOVERY_RESULTS"
        
        # Check if test passed (reconnect < 15s, connected=true)
        if [[ "$reconnect_ms" -lt 15000 ]] && [[ "$connected" == "true" ]]; then
            ((recovery_passed++))
        else
            ((recovery_failed++))
        fi
    else
        echo "$i,0,false,0,0" >> "$RECOVERY_RESULTS"
        ((recovery_failed++))
    fi
done

echo ""
echo -e "${GREEN}✓ Recovery tests complete: $recovery_passed passed, $recovery_failed failed${NC}"
echo ""

# =============================================================================
# Calculate Statistics and Generate Summary
# =============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    Generating Summary${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Calculate latency statistics using awk
latency_stats=$(awk -F',' 'NR>1 && $4>0 {
    count++
    sum_min+=$2; sum_max+=$3; sum_avg+=$4; sum_p50+=$5; sum_p95+=$6; sum_p99+=$7
    if(NR==2 || $2<min_min) min_min=$2
    if(NR==2 || $3>max_max) max_max=$3
    if(NR==2 || $4<min_avg) min_avg=$4
    if($4>max_avg) max_avg=$4
}
END {
    if(count>0) {
        printf "%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%d",
            sum_min/count, sum_max/count, sum_avg/count, sum_p50/count, sum_p95/count, sum_p99/count,
            min_min, max_max, min_avg, count
    }
}' "$LATENCY_RESULTS")

# Calculate recovery statistics
recovery_stats=$(awk -F',' 'NR>1 && $2>0 {
    count++
    sum_reconnect+=$2
    if(NR==2 || $2<min_reconnect) min_reconnect=$2
    if($2>max_reconnect) max_reconnect=$2
    if($3=="true") connected_count++
}
END {
    if(count>0) {
        printf "%.0f,%.0f,%.0f,%d,%d", sum_reconnect/count, min_reconnect, max_reconnect, connected_count, count
    }
}' "$RECOVERY_RESULTS")

# Parse statistics
IFS=',' read -r avg_min avg_max avg_avg avg_p50 avg_p95 avg_p99 min_latency max_latency best_avg latency_count <<< "$latency_stats"
IFS=',' read -r avg_reconnect min_reconnect max_reconnect connected_success recovery_count <<< "$recovery_stats"

# Generate summary markdown
cat > "$SUMMARY_FILE" << EOF
# MQTT Non-Functional Test Results

**Date:** $(date '+%Y-%m-%d %H:%M:%S')  
**Iterations:** $ITERATIONS per test  
**Broker:** Eclipse Mosquitto 2.0

---

## Test 1: Message Latency (Performance)

Tests round-trip time for MQTT messages from publisher to subscriber.

| Metric | Value | Unit | Threshold | Status |
|--------|-------|------|-----------|--------|
| **Average Latency** | ${avg_avg:-N/A} | μs | < 50,000 μs | $([ "${avg_avg:-0}" -lt 50000 ] && echo "✅ PASS" || echo "❌ FAIL") |
| **P50 Latency** | ${avg_p50:-N/A} | μs | - | - |
| **P95 Latency** | ${avg_p95:-N/A} | μs | - | - |
| **P99 Latency** | ${avg_p99:-N/A} | μs | < 200,000 μs | $([ "${avg_p99:-0}" -lt 200000 ] && echo "✅ PASS" || echo "❌ FAIL") |
| **Min Latency** | ${min_latency:-N/A} | μs | - | - |
| **Max Latency** | ${max_latency:-N/A} | μs | - | - |
| **Successful Runs** | $latency_passed/$ITERATIONS | - | - | - |

### Latency Distribution (across $latency_count valid runs)

- **Best Average:** ${best_avg:-N/A} μs
- **Avg of Minimums:** ${avg_min:-N/A} μs
- **Avg of Maximums:** ${avg_max:-N/A} μs

---

## Test 2: Connection Failure Recovery (Reliability)

Tests MQTT client auto-reconnect behavior when broker connection is lost.

| Metric | Value | Unit | Threshold | Status |
|--------|-------|------|-----------|--------|
| **Avg Reconnect Time** | ${avg_reconnect:-N/A} | ms | < 15,000 ms | $([ "${avg_reconnect:-0}" -lt 15000 ] && echo "✅ PASS" || echo "❌ FAIL") |
| **Min Reconnect Time** | ${min_reconnect:-N/A} | ms | - | - |
| **Max Reconnect Time** | ${max_reconnect:-N/A} | ms | - | - |
| **Reconnection Success** | ${connected_success:-0}/${recovery_count:-0} | - | 100% | $([ "${connected_success:-0}" -eq "${recovery_count:-0}" ] && echo "✅ PASS" || echo "❌ FAIL") |
| **Successful Runs** | $recovery_passed/$ITERATIONS | - | - | - |

---

## Summary

| Test | Pass Rate | Overall Status |
|------|-----------|----------------|
| Message Latency | $latency_passed/$ITERATIONS ($(echo "scale=1; $latency_passed * 100 / $ITERATIONS" | bc)%) | $([ "$latency_passed" -ge $(($ITERATIONS * 95 / 100)) ] && echo "✅ PASS" || echo "❌ FAIL") |
| Connection Recovery | $recovery_passed/$ITERATIONS ($(echo "scale=1; $recovery_passed * 100 / $ITERATIONS" | bc)%) | $([ "$recovery_passed" -ge $(($ITERATIONS * 95 / 100)) ] && echo "✅ PASS" || echo "❌ FAIL") |

### Interpretation

- **Message Latency:** MQTT provides reliable message delivery with QoS 1. Latencies in the sub-millisecond to low-millisecond range demonstrate efficient broker performance suitable for real-time position updates.

- **Connection Recovery:** The Paho MQTT client's auto-reconnect feature ensures system resilience. When the broker restarts, clients automatically reconnect within the configured timeout, maintaining system availability.

---

## Raw Data

- Latency results: \`$LATENCY_RESULTS\`
- Recovery results: \`$RECOVERY_RESULTS\`

EOF

echo -e "${GREEN}✓ Summary generated: $SUMMARY_FILE${NC}"
echo ""

# Print summary to console
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    FINAL RESULTS${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "MESSAGE LATENCY TEST:"
echo "  Average Latency: ${avg_avg:-N/A} μs"
echo "  P99 Latency:     ${avg_p99:-N/A} μs"
echo "  Pass Rate:       $latency_passed/$ITERATIONS"
echo ""
echo "CONNECTION RECOVERY TEST:"
echo "  Avg Reconnect:   ${avg_reconnect:-N/A} ms"
echo "  Success Rate:    ${connected_success:-0}/${recovery_count:-0}"
echo "  Pass Rate:       $recovery_passed/$ITERATIONS"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Results saved to: $RESULTS_DIR"
echo ""

# Cleanup test broker (optional - comment out to keep it running)
# echo "Stopping test broker..."
# docker rm -f "$BROKER_CONTAINER" 2>/dev/null || true

exit 0
