#!/bin/bash
# Run parallel bench with BENCH_DIAG=1 — server in foreground so stderr captured.
cd "$(dirname "$0")/.."

BIN="./shard-db"
CONNS=10
NCHUNKS=10
SPLITS=${SPLITS:-256}
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'

start_fg_server() {
    $BIN stop 2>/dev/null || true
    # Any stale daemon lingering?
    fuser -k -s "$(grep PORT db.env | head -1 | tr -cd '0-9')"/tcp 2>/dev/null
    sleep 0.5
    rm -f "$DB_ROOT/.shard-db.lock" 2>/dev/null
    BENCH_DIAG=1 $BIN server > /tmp/diag_srv.out 2> /tmp/diag.err &
    SERVER_PID=$!
    sleep 0.8
}

stop_fg_server() {
    kill -TERM $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null
}

run_scenario() {
    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null
}

# ---- Parallel ----
start_fg_server
run_scenario
: > /tmp/diag.err  # clear diag buffer after create-object noise
echo "=== Parallel CSV: 10 conns × 100k ==="
START=$(date +%s.%N)
for i in $(seq 0 $((NCHUNKS-1))); do
    while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
    $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
done
wait
END=$(date +%s.%N)
ELAPSED=$(awk "BEGIN{printf \"%.3f\", $END - $START}")
echo "wall=${ELAPSED}s"
sleep 0.2
stop_fg_server
echo ""
echo "DIAG lines per call (parallel):"
grep "^DIAG" /tmp/diag.err
echo ""

# ---- Single ----
cat /tmp/shard-db_par_*.csv > /tmp/single_1m.csv
start_fg_server
run_scenario
: > /tmp/diag.err
echo "=== Single call: 1M records ==="
START=$(date +%s.%N)
$BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/single_1m.csv\",\"delimiter\":\"|\"}" > /dev/null
END=$(date +%s.%N)
ELAPSED=$(awk "BEGIN{printf \"%.3f\", $END - $START}")
echo "wall=${ELAPSED}s"
sleep 0.2
stop_fg_server
echo ""
echo "DIAG line (single):"
grep "^DIAG" /tmp/diag.err

rm -f /tmp/single_1m.csv
$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
