#!/bin/bash
# Profile parallel CSV bulk-insert with 14 pre-existing indexes.
cd "$(dirname "$0")/.."

BIN="./shard-db"
TOTAL=1000000
CHUNK=100000
CONNS=10
SPLITS=${SPLITS:-256}
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))
PROFILE_SECS=10        # long enough to cover the ~6s bulk-insert

FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'
INDEXES_JSON='["buyerId","status","irbmStatus","supplierId","invoiceDate","number","batchNumber","validationDate","submissionDate","createdAt","irbmStatus+pdfSent","status+source","status+createdAt","status+invoiceDate"]'

recreate_object() {
    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":$INDEXES_JSON}" > /dev/null
}

fire_parallel_inserts() {
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
}

if [ ! -f /tmp/shard-db_par_0.csv ]; then
    echo "Generating $NCHUNKS chunks of $CHUNK records..."
    TOTAL=$TOTAL CHUNK=$CHUNK python3 bench/_gen_invoices.py || { echo "gen failed"; exit 1; }
fi
ls -lh /tmp/shard-db_par_0.csv

$BIN stop 2>/dev/null || true
sleep 0.3
$BIN start
sleep 0.5
SERVER_PID=$(cat "$DB_ROOT/.shard-db.lock" 2>/dev/null | tr -d '[:space:]')
echo "Server PID: $SERVER_PID"
if ! kill -0 $SERVER_PID 2>/dev/null; then echo "PID invalid — abort"; exit 1; fi

echo ""
echo "=== perf stat — parallel CSV × $CONNS conns, 14 indexes ==="
recreate_object
# Fire the work in the background, then perf-stat the server for a fixed window.
( fire_parallel_inserts ) &
WORK_PID=$!
perf stat -p $SERVER_PID -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses -- sleep $PROFILE_SECS
wait $WORK_PID

echo ""
echo "=== perf record — same scenario ==="
recreate_object
( fire_parallel_inserts ) &
WORK_PID=$!
perf record -F 997 -g -p $SERVER_PID -o /tmp/perf.data -- sleep $PROFILE_SECS 2>&1 | tail -5
wait $WORK_PID

echo ""
echo "--- top 30 symbols by self% ---"
perf report -i /tmp/perf.data --stdio --no-children -n 2>/dev/null \
  | awk '/^# Overhead/{hdr=1} hdr{print} /^$/ && hdr{exit}' | head -45

echo ""
echo "--- top 30 by inclusive (children %) ---"
perf report -i /tmp/perf.data --stdio -n 2>/dev/null \
  | awk '/^# Children/{hdr=1} hdr{print} /^$/ && hdr{exit}' | head -45

$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
$BIN stop
