#!/bin/bash
# Measure kernel vs user-space CPU time during parallel CSV bulk-insert (no indexes).
cd "$(dirname "$0")/.."

BIN="./shard-db"
TOTAL=1000000
CHUNK=100000
CONNS=10
SPLITS=${SPLITS:-256}
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))
PROFILE_SECS=8

FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'

recreate_object_noidx() {
    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null
}

fire_parallel_inserts() {
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
}

if [ ! -f /tmp/shard-db_par_0.csv ]; then
    TOTAL=$TOTAL CHUNK=$CHUNK python3 bench/_gen_invoices.py || exit 1
fi

$BIN stop 2>/dev/null || true; sleep 0.3
$BIN start; sleep 0.5
SERVER_PID=$(cat "$DB_ROOT/.shard-db.lock" 2>/dev/null | tr -d '[:space:]')
echo "Server PID: $SERVER_PID"

echo ""
echo "=== Kernel + user task-clock — parallel CSV × 10 conns, no indexes ==="
recreate_object_noidx
( fire_parallel_inserts ) &
WORK_PID=$!
# Both user+kernel (no :u) and user-only for comparison
perf stat -p $SERVER_PID \
  -e task-clock -e task-clock:u \
  -e context-switches -e context-switches:u \
  -e page-faults -e page-faults:u \
  -e cpu-migrations \
  -- sleep $PROFILE_SECS
wait $WORK_PID

echo ""
echo "=== perf trace syscall summary during bulk-insert ==="
recreate_object_noidx
( fire_parallel_inserts ) &
WORK_PID=$!
perf trace -s -p $SERVER_PID -- sleep $PROFILE_SECS 2>&1 | tail -30
wait $WORK_PID

$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
$BIN stop
