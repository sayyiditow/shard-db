#!/bin/bash
# Sweep inner Phase-2 parallelism: run parallel CSV no-idx bench at THREADS=1,2,4,8,16.
cd "$(dirname "$0")/.."

BIN="./shard-db"
TOTAL=1000000
CHUNK=100000
CONNS=10
SPLITS=${SPLITS:-256}
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))

FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'

if [ ! -f /tmp/shard-db_par_0.csv ]; then
    TOTAL=$TOTAL CHUNK=$CHUNK python3 bench/_gen_invoices.py || exit 1
fi

run_bench() {
    local threads=$1
    $BIN stop 2>/dev/null || true; sleep 0.3
    # Override THREADS for this run
    sed -i "s/^export THREADS=.*/export THREADS=$threads/" db.env
    $BIN start; sleep 0.5

    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null

    local START=$(date +%s.%N)
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
    local END=$(date +%s.%N)
    awk "BEGIN{printf \"%.3f\", $END - $START}"

    $BIN stop 2>/dev/null
}

echo "Parallel CSV 1M records, 10 conns, SPLITS=$SPLITS, NO indexes"
echo "THREADS | wall (best of 3)"
echo "--------+----"

for T in 1 2 4 8 16; do
    BEST=9999
    for run in 1 2 3; do
        T_RESULT=$(run_bench $T)
        awk -v cur="$BEST" -v new="$T_RESULT" 'BEGIN{if(new+0 < cur+0) print new; else print cur}' > /tmp/_best
        BEST=$(cat /tmp/_best)
    done
    rate=$(awk -v t="$BEST" 'BEGIN{printf "%.0f", 1000000/t}')
    printf "%7d | %6ss   (%s k/sec)\n" $T $BEST $((rate/1000))
done

# Restore
sed -i 's/^export THREADS=.*/export THREADS=0/' db.env
echo "THREADS restored to 0 (auto)"
