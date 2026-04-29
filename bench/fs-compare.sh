#!/bin/bash
# Compare filesystems by running parallel invoice bench on each.
# Usage: fs-compare.sh <DB_ROOT>
set -e
cd "$(dirname "$0")/.."

ROOT=${1:?"DB_ROOT required"}
echo "=== Using DB_ROOT=$ROOT ==="

# Save current config
cp db.env .db.env.bak
sed -i "s|^export DB_ROOT=.*|export DB_ROOT=\"$ROOT\"|" db.env

# Prep directory
mkdir -p "$ROOT"
rm -rf "$ROOT"/*
echo "default" > "$ROOT/dirs.conf"

./shard-db stop 2>/dev/null || true; sleep 0.5
./shard-db start
sleep 1

FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'

# Data
if [ ! -f /tmp/shard-db_par_0.csv ]; then
    TOTAL=1000000 CHUNK=100000 python3 bench/_gen_invoices.py > /dev/null
fi

run_par_csv_noidx() {
    ./shard-db query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$ROOT/default/bench"
    sed -i '/^default:bench:/d' "$ROOT/schema.conf" 2>/dev/null
    ./shard-db query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":256,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null
    local START=$(date +%s.%N)
    for i in $(seq 0 9); do
        while [ $(jobs -r | wc -l) -ge 10 ]; do sleep 0.02; done
        ./shard-db query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
    local END=$(date +%s.%N)
    awk "BEGIN{printf \"%.3f\", $END - $START}"
}

echo "Parallel CSV 10×100k invoice, no indexes (3 runs):"
for run in 1 2 3; do
    T=$(run_par_csv_noidx)
    echo "  run $run: ${T}s"
done

./shard-db stop 2>/dev/null
mv .db.env.bak db.env
