#!/bin/bash
# Compare bulk-insert with and without prealloc_mb.
cd "$(dirname "$0")/.."

BIN="./shard-db"
TOTAL=1000000
CHUNK=100000
CONNS=10
SPLITS=${SPLITS:-256}
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))

FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'

fire() {
    local START=$(date +%s.%N)
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.02; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
    local END=$(date +%s.%N)
    awk "BEGIN{printf \"%.3f\", $END - $START}"
}

run_scenario() {
    local label="$1"
    local prealloc="$2"

    $BIN stop 2>/dev/null || true; sleep 0.3
    $BIN start; sleep 0.5

    # Fresh object
    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":$SPLITS,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null

    # Patch schema.conf to add prealloc_mb and restart to reload the cache
    if [ "$prealloc" != "0" ]; then
        $BIN stop; sleep 0.3
        # Line format: default:bench:<splits>:<max_key>[:<prealloc>]
        sed -i "s/^default:bench:[0-9]*:[0-9]*.*/default:bench:$SPLITS:64:$prealloc/" "$DB_ROOT/schema.conf"
        $BIN start; sleep 0.5
        grep '^default:bench:' "$DB_ROOT/schema.conf" >&2
    fi

    local RESULTS=""
    for run in 1 2 3; do
        # reset object between runs
        $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
        rm -f "$DB_ROOT/default/bench"/data/*.bin 2>/dev/null
        local t=$(fire)
        RESULTS="$RESULTS $t"
    done
    echo "$label (prealloc_mb=$prealloc):$RESULTS"
    $BIN stop
}

if [ ! -f /tmp/shard-db_par_0.csv ]; then
    TOTAL=$TOTAL CHUNK=$CHUNK python3 bench/_gen_invoices.py || exit 1
fi

echo "Parallel CSV 1M records, 10 conns, SPLITS=$SPLITS, NO indexes"
echo "================================================================"

run_scenario "baseline" 0
run_scenario "prealloc 16 MB" 16

echo ""
echo "Disk size on prealloc=16 run:"
du -sh "$DB_ROOT/default/bench/" 2>/dev/null

$BIN stop 2>/dev/null
$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
