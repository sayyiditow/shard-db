#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# shard-db invoice benchmark — realistic 64-field typed objects
# Usage: ./bench-invoice.sh [record_count] [cli|persistent]

COUNT=${1:-100000}
MODE=${2:-persistent}
SPLITS=${SPLITS:-64}
BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep PORT db.env | sed "s/.*[\"']\{0,1\}\([0-9]*\).*/\1/")
PORT=${PORT:-9199}

echo "======================================"
echo "  shard-db INVOICE benchmark ($COUNT records)"
echo "  mode: $MODE"
echo "======================================"

# Setup
grep -q "^default$" "$DB_ROOT/dirs.conf" 2>/dev/null || echo "default" >> "$DB_ROOT/dirs.conf"

$BIN stop 2>/dev/null || true
sleep 0.3
$BIN start
sleep 0.5

# Query helpers
q_cli() { $BIN query "$1"; }
if [ "$MODE" = "persistent" ]; then
    q() { echo "$1" | socat - TCP:localhost:$PORT 2>/dev/null | tr -d '\0'; }
else
    q() { $BIN query "$1"; }
fi
cleanup_conn() { :; }

# Create object with all 58 fields (columnar storage)
q_cli '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
q_cli '{"mode":"create-object","dir":"default","object":"bench","splits":'$SPLITS',"max_key":64,"fields":["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"],"indexes":[]}' > /dev/null

# Generate test data — realistic invoice objects (~1.9KB each, 58 fields)
echo "Generating $COUNT invoice records..."
python3 -c "
import json, random, hashlib
from datetime import datetime, timedelta
statuses = ['DRAFT','PENDING','APPROVED','REJECTED','CANCELLED']
irbm_statuses = ['PENDING','VALID','INVALID','CANCELLED']
currencies = ['MYR','USD','SGD','EUR','GBP']
inv_types = ['STANDARD','CREDIT_NOTE','DEBIT_NOTE','REFUND','SELF_BILLED']
freqs = ['MONTHLY','QUARTERLY','YEARLY','DAILY','WEEKLY']
suppliers = [f'SUP-{hashlib.md5(str(i).encode()).hexdigest()}' for i in range(100)]
buyers = [f'BUY-{hashlib.md5(str(i).encode()).hexdigest()}' for i in range(500)]
users = [f'user{i}@company.com' for i in range(20)]
base_dt = datetime(2024, 1, 1, 0, 0, 0)
def dtfmt(dt): return dt.strftime('%Y%m%d%H%M%S')
def dfmt(dt): return dt.strftime('%Y%m%d') + '000000'

records = []
for i in range($COUNT):
    dt = base_dt + timedelta(days=i)
    amt = round(random.uniform(100, 50000), 2)
    tax = round(amt * 0.06, 2)
    records.append({'key': f'INV-{i:07d}', 'value': {
        'buyerId': buyers[i % 500], 'version': '1.0',
        'number': f'INV-2026-{i:07d}',
        'originalReference': f'REF-2026-{i:07d}',
        'supplierId': suppliers[i % 100],
        'irbmIdentifier': f'IRBM-{i:012d}',
        'source': ['API','PORTAL','BATCH','IMPORT'][i%4],
        'createdBy': users[i%20], 'updatedBy': users[(i+1)%20],
        'irbmLongId': f'IRBM-LONG-{hashlib.md5(str(i).encode()).hexdigest()}',
        'originalReferenceNumber': f'ORN-{i:07d}',
        'batchNumber': f'BATCH-{i//1000:04d}',
        'submissionUid': hashlib.sha256(str(i).encode()).hexdigest()[:36],
        'submittedBy': users[i%20],
        'taxExemptionReason': '' if i%10 != 0 else 'Government entity',
        'requestForRejectionReason': '' if i%20 != 0 else 'Invalid details',
        'cancellationReason': '' if i%30 != 0 else 'Duplicate invoice',
        'approvedBy': users[(i+5)%20] if i%3==0 else '',
        'approvalRemarks': 'Auto-approved' if i%3==0 else '',
        'transactionCodeId': f'TC-{i%50:03d}',
        'productCodeId': f'PC-{i%200:03d}',
        'cancellationSource': '', 'internalCancellationBy': '',
        'irbmSubmissionResponse': '',
        'invoiceDate': dtfmt(dt),
        'createdAt': dtfmt(dt),
        'updatedAt': dtfmt(dt + timedelta(hours=1)),
        'requestForRejectionDate': '',
        'cancellationDate': '',
        'submissionDate': dtfmt(dt + timedelta(minutes=30)),
        'validationDate': dtfmt(dt + timedelta(hours=1)),
        'approvalDate': dtfmt(dt + timedelta(hours=2)) if i%3==0 else '',
        'billingPeriodStart': dfmt(dt - timedelta(days=30)),
        'billingPeriodEnd': dfmt(dt),
        'internalCancellationDate': '',
        'status': statuses[i%5], 'irbmStatus': irbm_statuses[i%4],
        'currencyCode': currencies[i%5],
        'invoiceType': inv_types[i%5], 'frequency': freqs[i%5],
        'consolidated': i%7==0, 'pdfSent': i%3!=0,
        'exchangeRate': 1.0 if i%5==0 else round(random.uniform(0.2, 4.5), 4),
        'totalExcludingTax': amt, 'totalIncludingTax': round(amt+tax, 2),
        'totalPayableAmount': round(amt+tax, 2), 'totalNetAmount': amt,
        'totalDiscountAmount': 0.0, 'totalFee': 0.0,
        'totalTaxAmount': tax, 'roundingAmount': 0.0,
        'amountExemptedFromTax': 0.0,
        'invoiceAdditionalDiscountAmount': 0.0,
        'invoiceAdditionalFeeAmount': 0.0,
        'totalSalesTaxable': amt, 'totalSalesTaxAmount': tax,
        'totalServiceTaxable': 0.0, 'totalServiceTaxAmount': 0.0,
        'totalTourismTaxable': 0.0, 'totalTourismTaxAmount': 0.0,
        'totalHighValueTaxable': 0.0, 'totalHighValueTaxAmount': 0.0,
        'totalLowValueTaxable': 0.0, 'totalLowValueTaxAmount': 0.0
    }})
with open('/tmp/shard-db_bench.json', 'w') as f:
    json.dump(records, f)
"

# ==================== BULK INSERT ====================
echo ""
echo "--- BULK INSERT ($COUNT records, no index) ---"
time q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_bench.json\"}" > /dev/null
q_cli '{"mode":"size","dir":"default","object":"bench"}'

# ==================== GET x1000 (pipelined via socat) ====================
echo ""
echo "--- GET x1000 (pipelined, single connection) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        printf '{"mode":"get","dir":"default","object":"bench","key":"INV-%07d"}\n' $i
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== EXISTS x1000 ====================
echo ""
echo "--- EXISTS x1000 (pipelined) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        printf '{"mode":"exists","dir":"default","object":"bench","key":"INV-%07d"}\n' $i
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== ADD ALL INDEXES (14 indexes) ====================
echo ""
echo "--- ADD INDEXES (14 indexes, single-pass) ---"
time q_cli '{"mode":"add-index","dir":"default","object":"bench","fields":["buyerId","status","irbmStatus","supplierId","invoiceDate","number","batchNumber","validationDate","submissionDate","createdAt","irbmStatus+pdfSent","status+source","status+createdAt","status+invoiceDate"]}' > /dev/null

# ==================== INDEXED SEARCH x100 (pipelined) ====================
echo ""
echo "--- INDEXED SEARCH x100 supplierId (pipelined) ---"
# Precompute supplier IDs to avoid md5sum subprocess per iteration
python3 -c "
import hashlib
for i in range(100):
    h = hashlib.md5(str(i).encode()).hexdigest()
    print('{\"mode\":\"search\",\"dir\":\"default\",\"object\":\"bench\",\"field\":\"supplierId\",\"value\":\"SUP-'+h+'\"}')
" > /tmp/shard-db_search.txt
time (cat /tmp/shard-db_search.txt | socat - TCP:localhost:$PORT > /dev/null)
rm -f /tmp/shard-db_search.txt

# ==================== BULK INSERT WITH INDEXES ====================
echo ""
echo "--- BULK INSERT ($COUNT records, 14 indexes) ---"
q_cli '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
# Reopen persistent connection after truncate
# connection refreshed per-query
printf "buyerId\nstatus\nirbmStatus\nsupplierId\ninvoiceDate\nnumber\nbatchNumber\nvalidationDate\nsubmissionDate\ncreatedAt\nirbmStatus+pdfSent\nstatus+source\nstatus+createdAt\nstatus+invoiceDate\n" > "$DB_ROOT/default/bench/indexes/index.conf"
time q_cli "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_bench.json\"}" > /dev/null
q_cli '{"mode":"size","dir":"default","object":"bench"}'
# Reopen persistent connection after bulk insert
# connection refreshed per-query

# ==================== FIND ====================
echo ""
echo "--- FIND: eq (indexed supplierId, ~10K matches) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"supplierId","op":"eq","value":"SUP-cfcd208495d565ef66e7dff9f98764da"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: eq (indexed buyerId, ~2K matches) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"buyerId","op":"eq","value":"BUY-cfcd208495d565ef66e7dff9f98764da"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: contains number (indexed leaf scan) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"number","op":"contains","value":"00050"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: contains batchNumber (indexed leaf scan) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"batchNumber","op":"contains","value":"BATCH-05"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: IN indexed (2 statuses) ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"status","op":"in","value":["APPROVED","PENDING"]}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: indexed status + non-indexed currency ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"status","op":"eq","value":"APPROVED"},{"field":"currencyCode","op":"eq","value":"MYR"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: indexed status + amount range ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"status","op":"eq","value":"APPROVED"},{"field":"totalIncludingTax","op":"gte","value":"10000"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: composite status+invoiceDate starts ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"status+invoiceDate","op":"starts","value":"APPROVED2024"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: composite status+source eq ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"status+source","op":"eq","value":"APPROVEDAPI"}],"limit":10}' > /dev/null

echo ""
echo "--- FIND: composite irbmStatus+pdfSent eq ---"
time q '{"mode":"find","dir":"default","object":"bench","criteria":[{"field":"irbmStatus+pdfSent","op":"eq","value":"VALIDtrue"}],"limit":10}' > /dev/null

# ==================== RANGE ====================
echo ""
echo "--- RANGE: indexed invoiceDate ---"
time q '{"mode":"range","dir":"default","object":"bench","field":"invoiceDate","min":"20240101000000","max":"20240201000000"}' > /dev/null

echo ""
echo "--- RANGE: indexed createdAt ---"
time q '{"mode":"range","dir":"default","object":"bench","field":"createdAt","min":"20240101000000","max":"20240115000000"}' > /dev/null

# ==================== FETCH ====================
echo ""
echo "--- FETCH: page of 100, offset 5000 ---"
time q '{"mode":"fetch","dir":"default","object":"bench","offset":"5000","limit":"100"}' > /dev/null

echo ""
echo "--- FETCH: with projection ---"
time q '{"mode":"fetch","dir":"default","object":"bench","offset":"0","limit":"100","fields":["number","status","totalIncludingTax","supplierId"]}' > /dev/null

# ==================== KEYS ====================
echo ""
echo "--- KEYS: first 100 ---"
time q '{"mode":"keys","dir":"default","object":"bench","offset":"0","limit":"100"}' > /dev/null

# ==================== SINGLE DELETE x1000 (pipelined) ====================
echo ""
echo "--- SINGLE DELETE x1000 (pipelined, 14 indexes) ---"
time (
    for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
        printf '{"mode":"delete","dir":"default","object":"bench","key":"INV-%07d"}\n' $i
    done | socat - TCP:localhost:$PORT > /dev/null
)

# ==================== BULK DELETE x1000 ====================
echo ""
echo "--- BULK DELETE x1000 (inline JSON, 14 indexes) ---"
BULK_DEL_KEYS=""
for i in $(shuf -i 0-$((COUNT-1)) -n 1000); do
    BULK_DEL_KEYS="${BULK_DEL_KEYS}$(printf '"INV-%07d",' $i)"
done
BULK_DEL_KEYS="[${BULK_DEL_KEYS%,}]"
time q_cli "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bench\",\"keys\":$BULK_DEL_KEYS}" > /dev/null

# ==================== VACUUM ====================
echo ""
echo "--- VACUUM ---"
time q_cli '{"mode":"vacuum","dir":"default","object":"bench"}' > /dev/null

# ==================== RECOUNT ====================
echo ""
echo "--- RECOUNT ---"
time q_cli '{"mode":"recount","dir":"default","object":"bench"}' > /dev/null

# ==================== DISK USAGE ====================
echo ""
echo "--- DISK USAGE ---"
du -sh "$DB_ROOT/default/bench/"

# ==================== CLEANUP ====================
cleanup_conn
q_cli '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
$BIN stop
rm -f /tmp/shard-db_bench.json

echo ""
echo "======================================"
echo "  Benchmark complete"
echo "======================================"
