#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Parallel insert benchmark — 3 tests only
# Usage: ./bench-parallel.sh [total_records] [chunk_size] [connections]

TOTAL=${1:-1000000}
CHUNK=${2:-200000}
CONNS=${3:-5}
BIN="./shard-db"
DB_ROOT=$(grep DB_ROOT db.env | sed "s/.*[\"']\(.*\)[\"']/\1/")
PORT=$(grep PORT db.env | sed "s/.*[\"']\{0,1\}\([0-9]*\).*/\1/")
PORT=${PORT:-9199}

NCHUNKS=$(( (TOTAL + CHUNK - 1) / CHUNK ))
FIELDS_JSON='["buyerId:varchar:40","version:varchar:5","number:varchar:20","originalReference:varchar:20","supplierId:varchar:40","irbmIdentifier:varchar:20","source:varchar:8","createdBy:varchar:25","updatedBy:varchar:25","irbmLongId:varchar:40","originalReferenceNumber:varchar:15","batchNumber:varchar:12","submissionUid:varchar:36","submittedBy:varchar:25","taxExemptionReason:varchar:20","requestForRejectionReason:varchar:20","cancellationReason:varchar:20","approvedBy:varchar:25","approvalRemarks:varchar:15","transactionCodeId:varchar:8","productCodeId:varchar:8","cancellationSource:varchar:10","internalCancellationBy:varchar:10","irbmSubmissionResponse:varchar:10","invoiceDate:datetime","createdAt:datetime","updatedAt:datetime","requestForRejectionDate:datetime","cancellationDate:datetime","submissionDate:datetime","validationDate:datetime","approvalDate:datetime","billingPeriodStart:datetime","billingPeriodEnd:datetime","internalCancellationDate:datetime","status:varchar:12","irbmStatus:varchar:12","currencyCode:varchar:4","invoiceType:varchar:15","frequency:varchar:10","consolidated:bool","pdfSent:bool","exchangeRate:double","totalExcludingTax:currency","totalIncludingTax:currency","totalPayableAmount:currency","totalNetAmount:currency","totalDiscountAmount:currency","totalFee:currency","totalTaxAmount:currency","roundingAmount:currency","amountExemptedFromTax:currency","invoiceAdditionalDiscountAmount:currency","invoiceAdditionalFeeAmount:currency","totalSalesTaxable:currency","totalSalesTaxAmount:currency","totalServiceTaxable:currency","totalServiceTaxAmount:currency","totalTourismTaxable:currency","totalTourismTaxAmount:currency","totalHighValueTaxable:currency","totalHighValueTaxAmount:currency","totalLowValueTaxable:currency","totalLowValueTaxAmount:currency"]'
INDEXES_JSON='["buyerId","status","irbmStatus","supplierId","invoiceDate","number","batchNumber","validationDate","submissionDate","createdAt","irbmStatus+pdfSent","status+source","status+createdAt","status+invoiceDate"]'

echo "======================================"
echo "  Parallel Insert Benchmark"
echo "  $TOTAL records, ${CHUNK}/chunk, $CONNS connections"
echo "  $NCHUNKS chunks"
echo "======================================"

$BIN stop 2>/dev/null || true
sleep 0.3
$BIN start
sleep 0.5

# Generate chunks
echo "Generating $NCHUNKS chunks ($CHUNK records each)..."
TOTAL=$TOTAL CHUNK=$CHUNK python3 << 'PYEOF'
import json, hashlib, random, os
from datetime import datetime, timedelta

total = int(os.environ['TOTAL'])
chunk_size = int(os.environ['CHUNK'])

statuses = ['DRAFT','PENDING','APPROVED','REJECTED','CANCELLED']
irbm_statuses = ['PENDING','VALID','INVALID','CANCELLED']
currencies = ['MYR','USD','SGD','EUR','GBP']
inv_types = ['STANDARD','CREDIT_NOTE','DEBIT_NOTE','REFUND','SELF_BILLED']
freqs = ['MONTHLY','QUARTERLY','YEARLY','DAILY','WEEKLY']
suppliers = [f'SUP-{hashlib.md5(str(i).encode()).hexdigest()}' for i in range(100)]
buyers = [f'BUY-{hashlib.md5(str(i).encode()).hexdigest()}' for i in range(500)]
users = [f'user{i}@company.com' for i in range(20)]
base_dt = datetime(2024, 1, 1)
def dtfmt(dt): return dt.strftime('%Y%m%d%H%M%S')
def dfmt(dt): return dt.strftime('%Y%m%d') + '000000'

for c in range(0, total, chunk_size):
    end = min(c + chunk_size, total)
    idx = c // chunk_size
    records = []
    for i in range(c, end):
        dt = base_dt + timedelta(days=i)
        amt = round(random.uniform(100, 50000), 2)
        tax = round(amt * 0.06, 2)
        records.append({'id': f'INV-{i:07d}', 'data': {
            'buyerId': buyers[i%500], 'version': '1.0',
            'number': f'INV-2026-{i:07d}', 'originalReference': f'REF-2026-{i:07d}',
            'supplierId': suppliers[i%100], 'irbmIdentifier': f'IRBM-{i:012d}',
            'source': ['API','PORTAL','BATCH','IMPORT'][i%4],
            'createdBy': users[i%20], 'updatedBy': users[(i+1)%20],
            'irbmLongId': f'IRBM-LONG-{hashlib.md5(str(i).encode()).hexdigest()}',
            'originalReferenceNumber': f'ORN-{i:07d}', 'batchNumber': f'BATCH-{i//1000:04d}',
            'submissionUid': hashlib.sha256(str(i).encode()).hexdigest()[:36],
            'submittedBy': users[i%20],
            'taxExemptionReason': '' if i%10 != 0 else 'Government entity',
            'requestForRejectionReason': '' if i%20 != 0 else 'Invalid details',
            'cancellationReason': '' if i%30 != 0 else 'Duplicate invoice',
            'approvedBy': users[(i+5)%20] if i%3==0 else '',
            'approvalRemarks': 'Auto-approved' if i%3==0 else '',
            'transactionCodeId': f'TC-{i%50:03d}', 'productCodeId': f'PC-{i%200:03d}',
            'cancellationSource': '', 'internalCancellationBy': '', 'irbmSubmissionResponse': '',
            'invoiceDate': dtfmt(dt), 'createdAt': dtfmt(dt),
            'updatedAt': dtfmt(dt + timedelta(hours=1)),
            'requestForRejectionDate': '', 'cancellationDate': '',
            'submissionDate': dtfmt(dt + timedelta(minutes=30)),
            'validationDate': dtfmt(dt + timedelta(hours=1)),
            'approvalDate': dtfmt(dt + timedelta(hours=2)) if i%3==0 else '',
            'billingPeriodStart': dfmt(dt - timedelta(days=30)), 'billingPeriodEnd': dfmt(dt),
            'internalCancellationDate': '',
            'status': statuses[i%5], 'irbmStatus': irbm_statuses[i%4],
            'currencyCode': currencies[i%5], 'invoiceType': inv_types[i%5], 'frequency': freqs[i%5],
            'consolidated': i%7==0, 'pdfSent': i%3!=0,
            'exchangeRate': 1.0 if i%5==0 else round(random.uniform(0.2, 4.5), 4),
            'totalExcludingTax': amt, 'totalIncludingTax': round(amt+tax, 2),
            'totalPayableAmount': round(amt+tax, 2), 'totalNetAmount': amt,
            'totalDiscountAmount': 0.0, 'totalFee': 0.0,
            'totalTaxAmount': tax, 'roundingAmount': 0.0, 'amountExemptedFromTax': 0.0,
            'invoiceAdditionalDiscountAmount': 0.0, 'invoiceAdditionalFeeAmount': 0.0,
            'totalSalesTaxable': amt, 'totalSalesTaxAmount': tax,
            'totalServiceTaxable': 0.0, 'totalServiceTaxAmount': 0.0,
            'totalTourismTaxable': 0.0, 'totalTourismTaxAmount': 0.0,
            'totalHighValueTaxable': 0.0, 'totalHighValueTaxAmount': 0.0,
            'totalLowValueTaxable': 0.0, 'totalLowValueTaxAmount': 0.0
        }})
    with open(f'/tmp/shard-db_par_{idx}.json', 'w') as f:
        json.dump(records, f, separators=(',', ':'))

    # CSV version — key|fields in fields.conf order
    with open(f'/tmp/shard-db_par_{idx}.csv', 'w') as f:
        for r in records:
            d = r['data']
            vals = [d['buyerId'], d['version'], d['number'], d['originalReference'],
                    d['supplierId'], d['irbmIdentifier'], d['source'],
                    d['createdBy'], d['updatedBy'], d['irbmLongId'],
                    d['originalReferenceNumber'], d['batchNumber'],
                    d['submissionUid'], d['submittedBy'],
                    d['taxExemptionReason'], d['requestForRejectionReason'],
                    d['cancellationReason'], d['approvedBy'], d['approvalRemarks'],
                    d['transactionCodeId'], d['productCodeId'],
                    d['cancellationSource'], d['internalCancellationBy'],
                    d['irbmSubmissionResponse'],
                    d['invoiceDate'], d['createdAt'], d['updatedAt'],
                    d['requestForRejectionDate'], d['cancellationDate'],
                    d['submissionDate'], d['validationDate'], d['approvalDate'],
                    d['billingPeriodStart'], d['billingPeriodEnd'],
                    d['internalCancellationDate'],
                    d['status'], d['irbmStatus'], d['currencyCode'],
                    d['invoiceType'], d['frequency'],
                    str(d['consolidated']).lower(), str(d['pdfSent']).lower(),
                    str(d['exchangeRate']),
                    str(d['totalExcludingTax']), str(d['totalIncludingTax']),
                    str(d['totalPayableAmount']), str(d['totalNetAmount']),
                    str(d['totalDiscountAmount']), str(d['totalFee']),
                    str(d['totalTaxAmount']), str(d['roundingAmount']),
                    str(d['amountExemptedFromTax']),
                    str(d['invoiceAdditionalDiscountAmount']),
                    str(d['invoiceAdditionalFeeAmount']),
                    str(d['totalSalesTaxable']), str(d['totalSalesTaxAmount']),
                    str(d['totalServiceTaxable']), str(d['totalServiceTaxAmount']),
                    str(d['totalTourismTaxable']), str(d['totalTourismTaxAmount']),
                    str(d['totalHighValueTaxable']), str(d['totalHighValueTaxAmount']),
                    str(d['totalLowValueTaxable']), str(d['totalLowValueTaxAmount'])]
            f.write(r['id'] + '|' + '|'.join(vals) + '\n')

# Also write combined single file
with open('/tmp/shard-db_par_single.json', 'w') as out:
    out.write('[')
    for c in range(0, total, chunk_size):
        idx = c // chunk_size
        with open(f'/tmp/shard-db_par_{idx}.json') as f:
            data = f.read().strip()
            if data.startswith('['): data = data[1:]
            if data.endswith(']'): data = data[:-1]
            if idx > 0: out.write(',')
            out.write(data)
    out.write(']')
PYEOF

ls -lh /tmp/shard-db_par_0.json /tmp/shard-db_par_0.csv /tmp/shard-db_par_single.json

create_fresh() {
    $BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
    rm -rf "$DB_ROOT/default/bench"
    sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
    $BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":256,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":[]}" > /dev/null
}

# ==================== TEST 1a: Single JSON file (baseline) ====================
echo ""
echo "--- TEST 1a: Single JSON file, $TOTAL records, no indexes ---"
create_fresh
time $BIN query '{"mode":"bulk-insert","dir":"default","object":"bench","file":"/tmp/shard-db_par_single.json"}' > /dev/null
$BIN query '{"mode":"size","dir":"default","object":"bench"}'
echo "  add-indexes:"
time $BIN query "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\",\"fields\":$INDEXES_JSON}" > /dev/null

# ==================== TEST 1b: Single CSV file (baseline) ====================
echo ""
echo "--- TEST 1b: Single CSV file, $TOTAL records, no indexes ---"
create_fresh
# Combine CSV chunks into single file
cat /tmp/shard-db_par_*.csv > /tmp/shard-db_par_single.csv
time $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_single.csv\",\"delimiter\":\"|\"}" > /dev/null
$BIN query '{"mode":"size","dir":"default","object":"bench"}'
echo "  add-indexes:"
time $BIN query "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\",\"fields\":$INDEXES_JSON}" > /dev/null

# ==================== TEST 2: Parallel chunks, no indexes, then add-indexes ====================
echo ""
echo "--- TEST 2: Parallel $CONNS connections × ${CHUNK}, no indexes, then add-indexes ---"
create_fresh
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.json\"}" > /dev/null &
    done
    wait
)
$BIN query '{"mode":"size","dir":"default","object":"bench"}'
echo "  add-indexes:"
time $BIN query "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\",\"fields\":$INDEXES_JSON}" > /dev/null

# ==================== TEST 3: Parallel JSON with pre-existing indexes ====================
echo ""
echo "--- TEST 3: Parallel $CONNS connections × ${CHUNK}, WITH pre-existing indexes ---"
$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":256,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":$INDEXES_JSON}" > /dev/null
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.json\"}" > /dev/null &
    done
    wait
)
$BIN query '{"mode":"size","dir":"default","object":"bench"}'

# ==================== TEST 4: Parallel CSV, no indexes, then add-indexes ====================
echo ""
echo "--- TEST 4: Parallel CSV $CONNS connections × ${CHUNK}, no indexes, then add-indexes ---"
create_fresh
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
)
$BIN query '{"mode":"size","dir":"default","object":"bench"}'
echo "  add-indexes:"
time $BIN query "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bench\",\"fields\":$INDEXES_JSON}" > /dev/null

# ==================== TEST 5: Parallel CSV with pre-existing indexes ====================
echo ""
echo "--- TEST 5: Parallel CSV $CONNS connections × ${CHUNK}, WITH pre-existing indexes ---"
$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
$BIN query "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bench\",\"splits\":256,\"max_key\":64,\"fields\":$FIELDS_JSON,\"indexes\":$INDEXES_JSON}" > /dev/null
time (
    for i in $(seq 0 $((NCHUNKS-1))); do
        while [ $(jobs -r | wc -l) -ge $CONNS ]; do sleep 0.05; done
        $BIN query "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"bench\",\"file\":\"/tmp/shard-db_par_$i.csv\",\"delimiter\":\"|\"}" > /dev/null &
    done
    wait
)
$BIN query '{"mode":"size","dir":"default","object":"bench"}'

# ==================== CLEANUP ====================
echo ""
du -sh "$DB_ROOT/default/bench/"
$BIN query '{"mode":"truncate","dir":"default","object":"bench"}' > /dev/null 2>&1
rm -rf "$DB_ROOT/default/bench"
sed -i '/^default:bench:/d' "$DB_ROOT/schema.conf" 2>/dev/null
find /tmp -maxdepth 1 -name 'shard-db_par_*' -delete 2>/dev/null
rm -f /tmp/shard-db_invoice_1m.json
$BIN stop

echo ""
echo "======================================"
echo "  Benchmark complete"
echo "======================================"
