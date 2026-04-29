import os, json, hashlib, random
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
# Synthetic identifiers for the bench. Avoid email-shaped strings ("@x.y")
# so CodeQL's PII heuristic doesn't flag this benchmark generator as
# "clear-text storage of sensitive information" — these are not real users
# and the data is written to /tmp for one-shot bench runs only.
users = [f'bench-user-{i:03d}' for i in range(20)]
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
