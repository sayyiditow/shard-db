/* src/bench/bench_invoice_schema.h
 *
 * Shared schema definition for the invoice benchmarks
 * (bench_invoice and bench_parallel). 64 typed fields covering varchar,
 * datetime, bool, double, currency. Same wire-shape as the bash
 * bench-invoice.sh / bench-parallel.sh schema.
 */
#ifndef BENCH_INVOICE_SCHEMA_H
#define BENCH_INVOICE_SCHEMA_H

/* JSON array literal (without surrounding [ ]) of all 64 typed-field
   declarations. Drop into a "fields":[<this>] field of a create-object
   request. */
#define INVOICE_SCHEMA_FIELDS \
    "\"buyerId:varchar:40\",\"version:varchar:5\",\"number:varchar:20\"," \
    "\"originalReference:varchar:20\",\"supplierId:varchar:40\"," \
    "\"irbmIdentifier:varchar:20\",\"source:varchar:8\"," \
    "\"createdBy:varchar:25\",\"updatedBy:varchar:25\"," \
    "\"irbmLongId:varchar:40\",\"originalReferenceNumber:varchar:15\"," \
    "\"batchNumber:varchar:12\",\"submissionUid:varchar:36\"," \
    "\"submittedBy:varchar:25\",\"taxExemptionReason:varchar:20\"," \
    "\"requestForRejectionReason:varchar:20\",\"cancellationReason:varchar:20\"," \
    "\"approvedBy:varchar:25\",\"approvalRemarks:varchar:15\"," \
    "\"transactionCodeId:varchar:8\",\"productCodeId:varchar:8\"," \
    "\"cancellationSource:varchar:10\",\"internalCancellationBy:varchar:10\"," \
    "\"irbmSubmissionResponse:varchar:10\",\"invoiceDate:datetime\"," \
    "\"createdAt:datetime\",\"updatedAt:datetime\"," \
    "\"requestForRejectionDate:datetime\",\"cancellationDate:datetime\"," \
    "\"submissionDate:datetime\",\"validationDate:datetime\"," \
    "\"approvalDate:datetime\",\"billingPeriodStart:datetime\"," \
    "\"billingPeriodEnd:datetime\",\"internalCancellationDate:datetime\"," \
    "\"status:varchar:12\",\"irbmStatus:varchar:12\"," \
    "\"currencyCode:varchar:4\",\"invoiceType:varchar:15\"," \
    "\"frequency:varchar:10\",\"consolidated:bool\",\"pdfSent:bool\"," \
    "\"exchangeRate:double\"," \
    "\"totalExcludingTax:currency\",\"totalIncludingTax:currency\"," \
    "\"totalPayableAmount:currency\",\"totalNetAmount:currency\"," \
    "\"totalDiscountAmount:currency\",\"totalFee:currency\"," \
    "\"totalTaxAmount:currency\",\"roundingAmount:currency\"," \
    "\"amountExemptedFromTax:currency\"," \
    "\"invoiceAdditionalDiscountAmount:currency\"," \
    "\"invoiceAdditionalFeeAmount:currency\"," \
    "\"totalSalesTaxable:currency\",\"totalSalesTaxAmount:currency\"," \
    "\"totalServiceTaxable:currency\",\"totalServiceTaxAmount:currency\"," \
    "\"totalTourismTaxable:currency\",\"totalTourismTaxAmount:currency\"," \
    "\"totalHighValueTaxable:currency\",\"totalHighValueTaxAmount:currency\"," \
    "\"totalLowValueTaxable:currency\",\"totalLowValueTaxAmount:currency\""

/* Index field list for the 14 indexes the bench builds. */
#define INVOICE_INDEX_FIELDS \
    "\"buyerId\",\"status\",\"irbmStatus\",\"supplierId\"," \
    "\"invoiceDate\",\"number\",\"batchNumber\",\"validationDate\"," \
    "\"submissionDate\",\"createdAt\"," \
    "\"irbmStatus+pdfSent\",\"status+source\"," \
    "\"status+createdAt\",\"status+invoiceDate\""

#endif
