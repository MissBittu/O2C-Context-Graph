"""
Data Ingestion Script
Reads all 19 JSONL directories from the SAP O2C dataset and loads them into SQLite.
"""

import json
import os
import sqlite3
import glob
import sys

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "sap-order-to-cash-dataset", "sap-o2c-data")
DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")

# Table definitions: (directory_name, table_name, columns_with_types, primary_key, indexes)
TABLE_DEFS = [
    {
        "dir": "sales_order_headers",
        "table": "sales_order_headers",
        "columns": {
            "salesOrder": "TEXT", "salesOrderType": "TEXT", "salesOrganization": "TEXT",
            "distributionChannel": "TEXT", "organizationDivision": "TEXT",
            "salesGroup": "TEXT", "salesOffice": "TEXT", "soldToParty": "TEXT",
            "creationDate": "TEXT", "createdByUser": "TEXT", "lastChangeDateTime": "TEXT",
            "totalNetAmount": "REAL", "overallDeliveryStatus": "TEXT",
            "overallOrdReltdBillgStatus": "TEXT", "overallSdDocReferenceStatus": "TEXT",
            "transactionCurrency": "TEXT", "pricingDate": "TEXT",
            "requestedDeliveryDate": "TEXT", "headerBillingBlockReason": "TEXT",
            "deliveryBlockReason": "TEXT", "incotermsClassification": "TEXT",
            "incotermsLocation1": "TEXT", "customerPaymentTerms": "TEXT",
            "totalCreditCheckStatus": "TEXT"
        },
        "pk": ["salesOrder"],
        "indexes": [["soldToParty"]]
    },
    {
        "dir": "sales_order_items",
        "table": "sales_order_items",
        "columns": {
            "salesOrder": "TEXT", "salesOrderItem": "TEXT",
            "salesOrderItemCategory": "TEXT", "material": "TEXT",
            "requestedQuantity": "REAL", "requestedQuantityUnit": "TEXT",
            "netAmount": "REAL", "transactionCurrency": "TEXT",
            "materialGroup": "TEXT", "productionPlant": "TEXT",
            "storageLocation": "TEXT", "salesDocumentRjcnReason": "TEXT",
            "itemBillingBlockReason": "TEXT"
        },
        "pk": ["salesOrder", "salesOrderItem"],
        "indexes": [["salesOrder"], ["material"], ["productionPlant"]]
    },
    {
        "dir": "sales_order_schedule_lines",
        "table": "sales_order_schedule_lines",
        "columns": {
            "salesOrder": "TEXT", "salesOrderItem": "TEXT", "scheduleLine": "TEXT",
            "requestedDeliveryDate": "TEXT", "confirmedDeliveryDate": "TEXT",
            "orderQuantityUnit": "TEXT", "scheduleLineOrderQuantity": "REAL",
            "confdOrderQtyByMatlAvailCheck": "REAL"
        },
        "pk": ["salesOrder", "salesOrderItem", "scheduleLine"],
        "indexes": [["salesOrder"]]
    },
    {
        "dir": "outbound_delivery_headers",
        "table": "outbound_delivery_headers",
        "columns": {
            "deliveryDocument": "TEXT", "deliveryDocumentType": "TEXT",
            "soldToParty": "TEXT", "creationDate": "TEXT", "creationTime": "TEXT",
            "lastChangeDate": "TEXT", "actualGoodsMovementDate": "TEXT",
            "actualGoodsMovementTime": "TEXT", "overallGoodsMovementStatus": "TEXT",
            "overallPickingStatus": "TEXT", "overallProofOfDeliveryStatus": "TEXT",
            "shippingPoint": "TEXT"
        },
        "pk": ["deliveryDocument"],
        "indexes": [["soldToParty"], ["shippingPoint"]]
    },
    {
        "dir": "outbound_delivery_items",
        "table": "outbound_delivery_items",
        "columns": {
            "deliveryDocument": "TEXT", "deliveryDocumentItem": "TEXT",
            "actualDeliveryQuantity": "REAL", "batch": "TEXT",
            "deliveryQuantityUnit": "TEXT", "itemDescription": "TEXT",
            "itemGrossWeight": "REAL", "itemNetWeight": "REAL",
            "itemWeightUnit": "TEXT", "material": "TEXT",
            "materialByCustomer": "TEXT", "deliveryDocumentItemCategory": "TEXT",
            "lastChangeDate": "TEXT", "plant": "TEXT",
            "referenceSdDocument": "TEXT", "referenceSdDocumentItem": "TEXT",
            "storageLocation": "TEXT"
        },
        "pk": ["deliveryDocument", "deliveryDocumentItem"],
        "indexes": [["deliveryDocument"], ["referenceSdDocument"], ["material"], ["plant"]]
    },
    {
        "dir": "billing_document_headers",
        "table": "billing_document_headers",
        "columns": {
            "billingDocument": "TEXT", "billingDocumentType": "TEXT",
            "creationDate": "TEXT", "creationTime": "TEXT",
            "billingDocumentDate": "TEXT", "totalNetAmount": "REAL",
            "transactionCurrency": "TEXT", "companyCode": "TEXT",
            "fiscalYear": "TEXT", "accountingDocument": "TEXT",
            "soldToParty": "TEXT"
        },
        "pk": ["billingDocument"],
        "indexes": [["soldToParty"], ["accountingDocument"]]
    },
    {
        "dir": "billing_document_items",
        "table": "billing_document_items",
        "columns": {
            "billingDocument": "TEXT", "billingDocumentItem": "TEXT",
            "material": "TEXT", "billingQuantity": "REAL",
            "billingQuantityUnit": "TEXT", "netAmount": "REAL",
            "transactionCurrency": "TEXT", "referenceSdDocument": "TEXT",
            "referenceSdDocumentItem": "TEXT"
        },
        "pk": ["billingDocument", "billingDocumentItem"],
        "indexes": [["billingDocument"], ["referenceSdDocument"], ["material"]]
    },
    {
        "dir": "billing_document_cancellations",
        "table": "billing_document_cancellations",
        "columns": {
            "billingDocument": "TEXT", "billingDocumentType": "TEXT",
            "cancelledBillingDocument": "TEXT", "creationDate": "TEXT",
            "soldToParty": "TEXT"
        },
        "pk": ["billingDocument"],
        "indexes": [["cancelledBillingDocument"]]
    },
    {
        "dir": "journal_entry_items_accounts_receivable",
        "table": "journal_entry_items",
        "columns": {
            "companyCode": "TEXT", "fiscalYear": "TEXT",
            "accountingDocument": "TEXT", "glAccount": "TEXT",
            "referenceDocument": "TEXT", "costCenter": "TEXT",
            "profitCenter": "TEXT", "amountInCompanyCodeCurrency": "REAL",
            "companyCodeCurrency": "TEXT", "postingDate": "TEXT",
            "documentDate": "TEXT", "financialAccountType": "TEXT",
            "clearingDate": "TEXT", "clearingAccountingDocument": "TEXT",
            "clearingDocFiscalYear": "TEXT"
        },
        "pk": None,
        "indexes": [["accountingDocument"], ["referenceDocument"]]
    },
    {
        "dir": "payments_accounts_receivable",
        "table": "payments_accounts_receivable",
        "columns": {
            "companyCode": "TEXT", "fiscalYear": "TEXT",
            "accountingDocument": "TEXT", "accountingDocumentItem": "TEXT",
            "clearingDate": "TEXT", "assignmentReference": "TEXT",
            "glAccount": "TEXT", "financialAccountType": "TEXT",
            "profitCenter": "TEXT", "costCenter": "TEXT"
        },
        "pk": None,
        "indexes": [["accountingDocument"]]
    },
    {
        "dir": "business_partners",
        "table": "business_partners",
        "columns": {
            "businessPartner": "TEXT", "businessPartnerCategory": "TEXT",
            "firstName": "TEXT", "lastName": "TEXT",
            "organizationBpName1": "TEXT", "organizationBpName2": "TEXT",
            "formOfAddress": "TEXT", "createdByUser": "TEXT",
            "creationDate": "TEXT", "lastChangeDate": "TEXT",
            "lastChangeTime": "TEXT", "isMarkedForArchiving": "INTEGER"
        },
        "pk": ["businessPartner"],
        "indexes": []
    },
    {
        "dir": "business_partner_addresses",
        "table": "business_partner_addresses",
        "columns": {
            "businessPartner": "TEXT", "addressId": "TEXT",
            "country": "TEXT", "region": "TEXT",
            "cityName": "TEXT", "postalCode": "TEXT",
            "streetName": "TEXT", "houseNumber": "TEXT"
        },
        "pk": ["businessPartner", "addressId"],
        "indexes": [["businessPartner"]]
    },
    {
        "dir": "customer_company_assignments",
        "table": "customer_company_assignments",
        "columns": {
            "customer": "TEXT", "companyCode": "TEXT",
            "reconciliationAccount": "TEXT", "customerAccountGroup": "TEXT"
        },
        "pk": ["customer", "companyCode"],
        "indexes": [["customer"]]
    },
    {
        "dir": "customer_sales_area_assignments",
        "table": "customer_sales_area_assignments",
        "columns": {
            "customer": "TEXT", "salesOrganization": "TEXT",
            "distributionChannel": "TEXT", "division": "TEXT",
            "currency": "TEXT", "customerPaymentTerms": "TEXT",
            "incotermsClassification": "TEXT", "exchangeRateType": "TEXT"
        },
        "pk": ["customer", "salesOrganization", "distributionChannel", "division"],
        "indexes": [["customer"]]
    },
    {
        "dir": "products",
        "table": "products",
        "columns": {
            "product": "TEXT", "productType": "TEXT",
            "baseUnit": "TEXT", "grossWeight": "REAL",
            "weightUnit": "TEXT", "creationDate": "TEXT",
            "lastChangeDate": "TEXT", "productGroup": "TEXT",
            "industrySector": "TEXT"
        },
        "pk": ["product"],
        "indexes": []
    },
    {
        "dir": "product_descriptions",
        "table": "product_descriptions",
        "columns": {
            "product": "TEXT", "language": "TEXT",
            "productDescription": "TEXT"
        },
        "pk": ["product", "language"],
        "indexes": [["product"]]
    },
    {
        "dir": "product_plants",
        "table": "product_plants",
        "columns": {
            "product": "TEXT", "plant": "TEXT",
            "purchasingGroup": "TEXT", "mrpType": "TEXT",
            "profileCode": "TEXT", "availabilityCheckType": "TEXT"
        },
        "pk": ["product", "plant"],
        "indexes": [["product"], ["plant"]]
    },
    {
        "dir": "product_storage_locations",
        "table": "product_storage_locations",
        "columns": {
            "product": "TEXT", "plant": "TEXT",
            "storageLocation": "TEXT",
            "warehouseStorageBin": "TEXT",
            "maintenanceStatus": "TEXT",
            "inventoryCurrentUnRstrcdStk": "REAL"
        },
        "pk": ["product", "plant", "storageLocation"],
        "indexes": [["product"]]
    },
    {
        "dir": "plants",
        "table": "plants",
        "columns": {
            "plant": "TEXT", "plantName": "TEXT",
            "addressId": "TEXT", "defaultPurchasingOrganization": "TEXT",
            "companyCode": "TEXT", "salesOrganization": "TEXT",
            "distributionChannel": "TEXT", "language": "TEXT"
        },
        "pk": ["plant"],
        "indexes": []
    },
]


def flatten_value(val):
    """Flatten nested values (e.g., time objects) to strings."""
    if isinstance(val, dict):
        return json.dumps(val)
    return val


def create_table(cursor, tdef):
    """Create a table from a table definition."""
    cols = ", ".join(f'"{col}" {dtype}' for col, dtype in tdef["columns"].items())
    pk_clause = ""
    if tdef["pk"]:
        pk_cols = ", ".join(f'"{c}"' for c in tdef["pk"])
        pk_clause = f", PRIMARY KEY ({pk_cols})"
    
    sql = f'CREATE TABLE IF NOT EXISTS "{tdef["table"]}" ({cols}{pk_clause})'
    cursor.execute(sql)
    
    # Create indexes
    for idx_cols in tdef.get("indexes", []):
        idx_name = f'idx_{tdef["table"]}_{"_".join(idx_cols)}'
        idx_col_str = ", ".join(f'"{c}"' for c in idx_cols)
        cursor.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{tdef["table"]}" ({idx_col_str})')


def load_jsonl_files(directory):
    """Load all JSONL files from a directory."""
    rows = []
    pattern = os.path.join(directory, "*.jsonl")
    files = glob.glob(pattern)
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def insert_rows(cursor, tdef, rows):
    """Insert rows into a table."""
    if not rows:
        return 0
    
    columns = list(tdef["columns"].keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_str = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT OR IGNORE INTO "{tdef["table"]}" ({col_str}) VALUES ({placeholders})'
    
    batch = []
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            values.append(flatten_value(val))
        batch.append(tuple(values))
    
    cursor.executemany(sql, batch)
    return len(batch)


def main():
    """Main ingestion pipeline."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing database: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Enable WAL mode for performance
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    
    total_rows = 0
    
    for tdef in TABLE_DEFS:
        dir_path = os.path.join(DATA_DIR, tdef["dir"])
        if not os.path.exists(dir_path):
            print(f"WARNING: Directory not found: {dir_path}")
            continue
        
        # Create table
        create_table(cursor, tdef)
        
        # Load and insert data
        rows = load_jsonl_files(dir_path)
        count = insert_rows(cursor, tdef, rows)
        total_rows += count
        print(f"  {tdef['table']}: {count} rows loaded")
    
    conn.commit()
    
    # Verify
    print(f"\n--- Total: {total_rows} rows across {len(TABLE_DEFS)} tables ---")
    print(f"Database: {DB_PATH}")
    
    # Print summary
    print("\nTable row counts:")
    for tdef in TABLE_DEFS:
        cursor.execute(f'SELECT COUNT(*) FROM "{tdef["table"]}"')
        count = cursor.fetchone()[0]
        print(f"  {tdef['table']}: {count}")
    
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()

# Indexes automatically managed via SQLite

# Indexes automatically managed via SQLite
