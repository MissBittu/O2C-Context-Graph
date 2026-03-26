"""
Graph Model Module
Defines node types, edge relationships, and graph query functions over SQLite.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")

# Node type definitions: (type_name, table, id_columns, label_column, display_columns)
NODE_TYPES = {
    "SalesOrder": {
        "table": "sales_order_headers",
        "id_col": "salesOrder",
        "label": "salesOrder",
        "display": ["salesOrder", "salesOrderType", "soldToParty", "totalNetAmount", "transactionCurrency", "creationDate", "overallDeliveryStatus"]
    },
    "SalesOrderItem": {
        "table": "sales_order_items",
        "id_col": "salesOrder || '-' || salesOrderItem",
        "label": "salesOrder || '/' || salesOrderItem",
        "display": ["salesOrder", "salesOrderItem", "material", "requestedQuantity", "netAmount", "productionPlant"]
    },
    "Delivery": {
        "table": "outbound_delivery_headers",
        "id_col": "deliveryDocument",
        "label": "deliveryDocument",
        "display": ["deliveryDocument", "deliveryDocumentType", "soldToParty", "creationDate", "overallGoodsMovementStatus", "shippingPoint"]
    },
    "DeliveryItem": {
        "table": "outbound_delivery_items",
        "id_col": "deliveryDocument || '-' || deliveryDocumentItem",
        "label": "deliveryDocument || '/' || deliveryDocumentItem",
        "display": ["deliveryDocument", "deliveryDocumentItem", "material", "actualDeliveryQuantity", "plant", "referenceSdDocument"]
    },
    "BillingDocument": {
        "table": "billing_document_headers",
        "id_col": "billingDocument",
        "label": "billingDocument",
        "display": ["billingDocument", "billingDocumentType", "totalNetAmount", "transactionCurrency", "creationDate", "soldToParty", "accountingDocument"]
    },
    "BillingItem": {
        "table": "billing_document_items",
        "id_col": "billingDocument || '-' || billingDocumentItem",
        "label": "billingDocument || '/' || billingDocumentItem",
        "display": ["billingDocument", "billingDocumentItem", "material", "billingQuantity", "netAmount", "referenceSdDocument"]
    },
    "JournalEntry": {
        "table": "journal_entry_items",
        "id_col": "accountingDocument",
        "label": "accountingDocument",
        "display": ["companyCode", "fiscalYear", "accountingDocument", "glAccount", "referenceDocument", "postingDate", "clearingDate"]
    },
    "Payment": {
        "table": "payments_accounts_receivable",
        "id_col": "accountingDocument || '-' || accountingDocumentItem",
        "label": "accountingDocument || '/' || accountingDocumentItem",
        "display": ["companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem", "clearingDate", "glAccount"]
    },
    "Customer": {
        "table": "business_partners",
        "id_col": "businessPartner",
        "label": "COALESCE(organizationBpName1, firstName || ' ' || lastName, businessPartner)",
        "display": ["businessPartner", "firstName", "lastName", "organizationBpName1", "businessPartnerCategory", "creationDate"]
    },
    "Product": {
        "table": "products",
        "id_col": "product",
        "label": "product",
        "display": ["product", "productType", "baseUnit", "grossWeight", "productGroup"]
    },
    "Plant": {
        "table": "plants",
        "id_col": "plant",
        "label": "plant || ' - ' || plantName",
        "display": ["plant", "plantName", "companyCode", "salesOrganization"]
    },
}

# Edge definitions: source_type, target_type, relationship_label, SQL to get edges
EDGE_DEFS = [
    # Sales Order → Sales Order Item
    {
        "source": "SalesOrder", "target": "SalesOrderItem", "label": "HAS_ITEM",
        "sql": """SELECT soh.salesOrder AS source_id, soi.salesOrder || '-' || soi.salesOrderItem AS target_id
                  FROM sales_order_headers soh
                  JOIN sales_order_items soi ON soh.salesOrder = soi.salesOrder"""
    },
    # Sales Order → Customer (soldToParty)
    {
        "source": "SalesOrder", "target": "Customer", "label": "SOLD_TO",
        "sql": """SELECT soh.salesOrder AS source_id, soh.soldToParty AS target_id
                  FROM sales_order_headers soh
                  WHERE soh.soldToParty != ''"""
    },
    # Sales Order Item → Product (material)
    {
        "source": "SalesOrderItem", "target": "Product", "label": "CONTAINS_PRODUCT",
        "sql": """SELECT soi.salesOrder || '-' || soi.salesOrderItem AS source_id, soi.material AS target_id
                  FROM sales_order_items soi
                  WHERE soi.material != ''"""
    },
    # Sales Order Item → Plant (productionPlant)
    {
        "source": "SalesOrderItem", "target": "Plant", "label": "PRODUCED_AT",
        "sql": """SELECT soi.salesOrder || '-' || soi.salesOrderItem AS source_id, soi.productionPlant AS target_id
                  FROM sales_order_items soi
                  WHERE soi.productionPlant != ''"""
    },
    # Delivery → Delivery Item
    {
        "source": "Delivery", "target": "DeliveryItem", "label": "HAS_ITEM",
        "sql": """SELECT dh.deliveryDocument AS source_id, di.deliveryDocument || '-' || di.deliveryDocumentItem AS target_id
                  FROM outbound_delivery_headers dh
                  JOIN outbound_delivery_items di ON dh.deliveryDocument = di.deliveryDocument"""
    },
    # Delivery Item → Sales Order (referenceSdDocument links delivery to sales order)
    {
        "source": "DeliveryItem", "target": "SalesOrder", "label": "FULFILLS_ORDER",
        "sql": """SELECT di.deliveryDocument || '-' || di.deliveryDocumentItem AS source_id, di.referenceSdDocument AS target_id
                  FROM outbound_delivery_items di
                  WHERE di.referenceSdDocument != ''"""
    },
    # Delivery → Customer
    {
        "source": "Delivery", "target": "Customer", "label": "DELIVERED_TO",
        "sql": """SELECT dh.deliveryDocument AS source_id, dh.soldToParty AS target_id
                  FROM outbound_delivery_headers dh
                  WHERE dh.soldToParty != ''"""
    },
    # Delivery Item → Product
    {
        "source": "DeliveryItem", "target": "Product", "label": "DELIVERS_PRODUCT",
        "sql": """SELECT di.deliveryDocument || '-' || di.deliveryDocumentItem AS source_id, di.material AS target_id
                  FROM outbound_delivery_items di
                  WHERE di.material != ''"""
    },
    # Delivery Item → Plant
    {
        "source": "DeliveryItem", "target": "Plant", "label": "SHIPPED_FROM",
        "sql": """SELECT di.deliveryDocument || '-' || di.deliveryDocumentItem AS source_id, di.plant AS target_id
                  FROM outbound_delivery_items di
                  WHERE di.plant != ''"""
    },
    # Billing Document → Billing Item
    {
        "source": "BillingDocument", "target": "BillingItem", "label": "HAS_ITEM",
        "sql": """SELECT bh.billingDocument AS source_id, bi.billingDocument || '-' || bi.billingDocumentItem AS target_id
                  FROM billing_document_headers bh
                  JOIN billing_document_items bi ON bh.billingDocument = bi.billingDocument"""
    },
    # Billing Item → Delivery (referenceSdDocument links billing to delivery)
    {
        "source": "BillingItem", "target": "Delivery", "label": "BILLS_DELIVERY",
        "sql": """SELECT bi.billingDocument || '-' || bi.billingDocumentItem AS source_id, bi.referenceSdDocument AS target_id
                  FROM billing_document_items bi
                  WHERE bi.referenceSdDocument != ''"""
    },
    # Billing Document → Customer
    {
        "source": "BillingDocument", "target": "Customer", "label": "BILLED_TO",
        "sql": """SELECT bh.billingDocument AS source_id, bh.soldToParty AS target_id
                  FROM billing_document_headers bh
                  WHERE bh.soldToParty != ''"""
    },
    # Billing Item → Product
    {
        "source": "BillingItem", "target": "Product", "label": "BILLS_PRODUCT",
        "sql": """SELECT bi.billingDocument || '-' || bi.billingDocumentItem AS source_id, bi.material AS target_id
                  FROM billing_document_items bi
                  WHERE bi.material != ''"""
    },
    # Billing Document → Journal Entry (accountingDocument)
    {
        "source": "BillingDocument", "target": "JournalEntry", "label": "GENERATES_ENTRY",
        "sql": """SELECT bh.billingDocument AS source_id, bh.accountingDocument AS target_id
                  FROM billing_document_headers bh
                  WHERE bh.accountingDocument != ''"""
    },
    # Journal Entry ↔ Payment (via accountingDocument linkage)
    {
        "source": "JournalEntry", "target": "Payment", "label": "CLEARED_BY",
        "sql": """SELECT je.accountingDocument AS source_id,
                         p.accountingDocument || '-' || p.accountingDocumentItem AS target_id
                  FROM journal_entry_items je
                  JOIN payments_accounts_receivable p ON je.clearingAccountingDocument = p.accountingDocument
                  WHERE je.clearingAccountingDocument IS NOT NULL AND je.clearingAccountingDocument != ''"""
    },
]


def get_db():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_overview():
    """Get high-level graph overview: node counts by type."""
    conn = get_db()
    overview = {"nodes": [], "edges": []}
    
    for type_name, ndef in NODE_TYPES.items():
        try:
            cursor = conn.execute(f'SELECT COUNT(*) as cnt FROM "{ndef["table"]}"')
            count = cursor.fetchone()["cnt"]
            overview["nodes"].append({
                "type": type_name,
                "table": ndef["table"],
                "count": count
            })
        except Exception:
            overview["nodes"].append({"type": type_name, "table": ndef["table"], "count": 0})
    
    for edef in EDGE_DEFS:
        overview["edges"].append({
            "source": edef["source"],
            "target": edef["target"],
            "label": edef["label"]
        })
    
    conn.close()
    return overview


def get_nodes(node_type, limit=50, offset=0, search=None):
    """Get nodes of a given type."""
    ndef = NODE_TYPES.get(node_type)
    if not ndef:
        return []
    
    conn = get_db()
    display_cols = ", ".join(f'"{c}"' for c in ndef["display"])
    id_expr = ndef["id_col"]
    label_expr = ndef["label"]
    
    sql = f'SELECT {id_expr} AS id, {label_expr} AS label, {display_cols} FROM "{ndef["table"]}"'
    
    if search:
        conditions = " OR ".join(f'"{c}" LIKE ?' for c in ndef["display"])
        sql += f" WHERE {conditions}"
        params = [f"%{search}%"] * len(ndef["display"])
        sql += f" LIMIT {limit} OFFSET {offset}"
        cursor = conn.execute(sql, params)
    else:
        sql += f" LIMIT {limit} OFFSET {offset}"
        cursor = conn.execute(sql)
    
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_node_detail(node_type, node_id):
    """Get full detail of a specific node."""
    ndef = NODE_TYPES.get(node_type)
    if not ndef:
        return None
    
    conn = get_db()
    id_expr = ndef["id_col"]
    sql = f'SELECT * FROM "{ndef["table"]}" WHERE {id_expr} = ?'
    cursor = conn.execute(sql, [node_id])
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def expand_node(node_type, node_id):
    """Get all neighbors of a given node (connected via edges)."""
    result = {"nodes": [], "edges": []}
    conn = get_db()
    
    seen_nodes = set()
    
    for edef in EDGE_DEFS:
        # Check outgoing edges (this node is source)
        if edef["source"] == node_type:
            target_ndef = NODE_TYPES[edef["target"]]
            try:
                # Build a filtered version of the edge SQL
                full_sql = f"""SELECT target_id FROM ({edef['sql']}) sub WHERE sub.source_id = ?"""
                cursor = conn.execute(full_sql, [node_id])
                for row in cursor.fetchall():
                    target_id = row[0]
                    if target_id and (edef["target"], target_id) not in seen_nodes:
                        seen_nodes.add((edef["target"], target_id))
                        # Get target node details
                        detail = get_node_detail(edef["target"], target_id)
                        label_sql = f'SELECT {target_ndef["label"]} AS label FROM "{target_ndef["table"]}" WHERE {target_ndef["id_col"]} = ?'
                        label_row = conn.execute(label_sql, [target_id]).fetchone()
                        result["nodes"].append({
                            "id": target_id,
                            "type": edef["target"],
                            "label": label_row["label"] if label_row else target_id,
                            "data": detail
                        })
                        result["edges"].append({
                            "source": node_id,
                            "sourceType": node_type,
                            "target": target_id,
                            "targetType": edef["target"],
                            "label": edef["label"]
                        })
            except Exception as e:
                print(f"Edge query error ({edef['label']}): {e}")
        
        # Check incoming edges (this node is target)
        if edef["target"] == node_type:
            source_ndef = NODE_TYPES[edef["source"]]
            try:
                full_sql = f"""SELECT source_id FROM ({edef['sql']}) sub WHERE sub.target_id = ?"""
                cursor = conn.execute(full_sql, [node_id])
                for row in cursor.fetchall():
                    source_id = row[0]
                    if source_id and (edef["source"], source_id) not in seen_nodes:
                        seen_nodes.add((edef["source"], source_id))
                        detail = get_node_detail(edef["source"], source_id)
                        label_sql = f'SELECT {source_ndef["label"]} AS label FROM "{source_ndef["table"]}" WHERE {source_ndef["id_col"]} = ?'
                        label_row = conn.execute(label_sql, [source_id]).fetchone()
                        result["nodes"].append({
                            "id": source_id,
                            "type": edef["source"],
                            "label": label_row["label"] if label_row else source_id,
                            "data": detail
                        })
                        result["edges"].append({
                            "source": source_id,
                            "sourceType": edef["source"],
                            "target": node_id,
                            "targetType": node_type,
                            "label": edef["label"]
                        })
            except Exception as e:
                print(f"Edge query error ({edef['label']}): {e}")
    
    conn.close()
    return result


def get_schema_description():
    """Get a human-readable schema description for LLM context."""
    conn = get_db()
    schema_parts = []
    
    for tdef_name, ndef in NODE_TYPES.items():
        table = ndef["table"]
        cursor = conn.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()
        col_strs = [f"  {col['name']} ({col['type']})" for col in columns]
        
        # Get row count
        count_cursor = conn.execute(f'SELECT COUNT(*) as cnt FROM "{table}"')
        count = count_cursor.fetchone()["cnt"]
        
        schema_parts.append(f"Table: {table} ({count} rows)\nColumns:\n" + "\n".join(col_strs))
    
    conn.close()
    
    relationships = []
    for edef in EDGE_DEFS:
        relationships.append(f"  {edef['source']} --[{edef['label']}]--> {edef['target']}")
    
    return "\n\n".join(schema_parts) + "\n\nRelationships:\n" + "\n".join(relationships)
