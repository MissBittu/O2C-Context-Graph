# O2C Context Graph Explorer

An interactive **Graph-Based Data Modeling & Query System** for SAP Order-to-Cash data, with an LLM-powered natural language query interface.

![O2C Graph UI](./docs/screenshot.png)

---

## Quick Start

### Prerequisites
- Python 3.10+
- Node.js 18+
- A free **Google Gemini API key** from [ai.google.dev](https://ai.google.dev)

### 1. Backend Setup

```bash
cd backend

# Install dependencies
pip install fastapi uvicorn google-generativeai pydantic python-dotenv

# Set your Gemini API key
echo "GEMINI_API_KEY=your_key_here" > .env

# Ingest data into SQLite (run once)
python ingest.py

# Start the API server
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 2. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start the dev server
npm run dev
```

### 3. Open the App

Navigate to **http://localhost:5173**

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│            React + Vite Frontend (5173)              │
│  ┌──────────────────┐  ┌──────────────────────────┐ │
│  │ Graph Viewer      │  │ Chat Panel               │ │
│  │ (react-force-     │  │ • NL → SQL via LLM       │ │
│  │  graph-2d)        │  │ • SQL revealed on demand │ │
│  │ • Click-to-expand │  │ • Node highlighting      │ │
│  │ • Node metadata   │  │ • Sample queries         │ │
│  └──────────────────┘  └──────────────────────────┘ │
└───────────────────────────────────────────────────–──┘
                           │ HTTP
┌──────────────────────────▼────────────────────────────┐
│          FastAPI Backend (Python, port 8000)          │
│  /api/graph/overview    → Node type counts            │
│  /api/graph/nodes/{type} → Paginated node list        │
│  /api/graph/expand/{type}/{id} → Neighbor traversal  │
│  /api/chat              → NL → SQL → Answer           │
└──────────────────────────┬────────────────────────────┘
                           │ SQLite
┌──────────────────────────▼────────────────────────────┐
│               SQLite Database (o2c.db)                │
│       19 tables from SAP O2C JSONL dataset            │
└───────────────────────────────────────────────────────┘
```

---

## Database Choice: SQLite

**Why not Neo4j or PostgreSQL?**

| Criterion | SQLite | Neo4j | PostgreSQL |
|-----------|--------|-------|------------|
| Setup cost | Zero | Docker/install | Install |
| Dataset size | ~50K rows ✅ | Overkill | Overkill |
| NL-to-SQL via LLM | Native SQL ✅ | Cypher (complex) | SQL ✅ |
| Deployment | Single file ✅ | Server required | Server required |
| Graph traversal | FK JOINs ✅ | Native | FK JOINs |

The dataset has ~50K rows across 19 tables. SQLite handles this effortlessly. The "graph" is a **virtual graph over relational data** — nodes and edges are derived from FK relationships at query time. This also makes the LLM's job easier since SQL is well-understood by every frontier model.

---

## Graph Model

### Node Types (11)
| Node | Table | Key |
|------|-------|-----|
| SalesOrder | sales_order_headers | salesOrder |
| SalesOrderItem | sales_order_items | salesOrder + salesOrderItem |
| Delivery | outbound_delivery_headers | deliveryDocument |
| DeliveryItem | outbound_delivery_items | deliveryDocument + item |
| BillingDocument | billing_document_headers | billingDocument |
| BillingItem | billing_document_items | billingDocument + item |
| JournalEntry | journal_entry_items | accountingDocument |
| Payment | payments_accounts_receivable | accountingDocument + item |
| Customer | business_partners | businessPartner |
| Product | products | product |
| Plant | plants | plant |

### Core O2C Flow (Edge Chain)
```
SalesOrder → SalesOrderItem → Product
    ↓                          ↓
Delivery ────────────────── Plant
    ↓
BillingDocument
    ↓
JournalEntry
    ↓
Payment
```

All edges are FK-derived:
- `DeliveryItem.referenceSdDocument` → `SalesOrder.salesOrder`
- `BillingItem.referenceSdDocument` → `Delivery.deliveryDocument`
- `BillingDocument.accountingDocument` → `JournalEntry.accountingDocument`
- `JournalEntry.clearingAccountingDocument` → `Payment.accountingDocument`

---

## LLM Prompting Strategy

### System Prompt Structure
1. **Role definition**: Expert SAP O2C analyst
2. **Guardrails**: Explicit instruction to reject off-topic queries  
3. **Full schema**: All 11 tables with column names, types, and row counts
4. **Relationship map**: FK chains explained in plain English
5. **Output format**: Strict JSON `{ sql, explanation, highlightNodes }`

### NL → SQL Pipeline
```
User message
    ↓ Domain check (guardrails.py keyword filter)
    ↓ LLM generates SQL + explanation JSON
    ↓ SQL executed against SQLite
    ↓ On SQL error: retry with error fed back to LLM
    ↓ Data sent to LLM for natural language summary
    ↓ Answer + highlighted node IDs returned to UI
```

### Guardrails (Two Layers)
1. **Pre-filter** (Python): Regex patterns catch creative writing, general knowledge, coding help, etc.
2. **LLM-level**: System prompt explicitly instructs the model to refuse and return the rejection message if the query is not about the dataset.

---

## Example Queries

| Query | What it does |
|-------|-------------|
| "Which products have the highest number of billing documents?" | JOINs billing_document_items → products, groups by product, counts |
| "Trace the full flow of billing document 90504248" | Traces: billing → delivery → sales order → journal entry → payment |
| "Identify sales orders delivered but not billed" | LEFT JOIN delivery items to billing items, finds NULL billing |
| "Which customers have the highest total billed amount?" | JOINs billing_document_headers → business_partners, sums netAmount |

---

## Project Structure

```
d:\AS\DOU\
├── sap-order-to-cash-dataset\     # Raw JSONL data
├── backend\
│   ├── ingest.py                  # Data load → SQLite
│   ├── graph_model.py             # Node/edge definitions + queries
│   ├── guardrails.py              # Domain validation
│   ├── llm_engine.py              # Gemini NL→SQL engine
│   ├── main.py                    # FastAPI app
│   ├── o2c.db                     # SQLite database (generated)
│   └── .env                       # GEMINI_API_KEY
└── frontend\
    ├── src\
    │   ├── App.jsx                # Root layout
    │   ├── api.js                 # API helpers
    │   ├── graphConfig.js         # Node colors/sizes
    │   ├── index.css              # Premium dark theme
    │   └── components\
    │       ├── GraphViewer.jsx    # Force graph + expand
    │       ├── ChatPanel.jsx      # NL query interface
    │       └── NodeDetail.jsx     # Node metadata panel
    └── package.json
```

<!-- Architecture documentation updated -->

<!-- Architecture documentation updated -->

<!-- Architecture constraints validated -->
