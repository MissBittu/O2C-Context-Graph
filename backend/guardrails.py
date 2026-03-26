"""
Guardrails Module
Validates user queries to ensure they are related to the SAP O2C dataset.
"""

import re

# Keywords that indicate on-topic queries
DOMAIN_KEYWORDS = [
    "sales order", "order", "delivery", "billing", "invoice", "payment",
    "journal entry", "customer", "product", "material", "plant",
    "shipping", "document", "amount", "quantity", "date",
    "revenue", "flow", "trace", "track", "status",
    "cancelled", "cancellation", "pending", "completed",
    "account", "receivable", "credit", "debit",
    "sold", "buyer", "supplier", "partner",
    "incoterms", "currency", "price", "net amount",
    "sap", "o2c", "order to cash", "order-to-cash",
    "billing document", "delivery document", "sales document",
    "goods movement", "picking", "proof of delivery",
    "schedule line", "storage location", "warehouse",
    "distribution channel", "sales organization",
    "company code", "fiscal year", "profit center", "cost center",
    "clearing", "posting", "reference", "gl account",
    "address", "city", "country", "region",
    "which", "how many", "list", "show", "find", "get",
    "total", "average", "count", "sum", "max", "min",
    "top", "highest", "lowest", "most", "least",
    "between", "before", "after", "during",
    "broken", "incomplete", "missing", "without",
    "associated", "related", "connected", "linked",
    "table", "data", "dataset", "database", "query", "sql",
    "graph", "node", "edge", "relationship"
]

# Off-topic patterns that should be rejected
OFF_TOPIC_PATTERNS = [
    r"(write|compose|create)\s+(a\s+)?(poem|story|essay|song|joke|letter)",
    r"(what|who|where|when)\s+is\s+(the\s+)?(capital|president|king|queen|CEO)",
    r"(translate|convert)\s+.*\s+(to|into)\s+(french|spanish|german|chinese|japanese|hindi)",
    r"(code|program|script|function)\s+(in|using|with)\s+(python|javascript|java|c\+\+|rust)",
    r"(recipe|cook|bake|ingredients)\s+for",
    r"(weather|temperature|forecast)\s+(in|for|at)",
    r"(tell|give)\s+me\s+(a\s+)?(joke|riddle|fun fact)",
    r"(play|recommend)\s+(a\s+)?(game|movie|song|book)",
    r"(explain|define|what\s+is)\s+(quantum|relativity|evolution|philosophy|democracy|capitalism)",
    r"how\s+to\s+(lose weight|cook|travel|invest|meditate)",
    r"(my|your|his|her)\s+(feelings?|emotions?|mood|health)",
]

REJECTION_MESSAGE = (
    "This system is designed to answer questions related to the SAP Order-to-Cash dataset only. "
    "I can help you explore sales orders, deliveries, billing documents, journal entries, payments, "
    "customers, products, and their relationships. Please ask a question about the dataset."
)


def is_domain_relevant(query: str) -> tuple[bool, str]:
    """
    Check if a query is relevant to the SAP O2C domain.
    Returns (is_relevant, rejection_message_if_not).
    """
    query_lower = query.lower().strip()
    
    # Very short queries → let LLM handle
    if len(query_lower) < 5:
        return True, ""
    
    # Check off-topic patterns first
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, query_lower):
            return False, REJECTION_MESSAGE
    
    # Check if any domain keywords are present
    has_domain_keyword = any(kw in query_lower for kw in DOMAIN_KEYWORDS)
    
    # If no domain keywords at all, it's likely off-topic
    # But allow generic-sounding analytical queries 
    generic_analytical = any(w in query_lower for w in [
        "how many", "which", "list", "show", "find", "count", 
        "total", "average", "what", "trace", "identify", "compare"
    ])
    
    if has_domain_keyword or generic_analytical:
        return True, ""
    
    # Default: let the LLM handle it with guardrail in system prompt
    # Only reject if clearly off-topic
    return True, ""


def get_guardrail_system_prompt():
    """Get the guardrail portion of the LLM system prompt."""
    return """
IMPORTANT GUARDRAILS:
- You are ONLY allowed to answer questions about the SAP Order-to-Cash dataset.
- The dataset contains: sales orders, deliveries, billing documents, journal entries, 
  payments, customers (business partners), products, plants, and their relationships.
- If the user asks about anything unrelated to this dataset (general knowledge, creative writing, 
  coding help, personal advice, etc.), respond EXACTLY with:
  "This system is designed to answer questions related to the SAP Order-to-Cash dataset only."
- Do NOT generate fictional data. All answers MUST be backed by actual SQL query results.
- Do NOT help with tasks outside the scope of exploring and querying this dataset.
"""
