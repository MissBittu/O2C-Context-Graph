"""
LLM Engine Module
Uses Google Gemini (primary) or Groq (fallback) to translate natural language
queries into SQL, execute them, and generate data-backed answers.
Handles rate limits with retry + exponential backoff.
"""

import os
import re
import json
import sqlite3
import time
import asyncio
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ─── Provider Setup ──────────────────────────────────
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_KEY = os.getenv("GROQ_API_KEY", "")

# Try Groq FIRST (Gemini quota exhausted), fall back to Gemini
USE_GROQ = False
USE_GEMINI = False

if GROQ_KEY:
    try:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_KEY)
        USE_GROQ = True
    except ImportError:
        USE_GROQ = False

if GEMINI_KEY and GEMINI_KEY != "your_gemini_api_key_here":
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    GEMINI_MODELS = ["gemini-2.0-flash-lite", "gemini-2.0-flash"]
    USE_GEMINI = True

from graph_model import get_schema_description, DB_PATH
from guardrails import is_domain_relevant, get_guardrail_system_prompt, REJECTION_MESSAGE


def get_system_prompt():
    """Build the full system prompt with schema and guardrails."""
    schema = get_schema_description()
    guardrail_prompt = get_guardrail_system_prompt()

    return f"""You are an expert data analyst for the SAP Order-to-Cash (O2C) system.
You help users explore and query a SQLite database containing SAP O2C business data.

{guardrail_prompt}

DATABASE SCHEMA:
{schema}

KEY RELATIONSHIPS (Order-to-Cash Flow):
1. Sales Order (sales_order_headers) → has items in (sales_order_items)
2. Sales Order Item → links to Product via "material" column
3. Sales Order Item → links to Plant via "productionPlant" column
4. Delivery (outbound_delivery_headers) → has items in (outbound_delivery_items)
5. Delivery Item → links back to Sales Order via "referenceSdDocument" column (this contains the salesOrder number)
6. Billing Document (billing_document_headers) → has items in (billing_document_items)
7. Billing Item → links back to Delivery via "referenceSdDocument" column (this contains the deliveryDocument number)
8. Billing Document → links to Journal Entry via "accountingDocument" column
9. Journal Entry → links to Payment via "clearingAccountingDocument" = payments "accountingDocument"
10. Customer (business_partners) → linked from sales orders, deliveries, billing docs via "soldToParty" = "businessPartner"

TRACING THE FULL O2C FLOW:
- To trace a billing document's full flow:
  1. Start with billing_document_items → get referenceSdDocument (this is a deliveryDocument)
  2. From outbound_delivery_items where deliveryDocument matches → get referenceSdDocument (this is a salesOrder)
  3. From billing_document_headers → get accountingDocument (this is a journal entry)
  4. From journal_entry_items → check clearingAccountingDocument for payment linkage

FINDING BROKEN/INCOMPLETE FLOWS:
- "Delivered but not billed": Sales orders that have delivery items (in outbound_delivery_items.referenceSdDocument)
  but no corresponding billing (check if deliveryDocument appears in billing_document_items.referenceSdDocument)
- "Billed without delivery": Billing items whose referenceSdDocument does NOT appear in outbound_delivery_headers

YOUR TASK:
When the user asks a question:
1. First, determine if the question is about the SAP O2C dataset
2. If NOT about the dataset, reply: "This system is designed to answer questions related to the SAP Order-to-Cash dataset only."
3. If it IS about the dataset:
   a. Generate ONE or more SQL queries to answer the question
   b. Return your response in this EXACT JSON format:
   {{
     "sql": "SELECT ... FROM ... (the main SQL query)",
     "explanation": "Brief explanation of the natural language answer based on the query results",
     "highlightNodes": [
       {{"type": "SalesOrder", "id": "740506"}},
       {{"type": "Customer", "id": "310000108"}}
     ]
   }}

IMPORTANT RULES FOR SQL:
- Use ONLY tables and columns that exist in the schema above
- Always use double-quotes around table and column names: SELECT "salesOrder" FROM "sales_order_headers"
- Use SQLite syntax (e.g., LIMIT instead of TOP, || for string concatenation)
- For amounts, they are stored as REAL (numeric)
- When joining, be precise about which column links which table
- LIMIT results to 20 rows max unless the user asks for all
- FORMATTING: Ensure the SQL string is valid JSON without raw newlines. Write the SQL on a single line or escape newlines as \n.
- Always return the JSON format, never plain text

IMPORTANT RULES FOR highlightNodes:
- Include entity IDs from query results that are most relevant
- Valid types: SalesOrder, Delivery, BillingDocument, JournalEntry, Payment, Customer, Product, Plant
- Keep highlight list to max 10 most relevant nodes
"""


def execute_sql(sql: str) -> tuple[list[dict], str]:
    """Execute a SQL query and return results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql)
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows, ""
    except Exception as e:
        return [], str(e)


def parse_llm_response(text: str) -> dict:
    """Parse the LLM response to extract SQL and explanation."""
    # Try to extract JSON from markdown code fence
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try raw JSON
    json_match = re.search(r'\{[^{}]*"sql"[^{}]*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # Try JSON with nested objects
    json_match = re.search(r'\{.*"sql".*"explanation".*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # If all JSON decodes fail, try manual regex extraction for SQL and explanation as fallback
    sql_match = re.search(r'"sql"\s*:\s*"(.*?)"(?:\s*,|\s*\n\s*\})', text, re.DOTALL)
    exp_match = re.search(r'"explanation"\s*:\s*"(.*?)"(?:\s*,|\s*\n\s*\})', text, re.DOTALL)
    
    if sql_match:
        sql_str = sql_match.group(1).replace('\\n', '\n').strip()
        exp_str = exp_match.group(1).replace('\\n', '\n').strip() if exp_match else "Extracted partial data."
        return {"sql": sql_str, "explanation": exp_str, "highlightNodes": []}

    return {"sql": "", "explanation": text, "highlightNodes": []}


# ─── Gemini LLM Call with Retry ──────────────────────
def _call_gemini(messages: list[dict], system_prompt: str, max_retries=3):
    """Call Gemini with retry on 429 rate limit errors."""
    last_error = None
    for model_name in GEMINI_MODELS:
        for attempt in range(max_retries):
            try:
                model = genai.GenerativeModel(
                    model_name=model_name,
                    system_instruction=system_prompt
                )
                chat_session = model.start_chat(history=messages[:-1] if len(messages) > 1 else [])
                last_msg = messages[-1]["parts"][0] if messages else ""
                response = chat_session.send_message(last_msg)
                return response.text, chat_session
            except Exception as e:
                last_error = e
                err_str = str(e)
                if "429" in err_str or "quota" in err_str.lower() or "rate" in err_str.lower():
                    wait = min(2 ** attempt * 5, 60)
                    print(f"Rate limited on {model_name}, attempt {attempt+1}, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                else:
                    # Non-rate-limit error, try next model
                    print(f"Error with {model_name}: {e}")
                    break
    raise last_error or Exception("All Gemini models failed")


# ─── Groq LLM Call ──────────────────────────────────
def _call_groq(messages_for_groq: list[dict], system_prompt: str):
    """Call Groq API as fallback."""
    full_messages = [{"role": "system", "content": system_prompt}] + messages_for_groq
    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=full_messages,
        temperature=0.1,
        max_tokens=2048,
    )
    return response.choices[0].message.content


# ─── Unified LLM Call ────────────────────────────────
def call_llm(user_message: str, history: list[dict] = None) -> str:
    """Call the best available LLM provider. Groq first, Gemini fallback."""
    sys_prompt = get_system_prompt()

    # Try Groq first (faster, no quota issues)
    if USE_GROQ:
        groq_msgs = []
        if history:
            for h in history[-6:]:
                groq_msgs.append({"role": h["role"], "content": h["content"]})
        groq_msgs.append({"role": "user", "content": user_message})
        try:
            return _call_groq(groq_msgs, sys_prompt)
        except Exception as e:
            print(f"Groq failed: {e}")
            if USE_GEMINI:
                print("Falling back to Gemini...")
            else:
                raise

    # Fallback to Gemini
    if USE_GEMINI:
        gemini_msgs = []
        if history:
            for h in history[-6:]:
                gemini_msgs.append({
                    "role": "user" if h["role"] == "user" else "model",
                    "parts": [h["content"]]
                })
        gemini_msgs.append({"role": "user", "parts": [user_message]})
        text, _ = _call_gemini(gemini_msgs, sys_prompt)
        return text

    raise Exception(
        "No LLM configured. Set GEMINI_API_KEY or GROQ_API_KEY in backend/.env. "
        "Get a free key: Gemini → https://ai.google.dev  |  Groq → https://console.groq.com"
    )


def call_llm_followup(original_msg: str, followup_msg: str, history: list[dict] = None) -> str:
    """Send a follow-up message (e.g. error correction or data summary)."""
    combined_history = list(history or [])
    combined_history.append({"role": "user", "content": original_msg})
    return call_llm(followup_msg, combined_history)


async def chat(message: str, history: list[dict] = None) -> dict:
    """Process a chat message: check guardrails, generate SQL via LLM, execute, and respond."""

    # Step 1: Pre-check guardrails
    is_relevant, rejection_msg = is_domain_relevant(message)
    if not is_relevant:
        return {
            "answer": rejection_msg,
            "sql": "", "data": [], "highlightNodes": [],
            "isGuardrail": True
        }

    # Step 2: Call LLM
    if not GEMINI_KEY and not USE_GROQ:
        return {
            "answer": "Error: No LLM API key configured. Set GEMINI_API_KEY or GROQ_API_KEY in backend/.env",
            "sql": "", "data": [], "highlightNodes": [],
            "isGuardrail": False
        }

    try:
        response_text = call_llm(message, history)
        parsed = parse_llm_response(response_text)

        # Check if LLM triggered guardrail
        if "designed to answer questions related to" in parsed.get("explanation", ""):
            return {
                "answer": parsed["explanation"],
                "sql": "", "data": [], "highlightNodes": [],
                "isGuardrail": True
            }

        sql = parsed.get("sql", "")
        explanation = parsed.get("explanation", "")
        highlight_nodes = parsed.get("highlightNodes", [])

        # Step 3: Execute SQL
        data = []
        if sql and sql.strip():
            sql_stripped = sql.strip().upper()
            if not sql_stripped.startswith("SELECT") and not sql_stripped.startswith("WITH"):
                return {
                    "answer": "For safety, only SELECT queries are allowed.",
                    "sql": sql, "data": [], "highlightNodes": [],
                    "isGuardrail": False
                }

            data, error = execute_sql(sql)

            if error:
                # Retry: ask LLM to fix the SQL
                retry_msg = f"The SQL query failed with error: {error}\nPlease fix the SQL and try again. Original query: {sql}"
                try:
                    retry_text = call_llm_followup(message, retry_msg, history)
                    parsed_retry = parse_llm_response(retry_text)
                    retry_sql = parsed_retry.get("sql", "")
                    if retry_sql:
                        data, error2 = execute_sql(retry_sql)
                        if not error2:
                            sql = retry_sql
                            explanation = parsed_retry.get("explanation", explanation)
                            highlight_nodes = parsed_retry.get("highlightNodes", highlight_nodes)
                        else:
                            explanation = f"I encountered an error executing the query. Error: {error2}"
                except Exception:
                    explanation = f"I encountered an error executing the query. Error: {error}"

            # Step 4: Generate natural language answer from data
            if data:
                data_summary = json.dumps(data[:20], indent=2, default=str)
                summary_msg = f"""The SQL query returned {len(data)} rows. Here are the results (first 20):
{data_summary}

Please provide a clear, concise natural language answer to the user's question based on this data.
Also identify any relevant entity IDs for graph highlighting.
Return in JSON format: {{"explanation": "your answer", "highlightNodes": [{{"type": "...", "id": "..."}}]}}"""

                try:
                    summary_text = call_llm_followup(message, summary_msg, history)
                    summary_parsed = parse_llm_response(summary_text)
                    if summary_parsed.get("explanation"):
                        explanation = summary_parsed["explanation"]
                    if summary_parsed.get("highlightNodes"):
                        highlight_nodes = summary_parsed["highlightNodes"]
                except Exception:
                    pass  # Keep original explanation if summary fails

        return {
            "answer": explanation,
            "sql": sql,
            "data": data[:50],
            "highlightNodes": highlight_nodes,
            "isGuardrail": False
        }

    except Exception as e:
        return {
            "answer": f"An error occurred: {str(e)}",
            "sql": "", "data": [], "highlightNodes": [],
            "isGuardrail": False
        }

async def stream_chat(message: str, history: list[dict] = None):
    """Simulate streaming answer delivery for SSE interface."""
    import asyncio
    
    result = await chat(message, history)
    
    answer_text = result.get("answer", "")
    if answer_text:
        # Split but preserve spacing roughly
        parts = re.split(r'(\s+)', answer_text)
        for part in parts:
            if part:
                yield f'data: {json.dumps({"token": part})}\n\n'
                await asyncio.sleep(0.01)
                
    final_payload = {
        "done": True,
        "sql": result.get("sql", ""),
        "highlightNodes": result.get("highlightNodes", []),
        "data": result.get("data", []),
        "isGuardrail": result.get("isGuardrail", False)
    }
    yield f'data: {json.dumps(final_payload)}\n\n'

# Note: Prompt optimization for broken flows enabled

# Note: Prompt optimization for broken flows enabled

# Note: Prompt optimization for broken flows enabled
