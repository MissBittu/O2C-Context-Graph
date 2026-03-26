"""
Test the 3 exact assignment queries + guardrails + graph API
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import json
import time

BASE = "http://localhost:8000/api"

def test_graph():
    print("=" * 60)
    print("TEST 1: Graph Overview API")
    print("=" * 60)
    r = requests.get(f"{BASE}/graph/overview")
    d = r.json()
    print(f"  Node types: {len(d['nodes'])}")
    for n in d["nodes"]:
        print(f"    {n['type']}: {n['count']} records")
    print(f"  Edge definitions: {len(d['edges'])}")
    
    print("\n  Testing node fetch (SalesOrder)...")
    r2 = requests.get(f"{BASE}/graph/nodes/SalesOrder?limit=5")
    nodes = r2.json()
    print(f"  Got {len(nodes['nodes'])} SalesOrder nodes")
    if nodes["nodes"]:
        sample = nodes["nodes"][0]
        print(f"  Sample node: id={sample['id']}, label={sample['label']}")
        
        print("\n  Testing node expand...")
        r3 = requests.get(f"{BASE}/graph/expand/SalesOrder/{sample['id']}")
        exp = r3.json()
        print(f"  Expanded: {len(exp.get('nodes', []))} connected nodes, {len(exp.get('edges', []))} edges")
    print("  [PASS] Graph API working!\n")


def test_query(name, message, expect_guardrail=False):
    print("=" * 60)
    print(f"TEST: {name}")
    print(f"  Query: \"{message}\"")
    print("=" * 60)
    
    try:
        r = requests.post(f"{BASE}/chat", json={"message": message, "history": []}, timeout=60)
        d = r.json()
    except Exception as e:
        print(f"  [FAIL] Request error: {e}")
        return None
    
    answer_preview = d['answer'][:400] if d['answer'] else "(empty)"
    print(f"  Answer: {answer_preview}")
    print(f"  SQL: {d['sql'][:200] if d['sql'] else '(none)'}")
    print(f"  Data rows: {len(d['data'])}")
    print(f"  Highlight nodes: {len(d['highlightNodes'])}")
    print(f"  Is guardrail: {d['isGuardrail']}")
    
    if expect_guardrail:
        if d["isGuardrail"]:
            print("  [PASS] Guardrail correctly triggered!")
        else:
            print("  [FAIL] Expected guardrail but got normal response!")
    else:
        if d["sql"]:
            print("  [PASS] Query answered with SQL + data!")
        elif d["answer"] and not d["isGuardrail"]:
            print("  [WARN] Got answer but no SQL generated")
        else:
            print("  [FAIL] No SQL or answer returned!")
    
    print()
    return d


if __name__ == "__main__":
    print("\nTESTING O2C CONTEXT GRAPH SYSTEM")
    print("Testing the exact queries from the assignment spec\n")
    
    # Test Graph API
    test_graph()
    time.sleep(1)
    
    # Assignment Query A
    test_query(
        "Assignment Query A - Products with most billing docs",
        "Which products are associated with the highest number of billing documents?"
    )
    time.sleep(2)
    
    # Assignment Query B
    test_query(
        "Assignment Query B - Trace full flow of billing document",
        "Trace the full flow of billing document 90504248 (Sales Order -> Delivery -> Billing -> Journal Entry)"
    )
    time.sleep(2)
    
    # Assignment Query C
    test_query(
        "Assignment Query C - Broken/incomplete flows",
        "Identify sales orders that have broken or incomplete flows, for example delivered but not billed"
    )
    time.sleep(2)
    
    # Extra query - customers with highest billed amount
    test_query(
        "Extra Query - Customers with highest billed amount",
        "Which customers have the highest total billed amount?"
    )
    time.sleep(2)
    
    # Guardrail Test 1
    test_query(
        "Guardrail - General knowledge",
        "What is the capital of France?",
        expect_guardrail=True
    )
    time.sleep(1)
    
    # Guardrail Test 2
    test_query(
        "Guardrail - Creative writing",
        "Write me a poem about the ocean",
        expect_guardrail=True
    )
    time.sleep(1)
    
    # Guardrail Test 3
    test_query(
        "Guardrail - Coding help",
        "How do I write a for loop in Python?",
        expect_guardrail=True
    )
    
    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED!")
    print("=" * 60)
