"""
FastAPI Main Application
Serves the Graph API and Chat API for the O2C Context Graph System.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import graph_model
import llm_engine

app = FastAPI(title="O2C Context Graph API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Models ───────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str
    history: Optional[list[dict]] = None


class ChatResponse(BaseModel):
    answer: str
    sql: str
    data: list
    highlightNodes: list
    isGuardrail: bool


# ─── Graph Endpoints ─────────────────────────────────

@app.get("/api/graph/overview")
async def graph_overview():
    """Get graph overview with node type counts and edge definitions."""
    return graph_model.get_overview()


@app.get("/api/graph/nodes/{node_type}")
async def graph_nodes(node_type: str, limit: int = 50, offset: int = 0, search: str = None):
    """Get nodes of a specific type."""
    if node_type not in graph_model.NODE_TYPES:
        raise HTTPException(status_code=404, detail=f"Unknown node type: {node_type}")
    nodes = graph_model.get_nodes(node_type, limit=limit, offset=offset, search=search)
    return {"nodes": nodes, "type": node_type}


@app.get("/api/graph/expand/{node_type}/{node_id}")
async def graph_expand(node_type: str, node_id: str):
    """Expand a node to show all connected neighbors."""
    if node_type not in graph_model.NODE_TYPES:
        raise HTTPException(status_code=404, detail=f"Unknown node type: {node_type}")
    result = graph_model.expand_node(node_type, node_id)
    return result


@app.get("/api/graph/node/{node_type}/{node_id}")
async def graph_node_detail(node_type: str, node_id: str):
    """Get full details of a specific node."""
    if node_type not in graph_model.NODE_TYPES:
        raise HTTPException(status_code=404, detail=f"Unknown node type: {node_type}")
    detail = graph_model.get_node_detail(node_type, node_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Node not found")
    return {"type": node_type, "id": node_id, "data": detail}


@app.get("/api/graph/schema")
async def graph_schema():
    """Get graph schema for display."""
    return {
        "nodeTypes": list(graph_model.NODE_TYPES.keys()),
        "edgeDefinitions": [
            {"source": e["source"], "target": e["target"], "label": e["label"]}
            for e in graph_model.EDGE_DEFS
        ]
    }


# ─── Chat Endpoint ───────────────────────────────────

from fastapi.responses import StreamingResponse

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a natural language query against the O2C dataset."""
    result = await llm_engine.chat(request.message, request.history)
    return ChatResponse(**result)

@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """Process a natural language query via SSE streaming."""
    return StreamingResponse(llm_engine.stream_chat(request.message, request.history), media_type="text/event-stream")


# ─── Health ──────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# SSE Streaming Endpoint integrated

# Deployment-ready configuration

# SSE Streaming Endpoint integrated

# Deployment-ready configuration
