const BASE = 'http://localhost:8000/api';

export async function fetchOverview() {
  const res = await fetch(`${BASE}/graph/overview`);
  if (!res.ok) throw new Error('Failed to fetch overview');
  return res.json();
}

export async function fetchNodes(nodeType, { limit = 50, offset = 0, search } = {}) {
  const params = new URLSearchParams({ limit, offset });
  if (search) params.append('search', search);
  const res = await fetch(`${BASE}/graph/nodes/${nodeType}?${params}`);
  if (!res.ok) throw new Error(`Failed to fetch nodes: ${nodeType}`);
  return res.json();
}

export async function expandNode(nodeType, nodeId) {
  const res = await fetch(`${BASE}/graph/expand/${nodeType}/${encodeURIComponent(nodeId)}`);
  if (!res.ok) throw new Error('Failed to expand node');
  return res.json();
}

export async function fetchNodeDetail(nodeType, nodeId) {
  const res = await fetch(`${BASE}/graph/node/${nodeType}/${encodeURIComponent(nodeId)}`);
  if (!res.ok) throw new Error('Failed to fetch node detail');
  return res.json();
}

export async function sendChat(message, history = []) {
  const res = await fetch(`${BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history })
  });
  if (!res.ok) throw new Error('Chat request failed');
  return res.json();
}

// Error boundaries caught in promises

// Error boundaries caught in promises
