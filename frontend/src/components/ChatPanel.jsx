import React, { useState, useRef, useEffect, useCallback } from 'react';
import { sendChat } from '../api';
import { NODE_TYPE_CONFIG } from '../graphConfig';

const SAMPLE_QUERIES = [
  'Which products are associated with the highest number of billing documents?',
  'Trace the full flow of billing document 90504248',
  'Identify sales orders delivered but not billed',
  'Which customers have the highest total billed amount?',
  'Show sales orders with broken flows (billed without delivery)',
  'Which plants handle the most deliveries?',
];

const GUARDRAIL_CHECK = 'designed to answer questions related to';

export default function ChatPanel({ onHighlightNodes }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [sqlVisible, setSqlVisible] = useState({});
  const [dataVisible, setDataVisible] = useState({});
  const bottomRef = useRef(null);
  const textareaRef = useRef(null);

  // Scroll to bottom on new messages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const getHistory = useCallback(() =>
    messages
      .filter(m => m.type === 'user' || m.type === 'assistant')
      .map(m => ({ role: m.type, content: m.text }))
      .slice(-12),
    [messages]
  );

  const handleSend = useCallback(async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;

    setInput('');
    const userMsgId = Date.now();
    const aiMsgId = userMsgId + 1;
    
    setMessages(prev => [
      ...prev, 
      { id: userMsgId, type: 'user', text: msg },
      { id: aiMsgId, type: 'assistant', text: '', isGuardrail: false, sql: '', data: [], highlightNodes: [] }
    ]);
    
    setLoading(true);

    try {
      const response = await fetch('http://localhost:8000/api/chat/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, history: getHistory() })
      });
      
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n\n'); 
        buf = lines.pop(); // Keep incomplete chunk
        
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const d = JSON.parse(line.slice(6));
          
          if (d.token) {
            setMessages(prev => prev.map(m => m.id === aiMsgId ? { ...m, text: m.text + d.token } : m));
          }
          
          if (d.done) {
            setMessages(prev => prev.map(m => m.id === aiMsgId ? { 
              ...m, 
              sql: d.sql, 
              data: d.data, 
              highlightNodes: d.highlightNodes || [],
              isGuardrail: d.isGuardrail
            } : m));
            
            if (d.highlightNodes?.length > 0) {
              onHighlightNodes?.(d.highlightNodes);
            }
          }
        }
      }
    } catch (err) {
      setMessages(prev => prev.map(m => m.id === aiMsgId ? { 
        ...m, 
        text: `Error: ${err.message}. Make sure the backend is running at localhost:8000.` 
      } : m));
    } finally {
      setLoading(false);
    }
  }, [input, loading, getHistory, onHighlightNodes]);

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const toggleSql = (id) => setSqlVisible(prev => ({ ...prev, [id]: !prev[id] }));
  const toggleData = (id) => setDataVisible(prev => ({ ...prev, [id]: !prev[id] }));

  const handleClear = () => {
    setMessages([]);
    onHighlightNodes?.([]);
  };

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <div className="chat-title">
          <span>💬</span>
          <span>AI Query Engine</span>
          <span className="chat-ai-badge">Gemini</span>
        </div>
        {messages.length > 0 && (
          <button className="btn btn-ghost btn-sm" onClick={handleClear} title="Clear chat">
            🗑
          </button>
        )}
      </div>

      <div className="chat-messages">
        {messages.length === 0 && !loading && (
          <div className="chat-welcome">
            <div className="welcome-icon">🔍</div>
            <div className="welcome-title">Ask about your data</div>
            <div className="welcome-desc">
              Query the SAP Order-to-Cash graph using natural language. The AI translates your questions into SQL and returns data-backed answers.
            </div>
            <div className="sample-queries">
              {SAMPLE_QUERIES.map((q, i) => (
                <button key={i} className="sample-query" onClick={() => handleSend(q)}>
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <div key={msg.id} className={`message ${msg.type} ${msg.isGuardrail ? 'guardrail' : ''}`}>
            <div className="message-role">{msg.type === 'user' ? 'You' : 'AI'}</div>
            <div className="message-bubble">
              <MessageText text={msg.text} />
            </div>

            {/* SQL toggle */}
            {msg.sql && (
              <div className="message-sql">
                <button className="sql-toggle" onClick={() => toggleSql(msg.id)}>
                  <span>📊</span>
                  {sqlVisible[msg.id] ? 'Hide SQL' : 'View SQL'}
                  <span style={{ marginLeft: 2 }}>{sqlVisible[msg.id] ? '▲' : '▼'}</span>
                </button>
                {sqlVisible[msg.id] && (
                  <div className="sql-code">{msg.sql}</div>
                )}
              </div>
            )}

            {/* Data table toggle */}
            {msg.data?.length > 0 && (
              <div className="message-data">
                <button className="sql-toggle" onClick={() => toggleData(msg.id)}>
                  <span>📋</span>
                  {dataVisible[msg.id] ? 'Hide results' : `View ${msg.data.length} row${msg.data.length !== 1 ? 's' : ''}`}
                  <span style={{ marginLeft: 2 }}>{dataVisible[msg.id] ? '▲' : '▼'}</span>
                </button>
                {dataVisible[msg.id] && (
                  <>
                    <DataTable rows={msg.data} />
                    <div className="data-count">{msg.data.length} row(s) returned</div>
                  </>
                )}
              </div>
            )}

            {/* Highlight node chips */}
            {msg.highlightNodes?.length > 0 && (
              <div className="highlight-nodes">
                {msg.highlightNodes.slice(0, 8).map((n, i) => {
                  const cfg = NODE_TYPE_CONFIG[n.type] || {};
                  return (
                    <span key={i} className="node-chip"
                      style={{ color: cfg.color, borderColor: cfg.color + '44', background: cfg.color + '11' }}>
                      {cfg.emoji} {n.id}
                    </span>
                  );
                })}
                {msg.highlightNodes.length > 8 && (
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', alignSelf: 'center' }}>
                    +{msg.highlightNodes.length - 8} more
                  </span>
                )}
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div className="message assistant">
            <div className="message-role">AI</div>
            <div className="message-loading">
              <div className="spinner">
                <span /><span /><span />
              </div>
              Analyzing your query...
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      <div className="chat-input-area">
        <div className="chat-input-row">
          <textarea
            ref={textareaRef}
            className="chat-textarea"
            placeholder="Ask about orders, deliveries, billing, payments…"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={1}
            disabled={loading}
          />
          <button
            className="chat-send-btn"
            onClick={() => handleSend()}
            disabled={!input.trim() || loading}
            title="Send (Enter)"
          >
            ↑
          </button>
        </div>
        <div className="chat-hint">Press Enter to send · Shift+Enter for new line</div>
      </div>
    </div>
  );
}

function MessageText({ text }) {
  if (!text) return null;
  // Simple formatting: bold **...**
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return (
    <>
      {parts.map((part, i) =>
        part.startsWith('**') && part.endsWith('**')
          ? <strong key={i}>{part.slice(2, -2)}</strong>
          : <span key={i}>{part}</span>
      )}
    </>
  );
}

function DataTable({ rows }) {
  if (!rows?.length) return null;
  const cols = Object.keys(rows[0]);
  const displayCols = cols.slice(0, 6); // Cap at 6 columns

  return (
    <div style={{ overflowX: 'auto', marginTop: 6, borderRadius: 8, border: '1px solid var(--border)' }}>
      <table className="data-table">
        <thead>
          <tr>
            {displayCols.map(c => <th key={c}>{c}</th>)}
            {cols.length > 6 && <th>+{cols.length - 6} more</th>}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 10).map((row, i) => (
            <tr key={i}>
              {displayCols.map(c => (
                <td key={c} title={String(row[c] ?? '')}>
                  {String(row[c] ?? '').substring(0, 20)}{String(row[c] ?? '').length > 20 ? '…' : ''}
                </td>
              ))}
              {cols.length > 6 && <td style={{ color: 'var(--text-muted)' }}>…</td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Streaming text decoder implemented

// Added quick action sample queries

// Streaming text decoder implemented

// Added quick action sample queries

// Streaming text decoder logic active
