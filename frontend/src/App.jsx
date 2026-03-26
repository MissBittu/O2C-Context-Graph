import React, { useState, useCallback, useEffect } from 'react';
import GraphViewer from './components/GraphViewer';
import ChatPanel from './components/ChatPanel';
import { NODE_TYPE_CONFIG } from './graphConfig';
import { fetchOverview, fetchNodes } from './api';

export default function App() {
  const [overview, setOverview] = useState(null);
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [activeType, setActiveType] = useState(null);
  const [highlightIds, setHighlightIds] = useState([]);
  const [loadingType, setLoadingType] = useState(null);
  const [fgRef, setFgRef] = useState(null);

  // Fetch overview on mount
  useEffect(() => {
    fetchOverview()
      .then(setOverview)
      .catch(err => console.error('Overview fetch failed:', err));
  }, []);

  const handleLoadNodeType = useCallback(async (type) => {
    if (loadingType) return;
    setActiveType(type);
    setLoadingType(type);
    setHighlightIds([]);

    try {
      const result = await fetchNodes(type, { limit: 60 });
      const nodes = result.nodes.map((n, i) => ({
        id: n.id,
        type: type,
        label: n.label,
        data: n,
        // Spread in a circle for initial layout
        x: Math.cos((i / result.nodes.length) * 2 * Math.PI) * 200,
        y: Math.sin((i / result.nodes.length) * 2 * Math.PI) * 200,
      }));

      setGraphData({ nodes, links: [] });
    } catch (err) {
      console.error('Load nodes error:', err);
    } finally {
      setLoadingType(null);
    }
  }, [loadingType]);

  const handleHighlightNodes = useCallback((highlightList) => {
    setHighlightIds(highlightList || []);
  }, []);

  const handleClearGraph = () => {
    setGraphData({ nodes: [], links: [] });
    setActiveType(null);
    setHighlightIds([]);
  };

  const handleZoomFit = () => {
    fgRef?.zoomToFit?.(400, 40);
  };

  // Group node types for sidebar display
  const FLOW_TYPES = ['SalesOrder', 'SalesOrderItem', 'Delivery', 'DeliveryItem', 'BillingDocument', 'BillingItem', 'JournalEntry', 'Payment'];
  const SUPPORT_TYPES = ['Customer', 'Product', 'Plant'];

  const getCount = (type) => {
    if (!overview) return '…';
    const found = overview.nodes?.find(n => n.type === type);
    return found ? found.count.toLocaleString() : '0';
  };

  const NodeTypeButton = ({ type }) => {
    const cfg = NODE_TYPE_CONFIG[type];
    return (
      <button
        key={type}
        className={`node-type-btn ${activeType === type ? 'active' : ''}`}
        onClick={() => handleLoadNodeType(type)}
        disabled={loadingType === type}
      >
        <div className="node-dot" style={{ background: cfg.color }} />
        <span>{cfg.emoji} {type}</span>
        <span className="node-count">{getCount(type)}</span>
      </button>
    );
  };

  return (
    <div className="app">
      {/* Header */}
      <header className="app-header">
        <div className="app-logo">
          <div className="logo-icon">⬡</div>
          <div>
            <div className="logo-text">O2C Context Graph</div>
            <div className="logo-sub">SAP Order-to-Cash Explorer</div>
          </div>
        </div>
        <div className="header-stats">
          <div className="stat-badge">
            <div className="stat-dot" />
            <span>Live Dataset</span>
          </div>
          <div className="stat-badge" style={{ color: 'var(--text-muted)' }}>
            {overview ? `${overview.nodes?.reduce((s, n) => s + n.count, 0).toLocaleString()} records` : 'Loading…'}
          </div>
          <div className="stat-badge" style={{ color: 'var(--text-muted)' }}>
            19 tables · {overview?.edges?.length || 15} relationships
          </div>
        </div>
      </header>

      <div className="app-body">
        {/* Left Sidebar */}
        <aside className="sidebar">
          <div className="sidebar-section">
            <div className="sidebar-title">O2C Flow</div>
            {FLOW_TYPES.map(type => <NodeTypeButton key={type} type={type} />)}
          </div>
          <div className="sidebar-section">
            <div className="sidebar-title">Supporting</div>
            {SUPPORT_TYPES.map(type => <NodeTypeButton key={type} type={type} />)}
          </div>

          <div className="sidebar-actions">
            {graphData.nodes.length > 0 && (
              <>
                <button className="btn btn-ghost btn-sm btn-full" style={{ marginBottom: 6 }} onClick={handleZoomFit}>
                  ⊞ Fit Graph
                </button>
                <button className="btn btn-ghost btn-sm btn-full" onClick={handleClearGraph}>
                  ✕ Clear Graph
                </button>
              </>
            )}
            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 10, lineHeight: 1.7 }}>
              Click a node type to load it.<br />
              Click any node on the graph to expand its connections.
            </div>
          </div>
        </aside>

        {/* Center: Graph */}
        <main className="graph-container" style={{ position: 'relative', flex: 1 }}>
          <GraphViewer
            graphData={graphData}
            highlightIds={highlightIds}
            onGraphRef={setFgRef}
          />
        </main>

        {/* Right: Chat */}
        <ChatPanel onHighlightNodes={handleHighlightNodes} />
      </div>
    </div>
  );
}

// Layout flexbox padding adjusted

// Layout flexbox padding adjusted
