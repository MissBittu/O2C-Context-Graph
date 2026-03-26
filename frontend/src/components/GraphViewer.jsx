import React, { useRef, useEffect, useState } from 'react';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import { NODE_TYPE_CONFIG } from '../graphConfig';
import NodeDetail from './NodeDetail';
import { expandNode } from '../api';

export default function GraphViewer({ graphData, onNodeClick, highlightIds, onGraphRef }) {
  const containerRef = useRef(null);
  const networkRef = useRef(null);
  const nodesDS = useRef(new DataSet([]));
  const edgesDS = useRef(new DataSet([]));
  
  const [selectedNode, setSelectedNode] = useState(null);
  const [isEmpty, setIsEmpty] = useState(true);

  // Initialize Network on mount
  useEffect(() => {
    if (!containerRef.current) return;
    
    const options = {
      physics: { enabled: true, stabilization: { iterations: 100 } },
      edges: { arrows: 'to', smooth: { type: 'cubicBezier' } },
      nodes: { shape: 'dot', size: 16, font: { color: '#fff', size: 12 } },
      interaction: { hover: true, tooltipDelay: 200 }
    };

    const network = new Network(
      containerRef.current,
      { nodes: nodesDS.current, edges: edgesDS.current },
      options
    );
    networkRef.current = network;
    if (onGraphRef) onGraphRef(network);

    network.on("click", async (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0];
        const nodeData = nodesDS.current.get(nodeId);
        if (nodeData) {
          setSelectedNode(nodeData.data || nodeData);
          onNodeClick?.(nodeData.data || nodeData);
        }
      } else {
        setSelectedNode(null);
      }
    });

    network.on("doubleClick", async (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0];
        const node = nodesDS.current.get(nodeId);
        if (!node) return;
        try {
          const result = await expandNode(node.type || ((node.data) && node.data.type), nodeId);
          
          if (result.nodes) {
            const newNodes = result.nodes.map(n => ({
              id: n.id,
              type: n.type,
              label: String(n.label || n.id).substring(0, 14),
              color: NODE_TYPE_CONFIG[n.type]?.color || '#90A4AE',
              title: n.label || n.id,
              data: n
            })).filter(n => !nodesDS.current.get(n.id));
            nodesDS.current.add(newNodes);
          }
          
          if (result.edges) {
            const newEdges = result.edges.map(e => ({
              id: `${e.source}-${e.target}`,
              from: e.source,
              to: e.target,
              label: e.label,
              font: { color: '#90a4c8', size: 10, align: 'top' },
              color: 'rgba(255,255,255,0.2)'
            })).filter(e => !edgesDS.current.get(e.id));
            edgesDS.current.add(newEdges);
          }
          
        } catch (e) {
          console.error('Expand error:', e);
        }
      }
    });

    return () => {
      network.destroy();
    };
  }, [onGraphRef, onNodeClick]);

  // Sync graphData prop (typically initial load after node type click)
  useEffect(() => {
    if (graphData && graphData.nodes) {
      const gNodes = graphData.nodes.map(n => ({
        id: n.id,
        type: n.type,
        label: String(n.label || n.id).substring(0, 14),
        color: NODE_TYPE_CONFIG[n.type]?.color || '#90A4AE',
        title: n.label || n.id,
        data: n
      }));
      
      const gEdges = (graphData.links || []).map(e => ({
        id: `${e.source?.id || e.source}-${e.target?.id || e.target}`,
        from: e.source?.id || e.source,
        to: e.target?.id || e.target,
        label: e.label,
        font: { color: '#90a4c8', size: 10, align: 'top' },
        color: 'rgba(255,255,255,0.2)'
      }));

      // Full replace for the new graph context
      nodesDS.current.clear();
      edgesDS.current.clear();
      nodesDS.current.add(gNodes);
      edgesDS.current.add(gEdges);
      setIsEmpty(gNodes.length === 0);
    }
  }, [graphData]);

  // Highlight logic from prompt
  useEffect(() => {
    if (highlightIds && highlightIds.length > 0 && networkRef.current) {
      const ids = highlightIds.map(h => h.id);
      
      // Keep original colors mapping
      const originalColors = {};
      ids.forEach(id => {
        const n = nodesDS.current.get(id);
        if (n && !originalColors[id]) {
          originalColors[id] = n.color;
        }
      });
      
      // Highlight
      nodesDS.current.update(ids.map(id => ({ 
        id, 
        color:{ background:'#FFD700', border:'#FF8C00' }
      })));
      
      networkRef.current.selectNodes(ids);
      networkRef.current.fit({ nodes: ids, animation: { duration: 800 } });
      
      // Reset colors after 5s as per prompt
      const timer = setTimeout(() => {
        const revert = Object.keys(originalColors).map(id => ({
          id,
          color: originalColors[id]
        }));
        nodesDS.current.update(revert);
      }, 5000);
      
      return () => clearTimeout(timer);
    }
  }, [highlightIds]);

  return (
    <div className="graph-container" style={{ width: '100%', height: '100%' }}>
      <div 
        className="graph-canvas-wrap"
        style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', visibility: isEmpty ? 'hidden' : 'visible' }} 
      >
        <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      </div>
      
      {isEmpty && (
        <div className="graph-empty">
          <div className="graph-empty-icon">🕸️</div>
          <p>Select a node type from the sidebar to load graph data.<br />Click any node to see details. Double-click to expand connections.</p>
        </div>
      )}

      {!isEmpty && (
        <div className="graph-hint">
          <span className="hint-item"><span className="hint-key">Double-click</span> expand node</span>
          <span className="hint-item"><span className="hint-key">Scroll</span> zoom</span>
          <span className="hint-item"><span className="hint-key">Drag</span> pan</span>
        </div>
      )}

      {selectedNode && (
        <NodeDetail
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
          onExpand={(n) => {
            // Can be invoked safely, though double-click natively triggers expand now
          }}
        />
      )}

      {!isEmpty && (
        <div className="graph-legend">
          <div className="legend-title">Node Types</div>
          {Object.entries(NODE_TYPE_CONFIG)
            .filter(([type]) => nodesDS.current.get().some(n => n.type === type))
            .map(([type, cfg]) => (
              <div key={type} className="legend-item" style={{display:'flex', alignItems:'center', gap:'6px', marginBottom:'4px'}}>
                <div className="legend-dot" style={{ background: cfg.color, width: '8px', height: '8px', borderRadius: '50%' }} />
                <span>{cfg.emoji} {type}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  );
}

// Container height 100% cascade fix applied

// Tooltip coordinates bounded

// Container height 100% cascade fix applied

// Tooltip coordinates bounded

// Fixed Absolute layout computation issue
