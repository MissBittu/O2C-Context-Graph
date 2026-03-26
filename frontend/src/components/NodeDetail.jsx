import React from 'react';
import { NODE_TYPE_CONFIG } from '../graphConfig';

export default function NodeDetail({ node, onClose, onExpand }) {
  if (!node) return null;
  
  const cfg = NODE_TYPE_CONFIG[node.type] || { color: '#64748b', emoji: '⬤' };
  const data = node.data || {};

  const importantKeys = ['salesOrder', 'deliveryDocument', 'billingDocument', 
    'accountingDocument', 'businessPartner', 'product', 'plant', 'material',
    'totalNetAmount', 'netAmount', 'transactionCurrency', 'creationDate',
    'soldToParty', 'referenceSdDocument', 'organizationBpName1', 'cityName', 'country',
    'productDescription', 'actualDeliveryQuantity', 'billingQuantity', 'clearingDate'];
  
  const displayData = Object.entries(data).filter(([k, v]) => 
    v !== null && v !== '' && v !== undefined
  );

  // Sort: important keys first
  displayData.sort(([a], [b]) => {
    const ai = importantKeys.indexOf(a);
    const bi = importantKeys.indexOf(b);
    if (ai !== -1 && bi !== -1) return ai - bi;
    if (ai !== -1) return -1;
    if (bi !== -1) return 1;
    return 0;
  });

  return (
    <div className="node-detail-panel">
      <div className="node-detail-header">
        <div className="node-detail-type" style={{ color: cfg.color }}>
          <div className="node-dot" style={{ background: cfg.color }} />
          {cfg.emoji} {node.type}
        </div>
        <button className="node-detail-close" onClick={onClose}>✕</button>
      </div>

      <div className="node-detail-body">
        <div className="detail-row" style={{ marginBottom: 12 }}>
          <span className="detail-key">Node ID</span>
          <span className="detail-val highlight">{node.id}</span>
        </div>
        {displayData.slice(0, 20).map(([k, v]) => (
          <div key={k} className="detail-row">
            <span className="detail-key">{camelToLabel(k)}</span>
            <span className="detail-val">{String(v).substring(0, 60)}{String(v).length > 60 ? '…' : ''}</span>
          </div>
        ))}
        {displayData.length > 20 && (
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>
            +{displayData.length - 20} more fields
          </div>
        )}
      </div>

      <div className="node-detail-actions">
        <button
          className="btn btn-primary btn-sm btn-full"
          onClick={() => onExpand(node)}
        >
          🔍 Expand Connections
        </button>
      </div>
    </div>
  );
}

function camelToLabel(str) {
  return str
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, s => s.toUpperCase())
    .trim();
}
