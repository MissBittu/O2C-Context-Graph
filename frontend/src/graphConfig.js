/** Node type → { color, emoji } */
export const NODE_TYPE_CONFIG = {
  SalesOrder:       { color: '#3b82f6', emoji: '🛒', hex: '#3b82f6' },
  SalesOrderItem:   { color: '#60a5fa', emoji: '📦', hex: '#60a5fa' },
  Delivery:         { color: '#34d399', emoji: '🚚', hex: '#34d399' },
  DeliveryItem:     { color: '#6ee7b7', emoji: '📋', hex: '#6ee7b7' },
  BillingDocument:  { color: '#f59e0b', emoji: '🧾', hex: '#f59e0b' },
  BillingItem:      { color: '#fcd34d', emoji: '📝', hex: '#fcd34d' },
  JournalEntry:     { color: '#a855f7', emoji: '📒', hex: '#a855f7' },
  Payment:          { color: '#ec4899', emoji: '💳', hex: '#ec4899' },
  Customer:         { color: '#22d3ee', emoji: '👤', hex: '#22d3ee' },
  Product:          { color: '#f97316', emoji: '🏷️', hex: '#f97316' },
  Plant:            { color: '#84cc16', emoji: '🏭', hex: '#84cc16' },
};

export function getNodeColor(type, highlighted = false, dimmed = false) {
  const cfg = NODE_TYPE_CONFIG[type] || { color: '#64748b' };
  if (dimmed) return '#2a3040';
  if (highlighted) return '#ffffff';
  return cfg.color;
}

export function getNodeSize(type) {
  const sizes = {
    SalesOrder: 7, Customer: 8, BillingDocument: 7, Delivery: 7,
    JournalEntry: 6, Payment: 6, Product: 6, Plant: 6,
    SalesOrderItem: 4, DeliveryItem: 4, BillingItem: 4
  };
  return sizes[type] || 5;
}

// Final aesthetic verification

// Final aesthetic verification

// Visual QA completed
