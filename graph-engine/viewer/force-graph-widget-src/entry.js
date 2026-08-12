// Bundled into viewer/vendor/force-graph-widget.js by
// examples/build-force-graph-widget.sh (esbuild, IIFE, no externals — the
// whole point is one self-hosted file with zero runtime CDN dependency,
// consistent with the rest of this page). Exposes window.DOIForceGraph,
// a small imperative API so index.html's existing vanilla-JS query/status
// logic (computeStatuses, wireUp, ...) doesn't itself need to be React —
// only this one diagram widget is a React component internally.
import React from 'react';
import { createRoot } from 'react-dom/client';
import ForceGraph2D from 'react-force-graph-2d';

const { useRef, useEffect, useState, useMemo, useCallback } = React;

function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

// Read once per mount (this page has no live light/dark toggle — theme is
// decided by prefers-color-scheme/data-theme before first paint).
function readPalette() {
  return {
    inkFaint: cssVar('--ink-faint', '#948da8'),
    accentFlow: cssVar('--accent-flow', '#632ca6'),
    resultRing: cssVar('--result-ring', '#632ca6'),
    excludedFill: cssVar('--excluded-fill', '#f1eef8'),
    excludedStroke: cssVar('--excluded-stroke', '#d3cbe6'),
    excludedText: cssVar('--excluded-text', '#a79cc2'),
    danger: cssVar('--danger', '#a3244f'),
    nodeRds: cssVar('--node-rds', '#21a06f'),
    nodeCompute: cssVar('--node-compute', '#3f7fe0'),
    nodeRole: cssVar('--node-role', '#6b4fa0'),
    nodeStore: cssVar('--node-store', '#b2504a'),
    border: cssVar('--border', '#e3ddf0'),
  };
}

const NODE_W = 200;
const NODE_H = 56;
const NODE_R = 8;

function roundRectPath(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

const KIND_COLOR = {
  compute: 'nodeCompute',
  role: 'nodeRole',
  store: 'nodeStore',
};

function fillFor(node, mode, pal) {
  if (mode === 'query' && node.status === 'excluded') return pal.excludedFill;
  return pal[KIND_COLOR[node.kind]] || pal.nodeCompute;
}

function strokeFor(node, mode, pal) {
  if (node.highlight) return pal.danger;
  if (mode === 'query') {
    if (node.status === 'excluded') return pal.excludedStroke;
    if (node.status === 'included') return pal.resultRing;
  }
  return pal[KIND_COLOR[node.kind]] || pal.nodeCompute;
}

function GraphWidget({ nodes, links, mode, height }) {
  const fgRef = useRef();
  const containerRef = useRef();
  const [width, setWidth] = useState(900);
  const pal = useMemo(readPalette, []);

  useEffect(() => {
    if (!containerRef.current) return;
    const el = containerRef.current;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w) setWidth(Math.max(320, Math.round(w)));
    });
    ro.observe(el);
    setWidth(Math.max(320, Math.round(el.clientWidth)));
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    const t = setTimeout(() => {
      // zoomToFit's bounding box is computed from node x/y points only —
      // it has no notion of the 200x56 rounded-rect boxes actually drawn
      // by nodeCanvasObject, so a small padding clips them. Pad by half
      // the box diagonal plus margin instead of a cosmetic constant.
      if (fgRef.current) fgRef.current.zoomToFit(300, NODE_W / 2 + 30);
    }, 60);
    return () => clearTimeout(t);
  }, [width, height, nodes, links, mode]);

  const nodeCanvasObject = useCallback(
    (node, ctx) => {
      const fill = fillFor(node, mode, pal);
      const stroke = strokeFor(node, mode, pal);
      const lineWidth = mode === 'query' && node.status === 'included' ? 3 : node.highlight ? 2.5 : 1.6;

      roundRectPath(ctx, node.x - NODE_W / 2, node.y - NODE_H / 2, NODE_W, NODE_H, NODE_R);
      ctx.fillStyle = fill;
      ctx.fill();
      ctx.lineWidth = lineWidth;
      ctx.strokeStyle = stroke;
      ctx.stroke();

      const dimmed = mode === 'query' && node.status === 'excluded';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.font = '600 13px ui-sans-serif, -apple-system, "Segoe UI", Roboto, sans-serif';
      ctx.fillStyle = dimmed ? pal.excludedText : '#fff';
      ctx.fillText(node.name, node.x, node.y - 9);
      ctx.font = '10px ui-monospace, "SFMono-Regular", Consolas, monospace';
      ctx.fillStyle = dimmed ? pal.excludedText : 'rgba(255,255,255,0.82)';
      ctx.fillText(node.sub, node.x, node.y + 8);

      if (mode === 'query') {
        ctx.font = '600 10px ui-monospace, "SFMono-Regular", Consolas, monospace';
        if (node.status === 'included' && node.badge) {
          ctx.fillStyle = pal.danger;
          ctx.fillText(node.badge, node.x, node.y + NODE_H / 2 + 13);
        } else if (node.status === 'excluded' && node.reason) {
          ctx.fillStyle = pal.excludedText;
          ctx.fillText(node.reason, node.x, node.y + NODE_H / 2 + 13);
        }
      }
    },
    [mode, pal]
  );

  const nodePointerAreaPaint = useCallback((node, color, ctx) => {
    ctx.fillStyle = color;
    roundRectPath(ctx, node.x - NODE_W / 2, node.y - NODE_H / 2, NODE_W, NODE_H, NODE_R);
    ctx.fill();
  }, []);

  const linkColor = useCallback(
    (link) => {
      if (link.kind === 'accessed') return pal.nodeRds;
      if (mode === 'query') return link.state === 'in-range' ? pal.accentFlow : pal.excludedStroke;
      return pal.inkFaint;
    },
    [mode, pal]
  );

  const linkWidth = useCallback(
    (link) => {
      if (link.kind === 'accessed') return 2;
      if (mode === 'query') return link.state === 'in-range' ? 2.2 : 1;
      return link.kind === 'can_read' ? 1.3 : 1.6;
    },
    [mode]
  );

  const linkLineDash = useCallback(
    (link) => {
      if (link.kind === 'can_read') return [3, 4];
      if (mode === 'query' && link.kind !== 'accessed' && link.state !== 'in-range') return [2, 4];
      return null;
    },
    [mode]
  );

  const linkCanvasObjectMode = useCallback(() => 'after', []);
  const linkCanvasObject = useCallback(
    (link, ctx) => {
      if (link.kind !== 'accessed' || !link.label) return;
      const midX = (link.source.x + link.target.x) / 2;
      const midY = (link.source.y + link.target.y) / 2 - 8;
      ctx.font = '600 10px ui-monospace, "SFMono-Regular", Consolas, monospace';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillStyle = pal.nodeRds;
      ctx.fillText(link.label, midX, midY);
    },
    [pal]
  );

  return React.createElement('div', { ref: containerRef, style: { width: '100%' } },
    React.createElement(ForceGraph2D, {
      ref: fgRef,
      graphData: { nodes, links },
      width,
      height,
      backgroundColor: 'rgba(0,0,0,0)',
      nodeCanvasObject,
      nodePointerAreaPaint,
      nodeLabel: (n) => n.title || '',
      linkLabel: (l) => l.title || '',
      linkColor,
      linkWidth,
      linkLineDash,
      linkDirectionalArrowLength: 6,
      linkDirectionalArrowRelPos: 1,
      linkCanvasObjectMode,
      linkCanvasObject,
      cooldownTicks: 60,
      enableNodeDrag: true,
    })
  );
}

const roots = new WeakMap();

window.DOIForceGraph = {
  render(container, props) {
    let root = roots.get(container);
    if (!root) {
      root = createRoot(container);
      roots.set(container, root);
    }
    root.render(React.createElement(GraphWidget, props));
  },
};
