// Bundled into viewer/vendor/force-graph-3d-widget.js — deliberately as
// close to upstream's own "large-graph" example
// (vasturiano/react-force-graph, example/large-graph/index.html) as this
// page's constraints allow: graphData + nodeLabel + nodeAutoColorBy +
// linkDirectionalParticles, nothing else. That example's whole point is
// that a graph too big for hand-tuned custom rendering (see
// force-graph-widget-src/entry.js's 4-node custom canvas boxes) still
// reads fine off the library's own defaults — same lesson applies at
// this dataset's 1000 nodes.
import React from 'react';
import { createRoot } from 'react-dom/client';
import ForceGraph3D from 'react-force-graph-3d';

const { useRef, useEffect, useState } = React;

function GraphWidget({ graphData, height }) {
  const containerRef = useRef();
  const [width, setWidth] = useState(900);

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

  return React.createElement(
    'div',
    { ref: containerRef, style: { width: '100%' } },
    React.createElement(ForceGraph3D, {
      graphData,
      width,
      height,
      nodeLabel: (n) => `${n.label} · ${n.name}`,
      nodeAutoColorBy: 'label',
      linkDirectionalParticles: 1,
    })
  );
}

const roots = new WeakMap();

window.DOIForceGraph3D = {
  render(container, props) {
    let root = roots.get(container);
    if (!root) {
      root = createRoot(container);
      roots.set(container, root);
    }
    root.render(React.createElement(GraphWidget, props));
  },
};
