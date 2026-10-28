const fs = require("fs");
try {
  const [inp, out] = process.argv.slice(2);
  if (!inp || !out) throw new Error("usage: node ua-tour-analyze.js <input> <output>");
  const { nodes = [], edges = [], layers = [] } = JSON.parse(fs.readFileSync(inp, "utf8"));
  const byId = new Map(nodes.map(n => [n.id, n]));
  const byPath = new Map(nodes.filter(n => n.filePath).map(n => [n.filePath, n.id]));
  // lift sub-file nodes (class:path:Name, function:path:name) to their file node
  const lift = id => {
    if (byId.has(id)) return id;
    const m = /^[a-z_]+:(.+?)(?::[^:/]+)?$/.exec(id);
    if (m && byPath.has(m[1])) return byPath.get(m[1]);
    const rest = id.slice(id.indexOf(":") + 1);
    for (const [p, nid] of byPath) if (rest.startsWith(p + ":") || rest === p) return nid;
    return null;
  };
  const fe = []; const seen = new Set();
  for (const e of edges) {
    const s = lift(e.source), t = lift(e.target);
    if (!s || !t || s === t) continue;
    const k = s + "|" + t + "|" + e.type; if (seen.has(k)) continue; seen.add(k);
    fe.push({ source: s, target: t, type: e.type });
  }
  const inSet = new Map(), outSet = new Map();
  nodes.forEach(n => { inSet.set(n.id, new Set()); outSet.set(n.id, new Set()); });
  fe.forEach(e => { inSet.get(e.target).add(e.source); outSet.get(e.source).add(e.target); });
  const fanIn = nodes.map(n => ({ id: n.id, fanIn: inSet.get(n.id).size, name: n.name })).sort((a, b) => b.fanIn - a.fanIn);
  const fanOut = nodes.map(n => ({ id: n.id, fanOut: outSet.get(n.id).size, name: n.name })).sort((a, b) => b.fanOut - a.fanOut);
  const ENTRY = new Set("index.ts index.js main.ts main.js app.ts app.js server.ts server.js mod.rs main.go main.py main.rs manage.py app.py wsgi.py asgi.py run.py __main__.py Application.java Main.java Program.cs config.ru index.php App.swift Application.kt main.cpp main.c".split(" "));
  const outVals = fanOut.map(x => x.fanOut).sort((a, b) => b - a);
  const topOut = outVals[Math.max(0, Math.ceil(outVals.length * 0.1) - 1)];
  const inVals = fanIn.map(x => x.fanIn).sort((a, b) => a - b);
  const lowIn = inVals[Math.max(0, Math.floor(inVals.length * 0.25) - 1)];
  const cands = nodes.map(n => {
    const fp = n.filePath || ""; const depth = fp.split("/").length - 1; let s = 0;
    if (n.type === "document") {
      if (fp === "README.md") s += 5; else if (depth === 0 && fp.endsWith(".md")) s += 2;
    } else if (n.type === "file") {
      if (ENTRY.has(n.name)) s += 3;
      if (depth <= 1) s += 1;
      if (outSet.get(n.id).size >= topOut && topOut > 0) s += 1;
      if (inSet.get(n.id).size <= lowIn) s += 1;
    }
    return { id: n.id, score: s, name: n.name, type: n.type, summary: n.summary };
  }).filter(c => c.score > 0).sort((a, b) => b.score - a.score).slice(0, 5);
  const codeStart = cands.find(c => c.type !== "document" && ENTRY.has(c.name)) || cands.find(c => c.type !== "document");
  const bfs = { startNode: codeStart ? codeStart.id : null, order: [], depthMap: {}, byDepth: {} };
  if (codeStart) {
    const q = [codeStart.id]; bfs.depthMap[codeStart.id] = 0;
    while (q.length) {
      const c = q.shift(); bfs.order.push(c); const d = bfs.depthMap[c];
      (bfs.byDepth[d] = bfs.byDepth[d] || []).push(c);
      for (const e of fe) if (e.source === c && (e.type === "imports" || e.type === "calls") && !(e.target in bfs.depthMap)) { bfs.depthMap[e.target] = d + 1; q.push(e.target); }
    }
  }
  const pick = n => ({ id: n.id, name: n.name, type: n.type, summary: n.summary });
  const nonCode = {
    documentation: nodes.filter(n => n.type === "document").map(pick),
    infrastructure: nodes.filter(n => ["service", "pipeline", "resource"].includes(n.type)).map(pick),
    data: nodes.filter(n => ["table", "schema", "endpoint"].includes(n.type)).map(pick),
    config: nodes.filter(n => n.type === "config").map(pick),
  };
  const rel = new Set(fe.filter(e => e.type === "imports" || e.type === "calls").map(e => e.source + "|" + e.target));
  const adj = new Map(); nodes.forEach(n => adj.set(n.id, new Set()));
  fe.forEach(e => { adj.get(e.source).add(e.target); adj.get(e.target).add(e.source); });
  const clusters = []; const used = new Set();
  for (const k of rel) {
    const [a, b] = k.split("|"); if (!rel.has(b + "|" + a) || used.has(a) || used.has(b)) continue;
    const cl = new Set([a, b]);
    let grew = true;
    while (grew && cl.size < 5) { grew = false;
      for (const n of nodes) { if (cl.has(n.id) || used.has(n.id) || cl.size >= 5) continue;
        let c = 0; for (const m of cl) if (adj.get(n.id).has(m)) c++;
        if (c >= 2) { cl.add(n.id); grew = true; } } }
    cl.forEach(x => used.add(x));
    const arr = [...cl]; const ec = fe.filter(e => cl.has(e.source) && cl.has(e.target)).length;
    clusters.push({ nodes: arr, edgeCount: ec });
  }
  clusters.sort((x, y) => y.edgeCount - x.edgeCount);
  const idx = {}; nodes.forEach(n => idx[n.id] = { name: n.name, type: n.type, summary: n.summary });
  fs.writeFileSync(out, JSON.stringify({ scriptCompleted: true, entryPointCandidates: cands,
    fanInRanking: fanIn.slice(0, 20), fanOutRanking: fanOut.slice(0, 20), bfsTraversal: bfs,
    nonCodeFiles: nonCode, clusters: clusters.slice(0, 10),
    layers: { count: layers.length, list: layers.map(l => ({ id: l.id, name: l.name, description: l.description })) },
    nodeSummaryIndex: idx, totalNodes: nodes.length, totalEdges: edges.length, fileLevelEdges: fe.length }, null, 1));
  process.exit(0);
} catch (e) { console.error(e.stack || e); process.exit(1); }
