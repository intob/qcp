package main

// indexHTML is the whole static site: one file, no build step, no CDN, no
// fetch. runIndex substitutes the data in place of the INDEX_DATA marker,
// because a page opened from file:// cannot read a sibling JSON file — every
// browser treats that as a cross-origin request.
const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>qcp index</title>
<style>
:root {
  --bg: #14161a; --panel: #1b1e24; --panel2: #22262e; --line: #2c313a;
  --fg: #e6e8ec; --dim: #949aa6; --accent: #5aa9e6; --warn: #e6b45a; --ok: #6fcf97;
  --radius: 8px;
}
@media (prefers-color-scheme: light) {
  :root {
    --bg: #f6f7f9; --panel: #fff; --panel2: #eef0f4; --line: #dde0e6;
    --fg: #1a1d22; --dim: #666e7b; --accent: #1f6fb2; --warn: #a97516; --ok: #2f855a;
  }
}
* { box-sizing: border-box; }
html, body { height: 100%; }
body {
  margin: 0; background: var(--bg); color: var(--fg);
  font: 13px/1.5 ui-sans-serif, -apple-system, "SF Pro Text", Helvetica, Arial, sans-serif;
  display: flex; flex-direction: column;
}
header {
  display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  padding: 10px 14px; background: var(--panel); border-bottom: 1px solid var(--line);
}
header h1 { font-size: 14px; margin: 0; letter-spacing: .04em; font-weight: 600; }
header h1 span { color: var(--dim); font-weight: 400; margin-left: 6px; }
input, select, button {
  font: inherit; color: var(--fg); background: var(--panel2);
  border: 1px solid var(--line); border-radius: 6px; padding: 5px 8px;
}
input:focus, select:focus { outline: 2px solid var(--accent); outline-offset: -1px; }
button { cursor: pointer; }
button:hover { border-color: var(--accent); }
#q { min-width: 220px; flex: 1 1 220px; }
.stats { color: var(--dim); margin-left: auto; white-space: nowrap; }
main { display: flex; flex: 1; min-height: 0; }
aside {
  width: 260px; flex: none; overflow-y: auto; background: var(--panel);
  border-right: 1px solid var(--line); padding: 8px 0 24px;
}
.year { padding: 8px 14px 4px; color: var(--dim); font-size: 11px; letter-spacing: .1em; text-transform: uppercase; }
.mission {
  display: block; width: 100%; text-align: left; border: 0; background: none;
  border-radius: 0; padding: 5px 14px; color: var(--fg); line-height: 1.35;
}
.mission:hover { background: var(--panel2); }
.mission.sel { background: var(--accent); color: #fff; }
.mission.sel .meta, .mission.sel .num { color: rgba(255,255,255,.8); }
.mission .num { color: var(--dim); font-variant-numeric: tabular-nums; margin-right: 6px; }
.mission .meta { display: block; color: var(--dim); font-size: 11px; }
section { flex: 1; overflow-y: auto; padding: 14px; }
.crumb { display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
.crumb h2 { margin: 0; font-size: 16px; font-weight: 600; }
.crumb .sub { color: var(--dim); }
.badge {
  display: inline-block; padding: 1px 7px; border-radius: 99px; font-size: 11px;
  border: 1px solid var(--line); background: var(--panel2); color: var(--dim);
}
.badge.on { border-color: var(--ok); color: var(--ok); }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 12px; }
.card {
  background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
  overflow: hidden; cursor: pointer;
}
.card:hover { border-color: var(--accent); }
.thumb { position: relative; aspect-ratio: 16/9; background: #000 center/cover no-repeat; }
.thumb .sprite {
  position: absolute; inset: 0; opacity: 0; background-size: 1000% 1000%;
  background-repeat: no-repeat; transition: opacity .1s;
}
.card:hover .sprite { opacity: 1; }
.thumb .none { position: absolute; inset: 0; display: grid; place-items: center; color: var(--dim); font-size: 11px; }
.thumb .dur {
  position: absolute; right: 5px; bottom: 5px; background: rgba(0,0,0,.72); color: #fff;
  padding: 0 5px; border-radius: 4px; font-size: 11px; font-variant-numeric: tabular-nums;
}
.scrub { position: absolute; left: 0; bottom: 0; height: 2px; background: var(--accent); width: 0; }
.card .info { padding: 7px 9px; }
.card .name { font-weight: 500; word-break: break-all; }
.card .sub { color: var(--dim); font-size: 11px; margin-top: 2px; }
.empty { color: var(--dim); padding: 40px 0; text-align: center; }
dialog {
  border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel);
  color: var(--fg); padding: 0; width: min(1100px, 94vw); max-height: 92vh;
}
dialog::backdrop { background: rgba(0,0,0,.6); }
.modal-head { display: flex; align-items: baseline; gap: 10px; padding: 12px 14px; border-bottom: 1px solid var(--line); }
.modal-head h3 { margin: 0; font-size: 14px; word-break: break-all; }
.modal-head button { margin-left: auto; }
.modal-body { padding: 14px; overflow-y: auto; }
video { width: 100%; background: #000; border-radius: 6px; display: block; }
.offline { padding: 28px 14px; text-align: center; color: var(--dim); background: var(--panel2); border-radius: 6px; }
table.kv { width: 100%; border-collapse: collapse; margin-top: 12px; }
table.kv td { padding: 4px 0; vertical-align: top; border-top: 1px solid var(--line); }
table.kv td:first-child { color: var(--dim); width: 120px; white-space: nowrap; }
code {
  font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
  background: var(--panel2); border: 1px solid var(--line); border-radius: 5px;
  padding: 2px 6px; display: inline-block; word-break: break-all;
}
.copy { margin-left: 6px; padding: 1px 7px; font-size: 11px; }
</style>
</head>
<body>
<header>
  <h1>qcp<span id="gen"></span></h1>
  <input id="q" type="search" placeholder="Search missions and clips…" autocomplete="off">
  <select id="fyear"></select>
  <select id="fcard"></select>
  <select id="fcodec"></select>
  <select id="fgamma"></select>
  <select id="fdur"></select>
  <button id="clear">Clear</button>
  <span class="stats" id="stats"></span>
</header>
<main>
  <aside id="side"></aside>
  <section id="main"></section>
</main>

<dialog id="dlg">
  <div class="modal-head">
    <h3 id="dlg-title"></h3>
    <button id="dlg-close">Close</button>
  </div>
  <div class="modal-body" id="dlg-body"></div>
</dialog>

<script>
const DATA = /*INDEX_DATA*/null;
const $ = id => document.getElementById(id);
const q = $("q"), fyear = $("fyear"), fcard = $("fcard"), fcodec = $("fcodec"),
      fgamma = $("fgamma"), fdur = $("fdur"), clearBtn = $("clear"), stats = $("stats"),
      side = $("side"), main = $("main"), dlg = $("dlg"),
      dlgTitle = $("dlg-title"), dlgBody = $("dlg-body"), dlgClose = $("dlg-close"),
      gen = $("gen");

// ── model ───────────────────────────────────────────────────────────────────
const drives = new Map(DATA.drives.map(d => [d.name, d]));
const missions = [];
for (const y of DATA.years) for (const m of y.missions) {
  m.year = y.year;
  m.clips = m.clips || [];
  m.drives = m.drives || [];
  m.clips.forEach(c => { c.mission = m; });
  missions.push(m);
}
const allClips = missions.flatMap(m => m.clips);

const fmtDur = s => {
  if (!s) return "";
  s = Math.round(s);
  const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60;
  return (h ? h + ":" + String(m).padStart(2, "0") : m) + ":" + String(x).padStart(2, "0");
};
const fmtSize = b => {
  if (!b) return "";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return b.toFixed(i > 1 ? 1 : 0) + u[i];
};
const join = (...p) => p.filter(Boolean).join("/");
// Paths come from directory names on the drives, so every one of them is
// escaped per segment before it reaches a URL or a CSS url().
const encPath = p => p.split("/").map(encodeURIComponent).join("/");
const fileURL = p => "file://" + encPath(p);
const stem = rel => rel.replace(/\.[^./]+$/, "");
const stillURL = (c, kind) =>
  encPath("stills/" + c.mission.year + "/" + c.mission.slug + "/" + stem(c.rel) + "." + kind + ".jpg");
const browsePath = c => c.browse && c.mission.proxyDir ? join(c.mission.proxyDir, c.browse) : "";
const sourcePaths = c => c.mission.drives.map(n => {
  const d = drives.get(n);
  return d ? { drive: n, path: join(d.base, d.root, String(c.mission.year), c.mission.slug, c.rel) } : null;
}).filter(Boolean);

const DURATIONS = [
  ["", "Any length"],
  ["s", "Under 30s"],
  ["m", "30s – 2m"],
  ["l", "2m – 10m"],
  ["x", "Over 10m"],
];
const durBucket = d => d < 30 ? "s" : d < 120 ? "m" : d < 600 ? "l" : "x";

// ── filters ─────────────────────────────────────────────────────────────────
const state = { q: "", year: "", card: "", codec: "", gamma: "", dur: "", mission: null };

function fillSelect(el, label, values) {
  el.innerHTML = "";
  const first = new Option(label, "");
  el.append(first);
  for (const v of values) el.append(new Option(v, v));
}
const uniq = f => [...new Set(allClips.map(f).filter(Boolean))].sort();

fillSelect(fyear, "Any year", DATA.years.map(y => String(y.year)));
fillSelect(fcard, "Any card", uniq(c => c.card));
fillSelect(fcodec, "Any codec", uniq(c => c.codec));
fillSelect(fgamma, "Any gamma", uniq(c => c.gamma));
fdur.innerHTML = "";
for (const [v, l] of DURATIONS) fdur.append(new Option(l, v));

function clipMatches(c) {
  if (state.year && String(c.mission.year) !== state.year) return false;
  if (state.card && c.card !== state.card) return false;
  if (state.codec && c.codec !== state.codec) return false;
  if (state.gamma && c.gamma !== state.gamma) return false;
  if (state.dur && durBucket(c.dur || 0) !== state.dur) return false;
  if (state.q) {
    const q = state.q.toLowerCase();
    if (!(c.rel.toLowerCase().includes(q) || c.mission.name.toLowerCase().includes(q)
      || c.mission.slug.toLowerCase().includes(q))) return false;
  }
  return true;
}
const filtering = () => !!(state.q || state.year || state.card || state.codec || state.gamma || state.dur);

// A mission with no proxies yet still belongs in the list — the index is the
// map of the whole library, not only of what has been transcoded.
function missionMatches(m) {
  if (state.year && String(m.year) !== state.year) return false;
  const clipFilters = !!(state.card || state.codec || state.gamma || state.dur);
  if (clipFilters) return m.clips.some(clipMatches);
  if (state.q) {
    const q = state.q.toLowerCase();
    return (m.name + " " + m.slug).toLowerCase().includes(q) || m.clips.some(clipMatches);
  }
  return true;
}

// ── rendering ───────────────────────────────────────────────────────────────
function renderSide() {
  const frag = document.createDocumentFragment();
  let shown = 0;
  for (const y of DATA.years) {
    const list = y.missions.filter(missionMatches);
    if (!list.length) continue;
    const h = document.createElement("div");
    h.className = "year";
    h.textContent = y.year + "  ·  " + list.length;
    frag.append(h);
    for (const m of list) {
      shown++;
      const b = document.createElement("button");
      b.className = "mission" + (state.mission === m ? " sel" : "");
      b.innerHTML = '<span class="num">' + String(m.num).padStart(3, "0") + "</span>" +
        esc(m.name) + '<span class="meta">' + m.files + " files · " + fmtSize(m.size) +
        (m.clips.length ? "" : " · no proxies") + " · " + m.drives.join(", ") + "</span>";
      b.onclick = () => { state.mission = m; render(); };
      frag.append(b);
    }
  }
  if (!shown) {
    const d = document.createElement("div");
    d.className = "empty";
    d.textContent = "No missions match.";
    frag.append(d);
  }
  side.replaceChildren(frag);
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, m => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
}

function clipCard(c) {
  const el = document.createElement("div");
  el.className = "card";
  const poster = c.poster ? stillURL(c, "poster") : "";
  el.innerHTML =
    '<div class="thumb"' + (poster ? ' style="background-image:url(&quot;' + esc(poster) + '&quot;)"' : "") + ">" +
      (c.sprite ? '<div class="sprite" style="background-image:url(&quot;' + esc(stillURL(c, "sprite")) + '&quot;)"></div>' : "") +
      (poster ? "" : '<div class="none">no still</div>') +
      '<div class="scrub"></div>' +
      (c.dur ? '<div class="dur">' + fmtDur(c.dur) + "</div>" : "") +
    "</div>" +
    '<div class="info"><div class="name">' + esc(c.rel.split("/").pop()) + "</div>" +
    '<div class="sub">' + esc([c.codec, c.w ? c.w + "×" + c.h : "", c.fps ? c.fps.toFixed(2).replace(/\.?0+$/, "") + "p" : "", fmtSize(c.size)].filter(Boolean).join(" · ")) + "</div>" +
    (c.gamma ? '<div class="sub">' + esc(c.gamma) + (c.xf && c.xf !== "none" ? " → rec709" : "") + "</div>" : "") +
    "</div>";

  if (c.sprite) {
    const thumb = el.querySelector(".thumb");
    const sprite = el.querySelector(".sprite");
    const scrub = el.querySelector(".scrub");
    thumb.addEventListener("mousemove", e => {
      const r = thumb.getBoundingClientRect();
      const p = Math.min(0.999, Math.max(0, (e.clientX - r.left) / r.width));
      const f = Math.floor(p * 100);
      sprite.style.backgroundPosition = ((f % 10) / 9 * 100) + "% " + (Math.floor(f / 10) / 9 * 100) + "%";
      scrub.style.width = (p * 100) + "%";
    });
    thumb.addEventListener("mouseleave", () => { scrub.style.width = "0"; });
  }
  el.onclick = () => openClip(c);
  return el;
}

function render() {
  renderSide();
  const frag = document.createDocumentFragment();
  let clips, head;
  if (filtering() && !state.mission) {
    clips = allClips.filter(clipMatches);
    head = { title: "Search", sub: clips.length + " clip(s) across " + new Set(clips.map(c => c.mission)).size + " mission(s)", badges: [] };
  } else if (state.mission) {
    const m = state.mission;
    clips = m.clips.filter(clipMatches);
    head = {
      title: String(m.num).padStart(3, "0") + " " + m.name,
      sub: m.year + " · " + m.files + " files · " + fmtSize(m.size) +
        (m.clips.length ? " · " + m.clips.length + " clip(s) proxied" : " · no proxies generated"),
      badges: m.drives.map(d => ({ text: d, on: (m.verified || []).includes(d) })),
      pull: "qcp -pull " + m.num + " -year " + m.year,
    };
  } else {
    head = { title: "All missions", sub: missions.length + " mission(s) · " + allClips.length + " clip(s)", badges: [] };
    clips = [];
  }

  const crumb = document.createElement("div");
  crumb.className = "crumb";
  crumb.innerHTML = "<h2>" + esc(head.title) + '</h2><span class="sub">' + esc(head.sub) + "</span>" +
    (head.badges || []).map(b => '<span class="badge' + (b.on ? " on" : "") + '">' + esc(b.text) + "</span>").join("");
  if (head.pull) {
    const c = document.createElement("span");
    c.innerHTML = "<code>" + esc(head.pull) + "</code>";
    c.append(copyBtn(head.pull));
    crumb.append(c);
  }
  frag.append(crumb);

  if (clips.length) {
    const grid = document.createElement("div");
    grid.className = "grid";
    for (const c of clips) grid.append(clipCard(c));
    frag.append(grid);
  } else {
    const d = document.createElement("div");
    d.className = "empty";
    d.textContent = state.mission && !state.mission.clips.length
      ? "No proxies for this mission yet — run  qcp -proxy " + state.mission.num + " -year " + state.mission.year
      : filtering() ? "Nothing matches those filters." : "Pick a mission, or start typing to search.";
    frag.append(d);
  }
  main.replaceChildren(frag);
  main.scrollTop = 0;
  stats.textContent = missions.length + " missions · " + allClips.length + " clips · " + fmtSize(missions.reduce((a, m) => a + m.size, 0));
}

function copyBtn(text) {
  const b = document.createElement("button");
  b.className = "copy";
  b.textContent = "Copy";
  b.onclick = e => {
    e.stopPropagation();
    // navigator.clipboard is unavailable on file:// in most browsers.
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.append(ta);
    ta.select();
    try { document.execCommand("copy"); b.textContent = "Copied"; }
    catch (_) { b.textContent = "Select it"; }
    ta.remove();
    setTimeout(() => { b.textContent = "Copy"; }, 1200);
  };
  return b;
}

function openClip(c) {
  dlgTitle.textContent = c.mission.slug + " / " + c.rel;
  const body = document.createElement("div");

  const path = browsePath(c);
  if (path) {
    const v = document.createElement("video");
    v.controls = true;
    v.preload = "metadata";
    v.src = fileURL(path);
    v.onerror = () => v.replaceWith(offlineBox(path));
    body.append(v);
  } else {
    body.append(offlineBox("", "No browse proxy has been generated for this clip."));
  }

  const rows = [
    ["Duration", fmtDur(c.dur)],
    ["Resolution", c.w ? c.w + " × " + c.h : ""],
    ["Frame rate", c.fps ? c.fps.toFixed(3).replace(/\.?0+$/, "") + " fps" : ""],
    ["Codec", c.codec],
    ["Size", fmtSize(c.size)],
    ["Card", c.card],
    ["Capture gamma", [c.gamma, c.prim].filter(Boolean).join(" / ")],
    ["Transform", c.xf === "none" || !c.xf ? "none — passed through" : c.xf + " → Rec.709 (browse tier only)"],
  ];
  const t = document.createElement("table");
  t.className = "kv";
  for (const [k, v] of rows) {
    if (!v) continue;
    const tr = t.insertRow();
    tr.insertCell().textContent = k;
    tr.insertCell().textContent = v;
  }
  for (const { drive, path } of sourcePaths(c)) {
    const tr = t.insertRow();
    tr.insertCell().textContent = drive;
    const td = tr.insertCell();
    td.innerHTML = "<code>" + esc(path) + "</code>";
    td.append(copyBtn(path));
  }
  const tr = t.insertRow();
  tr.insertCell().textContent = "Retrieve";
  const td = tr.insertCell();
  const cmd = "qcp -pull " + c.mission.num + " -year " + c.mission.year;
  td.innerHTML = "<code>" + esc(cmd) + "</code>";
  td.append(copyBtn(cmd));
  body.append(t);

  dlgBody.replaceChildren(body);
  dlg.showModal();
}

function offlineBox(path, msg) {
  const d = document.createElement("div");
  d.className = "offline";
  d.textContent = msg || "Proxy not reachable — the drive is probably not mounted.";
  if (path) {
    const p = document.createElement("div");
    p.style.marginTop = "10px";
    p.innerHTML = "<code>" + esc(path) + "</code>";
    p.append(copyBtn(path));
    d.append(p);
  }
  return d;
}

// ── wiring ──────────────────────────────────────────────────────────────────
const bind = (el, key) => el.addEventListener("input", () => {
  state[key] = el.value;
  if (key !== "mission") state.mission = null;
  render();
});
bind(q, "q"); bind(fyear, "year"); bind(fcard, "card");
bind(fcodec, "codec"); bind(fgamma, "gamma"); bind(fdur, "dur");
clearBtn.onclick = () => {
  Object.assign(state, { q: "", year: "", card: "", codec: "", gamma: "", dur: "", mission: null });
  q.value = ""; fyear.value = ""; fcard.value = ""; fcodec.value = ""; fgamma.value = ""; fdur.value = "";
  render();
};
dlgClose.onclick = () => dlg.close();
dlg.addEventListener("close", () => dlgBody.replaceChildren());
document.addEventListener("keydown", e => {
  if (e.key === "/" && document.activeElement !== q) { e.preventDefault(); q.focus(); }
});
gen.textContent = "index · " + DATA.generated.slice(0, 10) + " · qcp " + DATA.version;
render();
</script>
</body>
</html>
`
