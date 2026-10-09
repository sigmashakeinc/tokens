const COLORS = {
  input: "#6c9eff",
  output: "#fc4d6f",
  cacheRead: "#4ade80",
  cacheWrite: "#fbbf24",
  cost: "#a78bfa",
};

const MODEL_PALETTE = [
  "#fc4d6f",
  "#6c9eff",
  "#4ade80",
  "#fbbf24",
  "#a78bfa",
  "#22d3ee",
  "#f472b6",
  "#facc15",
  "#94a3b8",
];

let dailyChart = null;
let modelChart = null;
let projectChart = null;
let throughputModelChart = null;
let throughputHarnessChart = null;
let throughputSparkChart = null;
let currentData = null;
let liveEventCount = 0;
let pulseTimer = null;
let eventSource = null;
let refreshTimer = null;
let refreshInFlight = false;
let refreshQueued = false;

// Client-side ring buffer of REAL /api/rate snapshots collected over time —
// resets on reload; never seeded with fake/backfilled trend points.
const THROUGHPUT_RING_MAX = 120;
const throughputRing = [];

// Filter state
const filters = {
  enabledModels: null,        // null = all on; Set<string> when user toggled
  enabledProjects: null,
  customFrom: null,
  customTo: null,
  search: "",
};

// Expansion state — date strings of expanded rows
const expandedDays = new Set();
// Cache of day-detail responses (date → DayDetail) so we don't refetch on every refresh.
const dayCache = new Map();
// Per-day inline charts — keep refs so we can destroy them on re-render.
const dayCharts = new Map();

function fmt(n) {
  if (n == null) return "—";
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(2) + "B";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "k";
  return Math.round(n).toString();
}

function fmtInt(n) {
  if (n == null) return "—";
  return Math.round(n).toLocaleString();
}

function fmtUsd(n) {
  if (n == null) return "—";
  if (n >= 1000) return "$" + n.toFixed(0);
  if (n >= 100) return "$" + n.toFixed(1);
  return "$" + n.toFixed(2);
}

function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  return d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function fmtClock(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function fmtRel(ts) {
  if (!ts) return "—";
  const diff = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (diff < 60) return diff + "s ago";
  if (diff < 3600) return Math.floor(diff / 60) + "m ago";
  if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
  return Math.floor(diff / 86400) + "d ago";
}

function fmtDuration(ms) {
  if (!ms || ms < 0) return "—";
  const s = Math.floor(ms / 1000);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h + "h " + m + "m";
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

async function fetchUsage() {
  const params = new URLSearchParams();
  if (filters.customFrom && filters.customTo) {
    params.set("from", filters.customFrom);
    params.set("to", filters.customTo);
  } else {
    const days = Number(document.getElementById("rangeSelect").value);
    if (days > 0) {
      const from = new Date(Date.now() - (days - 1) * 86400 * 1000);
      params.set("from", from.toISOString().slice(0, 10));
    }
  }
  if (filters.enabledModels && filters.enabledModels.size > 0) {
    params.set("models", [...filters.enabledModels].join(","));
  }
  if (filters.enabledProjects && filters.enabledProjects.size > 0) {
    params.set("projects", [...filters.enabledProjects].join(","));
  }
  const url = "/api/usage" + (params.size ? "?" + params.toString() : "");
  const res = await fetch(url);
  if (!res.ok) throw new Error("fetch failed: " + res.status);
  return res.json();
}

async function fetchRate() {
  const res = await fetch("/api/rate");
  if (!res.ok) throw new Error("rate fetch failed: " + res.status);
  return res.json();
}

async function fetchDay(date) {
  const res = await fetch("/api/day?date=" + encodeURIComponent(date));
  if (!res.ok) throw new Error("day fetch failed: " + res.status);
  return res.json();
}

async function fetchSession(sessionId) {
  const res = await fetch("/api/session?id=" + encodeURIComponent(sessionId));
  if (!res.ok) throw new Error("session fetch failed: " + res.status);
  return res.json();
}

async function fetchSavings() {
  const res = await fetch("/api/savings");
  if (!res.ok) throw new Error("savings fetch failed: " + res.status);
  return res.json();
}

async function fetchTokenomics() {
  const res = await fetch("/api/tokenomics");
  if (!res.ok) throw new Error("tokenomics fetch failed: " + res.status);
  return res.json();
}

function setTokenomics(data) {
  const body = document.getElementById("tokenomicsBody");
  if (!body) return;
  if (!data) {
    body.innerHTML = '<p class="muted hint">Tokenomics data unavailable.</p>';
    return;
  }

  const c = data.consumption || {};
  const allTime = c.allTime || {};
  const win = c.window || {};
  const wh = c.windowHours || 72;
  const eff = data.efficiency || {};
  const obs = eff.observedByTool || {};
  const findings = eff.findings || [];
  const capped = eff.capped ? ' <span style="color:var(--warn)">(capped)</span>' : "";

  // Consumption headline (SPENT)
  const consumptionHtml = `
    <div class="drill-card" style="margin-bottom:12px;">
      <h3>Spent (all-time)</h3>
      <div class="savings-kpis">
        <div class="kpi"><div class="kpi-label">Total tokens</div><div class="kpi-value">${fmt(allTime.totalTokens)}</div></div>
        <div class="kpi"><div class="kpi-label">Estimated cost</div><div class="kpi-value">${fmtUsd(allTime.cost)}</div></div>
        <div class="kpi"><div class="kpi-label">Last ${wh}h tokens</div><div class="kpi-value">${fmt(win.totalTokens)}</div><div class="kpi-sub">${fmtUsd(win.cost)}</div></div>
        <div class="kpi"><div class="kpi-label">Last ${wh}h messages</div><div class="kpi-value">${(win.messages || 0).toLocaleString()}</div></div>
      </div>
    </div>`;

  // Observed-per-tool table
  const toolRows = Object.entries(obs)
    .sort((a, b) => b[1].count - a[1].count)
    .map(([tool, ob]) =>
      `<tr><td>${escapeHtml(tool)}</td><td class="r">${ob.count.toLocaleString()}</td><td class="r">${fmt(ob.estBytes)}</td></tr>`,
    ).join("");

  const observedHtml = toolRows
    ? `<div class="drill-card" style="margin-bottom:12px;">
        <h3>Observed tool calls (last ${wh}h window · ${eff.filesScanned || 0} files${capped})</h3>
        <table class="table table-compact">
          <thead><tr><th>Tool / category</th><th class="r">Calls</th><th class="r">~Input bytes</th></tr></thead>
          <tbody>${toolRows}</tbody>
        </table>
       </div>`
    : `<div class="drill-card" style="margin-bottom:12px;"><p class="muted hint">No tool calls observed in the last ${wh}h window.</p></div>`;

  // Findings list (ADVISORY)
  const findingColor = { warn: "#fbbf24", info: "#6c9eff" };
  const findingsHtml = findings.length
    ? `<div class="drill-card">
        <h3>Advisory findings <span class="muted" style="font-size:11px;font-weight:400;">(heuristic — review before acting)</span></h3>
        <ul style="margin:0;padding:0 0 0 18px;">
          ${findings.map(f => {
            const color = findingColor[f.severity] || "#8c93a8";
            return `<li style="margin:4px 0;color:${color};">
              <strong>[${escapeHtml(f.kind)}]</strong>
              <span style="color:var(--text-dim);margin-left:4px;">${escapeHtml(f.detail)}</span>
              <span style="color:var(--text-dim);font-size:11px;margin-left:6px;">· session ${escapeHtml(f.sessionId.slice(0, 8))}…</span>
            </li>`;
          }).join("")}
        </ul>
       </div>`
    : `<div class="drill-card"><p class="muted hint">No advisory findings in the current window.</p></div>`;

  // Ponytail roll-up (decision-ladder signals — advisory)
  const pt = eff.ponytail || { newFiles: 0, depInstalls: 0 };
  const ponytailHtml = `<div class="drill-card" style="margin-top:8px;font-size:12px;color:var(--text-dim);">
    <strong style="color:var(--text);">Ponytail</strong>
    <span style="margin-left:8px;">${pt.newFiles} new source file${pt.newFiles === 1 ? "" : "s"}</span>
    <span style="margin-left:12px;color:${pt.depInstalls > 0 ? "#fbbf24" : "inherit"};">${pt.depInstalls} dep install${pt.depInstalls === 1 ? "" : "s"}</span>
    <span class="muted" style="margin-left:8px;font-size:11px;">advisory · decision-ladder signals</span>
  </div>`;

  body.innerHTML = consumptionHtml + observedHtml + findingsHtml + ponytailHtml;
}

function setSavings(data) {
  const body = document.getElementById("savingsBody");
  if (!body) return;
  if (!data || !data.present) {
    body.innerHTML =
      '<p class="muted hint">No reductions recorded yet — run <code>stk</code> or <code>headroom</code>.</p>';
    return;
  }

  function tileHtml(label, agg) {
    return `<div class="kpi">
      <div class="kpi-label">${escapeHtml(label)}</div>
      <div class="kpi-value">${fmt(agg.savedTokensEst)}<span class="kpi-est">~est tok</span></div>
      <div class="kpi-sub">${fmt(agg.savedBytes)} bytes saved \xb7 ${agg.savedPct.toFixed(1)}% \xb7 ${agg.count.toLocaleString()} reductions</div>
    </div>`;
  }

  const s = data.byTool.stk;
  const h = data.byTool.headroom;
  const c = data.combined;
  body.innerHTML = `
    <div class="savings-kpis">
      ${tileHtml("STK", s)}
      ${tileHtml("headroom", h)}
      ${tileHtml("Combined", c)}
      <div class="kpi">
        <div class="kpi-label">Raw bytes</div>
        <div class="kpi-value">${fmt(c.origBytes)}</div>
        <div class="kpi-sub">${fmt(c.compBytes)} after reduction \xb7 ${fmt(c.savedBytes)} net saved</div>
      </div>
    </div>
    <p class="muted hint" style="margin-top:8px;">Token estimate (\xf7\xa04) is approximate — byte counts are exact.</p>
  `;
}

function setKpis(data) {
  const t = data.totals;
  document.getElementById("kpiTotal").textContent = fmt(t.totalTokens);
  document.getElementById("kpiTotalSub").textContent =
    `in ${fmt(t.inputTokens)} · out ${fmt(t.outputTokens)} · cache ${fmt(t.cacheReadTokens + t.cacheWrite5mTokens + t.cacheWrite1hTokens)}`;
  document.getElementById("kpiCost").textContent = fmtUsd(t.cost);
  document.getElementById("kpiCostSub").textContent =
    data.range.from && data.range.to ? `${data.range.from} → ${data.range.to}` : "no data";

  const today = new Date().toISOString().slice(0, 10);
  const todayBucket = data.daily.find((d) => d.date === today);
  document.getElementById("kpiToday").textContent = todayBucket ? fmt(todayBucket.totalTokens) : "—";
  document.getElementById("kpiTodaySub").textContent = todayBucket
    ? fmtUsd(todayBucket.cost) + ` · ${todayBucket.messages.toLocaleString()} msgs`
    : "no usage yet";

  const days = data.daily.length || 1;
  document.getElementById("kpiAvg").textContent = fmt(t.totalTokens / days);
  document.getElementById("kpiAvgSub").textContent = `${fmtUsd(t.cost / days)}/day over ${days} day${days === 1 ? "" : "s"}`;

  document.getElementById("kpiMsgs").textContent = t.messages.toLocaleString();
  document.getElementById("kpiMsgsSub").textContent =
    `${fmt(t.messages ? t.totalTokens / t.messages : 0)} tok/msg avg`;

  document.getElementById("kpiFiles").textContent = data.files.toLocaleString();
  document.getElementById("kpiFilesSub").textContent = `${data.byProject.length} projects`;
}

function drawDaily(data) {
  const showTokens = document.getElementById("toggleTokens").checked;
  const showCost = document.getElementById("toggleCost").checked;
  const labels = data.daily.map((d) => d.date);
  const datasets = [];

  if (showTokens) {
    datasets.push(
      { label: "Input", data: data.daily.map((d) => d.inputTokens), backgroundColor: COLORS.input, stack: "tok", borderWidth: 0 },
      { label: "Cache read", data: data.daily.map((d) => d.cacheReadTokens), backgroundColor: COLORS.cacheRead, stack: "tok", borderWidth: 0 },
      { label: "Cache write", data: data.daily.map((d) => d.cacheWrite5mTokens + d.cacheWrite1hTokens), backgroundColor: COLORS.cacheWrite, stack: "tok", borderWidth: 0 },
      { label: "Output", data: data.daily.map((d) => d.outputTokens), backgroundColor: COLORS.output, stack: "tok", borderWidth: 0 },
    );
  }
  if (showCost) {
    datasets.push({
      label: "Cost ($)",
      data: data.daily.map((d) => d.cost),
      type: "line",
      borderColor: COLORS.cost,
      backgroundColor: "transparent",
      borderWidth: 2,
      yAxisID: "cost",
      tension: 0.25,
      pointRadius: 2,
    });
  }

  const ctx = document.getElementById("dailyChart").getContext("2d");
  if (dailyChart) dailyChart.destroy();
  dailyChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      onClick: (_evt, items) => {
        if (items.length === 0) return;
        const label = labels[items[0].index];
        if (label) toggleDayRow(label, true);
      },
      plugins: {
        legend: { labels: { color: "#e6e9f2", boxWidth: 12 } },
        tooltip: {
          callbacks: {
            label: (item) =>
              item.dataset.yAxisID === "cost"
                ? `${item.dataset.label}: ${fmtUsd(item.parsed.y)}`
                : `${item.dataset.label}: ${fmt(item.parsed.y)}`,
          },
        },
      },
      scales: {
        x: { stacked: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", maxRotation: 0, autoSkip: true } },
        y: {
          stacked: true,
          beginAtZero: true,
          grid: { color: "#232a3a" },
          ticks: { color: "#8c93a8", callback: (v) => fmt(v) },
        },
        cost: {
          position: "right",
          beginAtZero: true,
          grid: { drawOnChartArea: false },
          ticks: { color: "#a78bfa", callback: (v) => fmtUsd(v) },
          display: showCost,
        },
      },
    },
  });
}

function dailyRowHtml(d, isToday) {
  const avg = d.messages ? d.totalTokens / d.messages : 0;
  const cls = isToday ? "table-row-today" : "";
  const isExpanded = expandedDays.has(d.date);
  const toggle = isExpanded ? "▾" : "▸";
  return `
    <tr class="day-row ${cls} ${isExpanded ? "expanded" : ""}" data-date="${d.date}">
      <td><span class="row-toggle">${toggle}</span></td>
      <td>${d.date}${isToday ? " · today" : ""}</td>
      <td class="r">${d.messages.toLocaleString()}</td>
      <td class="r">${fmt(d.inputTokens)}</td>
      <td class="r">${fmt(d.outputTokens)}</td>
      <td class="r">${fmt(d.cacheReadTokens)}</td>
      <td class="r">${fmt(d.cacheWrite5mTokens + d.cacheWrite1hTokens)}</td>
      <td class="r">${fmt(d.totalTokens)}</td>
      <td class="r">${fmtUsd(d.cost)}</td>
      <td class="r">${fmt(avg)}</td>
    </tr>
  `;
}

function drawDailyTable(data) {
  const tbody = document.querySelector("#dailyTable tbody");
  for (const chart of dayCharts.values()) chart.destroy();
  dayCharts.clear();
  tbody.innerHTML = "";
  const today = new Date().toISOString().slice(0, 10);
  const reversed = [...data.daily].reverse();
  for (const d of reversed) {
    tbody.insertAdjacentHTML("beforeend", dailyRowHtml(d, d.date === today));
    if (expandedDays.has(d.date)) {
      tbody.insertAdjacentHTML("beforeend", dayDetailRowSkeleton(d.date));
      renderDayDetail(d.date);
    }
  }
  applySearch();
  document.getElementById("dailyDetailRange").textContent =
    data.range.from && data.range.to ? `${data.range.from} → ${data.range.to}` : "";
}

function dayDetailRowSkeleton(date) {
  return `
    <tr class="expanded-row" data-detail-for="${date}">
      <td colspan="10">
        <div class="expanded-inner" id="dayDetail-${date}">
          <div class="muted">Loading ${date}…</div>
        </div>
      </td>
    </tr>
  `;
}

async function renderDayDetail(date) {
  const host = document.getElementById("dayDetail-" + date);
  if (!host) return;
  let detail = dayCache.get(date);
  if (!detail) {
    try {
      detail = await fetchDay(date);
      dayCache.set(date, detail);
    } catch (err) {
      host.innerHTML = `<div class="muted">Error: ${escapeHtml(err.message)}</div>`;
      return;
    }
  }

  const t = detail.totals;
  const modelChips = detail.byModel
    .map(
      (m) =>
        `<span class="chip"><span>${escapeHtml(m.model)}</span><span class="chip-count">${fmt(m.totalTokens)} · ${fmtUsd(m.cost)}</span></span>`,
    )
    .join(" ");

  const sessionsRows = detail.sessions
    .slice(0, 10)
    .map(
      (s) => `
      <tr data-session="${escapeHtml(s.sessionId)}">
        <td><span class="row-toggle">▸</span></td>
        <td>${fmtTime(s.firstTs)}<div class="session-id">${escapeHtml(s.sessionId.slice(0, 12))}…</div></td>
        <td>${s.models.map((m) => `<span class="model-tag">${escapeHtml(m)}</span>`).join("")}</td>
        <td class="r">${fmt(s.totalTokens)}</td>
        <td class="r">${fmtUsd(s.cost)}</td>
        <td class="r">${s.messages.toLocaleString()}</td>
      </tr>
    `,
    )
    .join("");

  const topRows = detail.topMessages
    .map(
      (m) => `
      <tr data-session="${escapeHtml(m.sessionId)}">
        <td>${fmtClock(m.ts)}</td>
        <td><span class="model-tag">${escapeHtml(m.model)}</span></td>
        <td class="r">${fmt(m.inputTokens)}</td>
        <td class="r">${fmt(m.outputTokens)}</td>
        <td class="r">${fmt(m.cacheReadTokens)}</td>
        <td class="r">${fmt(m.cacheWriteTokens)}</td>
        <td class="r">${fmt(m.totalTokens)}</td>
        <td class="r">${fmtUsd(m.cost)}</td>
      </tr>
    `,
    )
    .join("");

  host.innerHTML = `
    <div class="drawer-meta">
      <div class="kpi"><div class="kpi-label">Messages</div><div class="kpi-value">${t.messages.toLocaleString()}</div></div>
      <div class="kpi"><div class="kpi-label">Total tokens</div><div class="kpi-value">${fmt(t.totalTokens)}</div></div>
      <div class="kpi"><div class="kpi-label">Cost</div><div class="kpi-value">${fmtUsd(t.cost)}</div></div>
      <div class="kpi"><div class="kpi-label">Sessions</div><div class="kpi-value">${detail.sessions.length}</div></div>
    </div>

    <div class="drill-grid">
      <div class="drill-card">
        <h3>Hourly breakdown (UTC)</h3>
        <div class="drill-chart-wrap"><canvas id="hourly-${date}"></canvas></div>
      </div>
      <div class="drill-card">
        <h3>Models used</h3>
        <div class="chip-row">${modelChips || '<span class="muted">none</span>'}</div>
      </div>
    </div>

    <div class="drill-card" style="margin-top: 12px;">
      <h3>Top sessions this day</h3>
      <table class="table table-compact table-clickable" data-day-session-table>
        <thead><tr><th></th><th>Started</th><th>Models</th><th class="r">Tokens</th><th class="r">Cost</th><th class="r">Msgs</th></tr></thead>
        <tbody>${sessionsRows || '<tr><td colspan="6" class="muted">no sessions</td></tr>'}</tbody>
      </table>
    </div>

    <div class="drill-card" style="margin-top: 12px;">
      <h3>Top messages by total tokens</h3>
      <table class="table table-compact">
        <thead><tr><th>Time</th><th>Model</th><th class="r">Input</th><th class="r">Output</th><th class="r">Cache R</th><th class="r">Cache W</th><th class="r">Total</th><th class="r">Cost</th></tr></thead>
        <tbody>${topRows || '<tr><td colspan="8" class="muted">no messages</td></tr>'}</tbody>
      </table>
    </div>
  `;

  // Hourly chart
  const canvas = document.getElementById("hourly-" + date);
  if (canvas) {
    const labels = detail.hourly.map((h) => String(h.hour).padStart(2, "0") + "h");
    const chart = new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: "Input", data: detail.hourly.map((h) => h.inputTokens), backgroundColor: COLORS.input, stack: "tok", borderWidth: 0 },
          { label: "Cache read", data: detail.hourly.map((h) => h.cacheReadTokens), backgroundColor: COLORS.cacheRead, stack: "tok", borderWidth: 0 },
          { label: "Cache write", data: detail.hourly.map((h) => h.cacheWrite5mTokens + h.cacheWrite1hTokens), backgroundColor: COLORS.cacheWrite, stack: "tok", borderWidth: 0 },
          { label: "Output", data: detail.hourly.map((h) => h.outputTokens), backgroundColor: COLORS.output, stack: "tok", borderWidth: 0 },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: { legend: { labels: { color: "#e6e9f2", boxWidth: 10, font: { size: 11 } } } },
        scales: {
          x: { stacked: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", font: { size: 10 } } },
          y: { stacked: true, beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmt(v), font: { size: 10 } } },
        },
      },
    });
    dayCharts.set(date, chart);
  }
}

function toggleDayRow(date, openOnly = false) {
  if (expandedDays.has(date)) {
    if (openOnly) return;
    expandedDays.delete(date);
  } else {
    expandedDays.add(date);
  }
  if (currentData) drawDailyTable(currentData);
}

function drawModelChart(data) {
  const items = data.byModel.slice(0, 8);
  const labels = items.map((m) => m.model);
  const tokens = items.map((m) => m.totalTokens);
  const colors = labels.map((_, i) => MODEL_PALETTE[i % MODEL_PALETTE.length]);

  const ctx = document.getElementById("modelChart").getContext("2d");
  if (modelChart) modelChart.destroy();
  modelChart = new Chart(ctx, {
    type: "doughnut",
    data: { labels, datasets: [{ data: tokens, backgroundColor: colors, borderColor: "#0b0d12", borderWidth: 2 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { position: "right", labels: { color: "#e6e9f2", boxWidth: 12, font: { size: 11 } } },
        tooltip: { callbacks: { label: (item) => `${item.label}: ${fmt(item.parsed)}` } },
      },
    },
  });

  const tbody = document.querySelector("#modelTable tbody");
  tbody.innerHTML = "";
  for (const m of data.byModel) {
    const tr = document.createElement("tr");
    const isFallback = data.pricing?.[m.model]?.source === "fallback";
    const modelCell = isFallback
      ? `<span title="Unknown model — priced at the Sonnet-tier fallback rate, not this model's actual price">${escapeHtml(m.model)} <span class="muted">(est.)</span></span>`
      : escapeHtml(m.model);
    tr.innerHTML = `<td>${modelCell}</td><td class="r">${fmt(m.totalTokens)}</td><td class="r">${fmtUsd(m.cost)}</td><td class="r">${m.messages.toLocaleString()}</td><td class="r">${fmt(m.tokensPerSecond)}</td>`;
    tbody.appendChild(tr);
  }
}

function drawHarness(data) {
  const tbody = document.querySelector("#harnessTable tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  for (const h of data.byHarness || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${escapeHtml(h.harness)}</td><td class="r">${fmt(h.tokensPerSecond)}</td><td class="r">${fmt(h.totalTokens)}</td><td class="r">${fmtUsd(h.cost)}</td><td class="r">${h.messages.toLocaleString()}</td>`;
    tbody.appendChild(tr);
  }
}

function drawProjectChart(data) {
  const items = data.byProject.slice(0, 10);
  const ctx = document.getElementById("projectChart").getContext("2d");
  if (projectChart) projectChart.destroy();
  projectChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: items.map((p) => p.project),
      datasets: [{ label: "Tokens", data: items.map((p) => p.totalTokens), backgroundColor: COLORS.input }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item) => fmt(item.parsed.x) } } },
      scales: {
        x: { beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmt(v) } },
        y: { grid: { color: "#232a3a" }, ticks: { color: "#e6e9f2" } },
      },
    },
  });

  const tbody = document.querySelector("#projectTable tbody");
  tbody.innerHTML = "";
  for (const p of data.byProject.slice(0, 30)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td title="${escapeHtml(p.cwd)}">${escapeHtml(p.project)}</td><td class="r">${fmt(p.totalTokens)}</td><td class="r">${fmtUsd(p.cost)}</td><td class="r">${p.sessions}</td><td class="r">${fmtRel(p.lastTs)}</td>`;
    tbody.appendChild(tr);
  }
}

function drawSessions(data) {
  const tbody = document.querySelector("#sessionTable tbody");
  tbody.innerHTML = "";
  for (const s of data.topSessions) {
    const models = s.models.map((m) => `<span class="model-tag">${escapeHtml(m)}</span>`).join("");
    const tr = document.createElement("tr");
    tr.dataset.session = s.sessionId;
    tr.innerHTML = `
      <td><span class="row-toggle">▸</span></td>
      <td><div>${fmtTime(s.firstTs)}</div><div class="session-id">${escapeHtml(s.sessionId.slice(0, 12))}…</div></td>
      <td>${escapeHtml(s.project)}</td>
      <td>${models}</td>
      <td class="r">${fmt(s.totalTokens)}</td>
      <td class="r">${fmtUsd(s.cost)}</td>
      <td class="r">${s.messages.toLocaleString()}</td>
    `;
    tbody.appendChild(tr);
  }
  document.getElementById("sessionCountHint").textContent =
    `${data.topSessions.length} shown · ranked by cost`;
  applySearch();
}

function drawRate(rate) {
  for (const node of document.querySelectorAll(".rate")) {
    const win = Number(node.dataset.window);
    const b = rate.buckets.find((x) => x.windowSec === win);
    if (!b) continue;
    const tokensEl = node.querySelector("[data-rate-tokens]");
    const subEl = node.querySelector("[data-rate-sub]");
    tokensEl.textContent = fmt(b.tokens) + " tok";
    subEl.textContent =
      `${fmtUsd(b.cost)} · ${b.messages} msg · ${b.activeSessions} active · ${fmtInt(b.tokensPerMinute)}/min`;
  }
  document.getElementById("rateUpdated").textContent =
    "updated " + new Date(rate.now).toLocaleTimeString();
}

// Lightweight live-tick update of just the hero number, driven by the SSE
// `rate` field (the 60s window). Full bar/sparkline redraw stays on the
// debounced /api/rate poll in drawThroughput to avoid Chart.js
// destroy+recreate thrash on every SSE tick.
function updateThroughputLive(rateBucket) {
  if (!rateBucket) return;
  const el = document.getElementById("throughputGlobal");
  if (el) el.textContent = fmt(rateBucket.tokensPerSecond) + " tok/s";
}

function drawThroughput(rate) {
  const b60 = rate.buckets.find((x) => x.windowSec === 60);
  if (!b60) return;

  document.getElementById("throughputGlobal").textContent = fmt(b60.tokensPerSecond) + " tok/s";
  document.getElementById("throughputGlobalSub").textContent =
    `${b60.messages.toLocaleString()} msgs \xb7 ${b60.activeSessions} active sessions \xb7 last 60s`;

  // Ring buffer of REAL /api/rate snapshots — never seeded/backfilled.
  throughputRing.push({ ts: rate.now, tokensPerSecond: b60.tokensPerSecond });
  while (throughputRing.length > THROUGHPUT_RING_MAX) throughputRing.shift();

  const modelCtx = document.getElementById("throughputModelChart").getContext("2d");
  if (throughputModelChart) throughputModelChart.destroy();
  throughputModelChart = new Chart(modelCtx, {
    type: "bar",
    data: {
      labels: b60.byModel.map((m) => m.model),
      datasets: [{ label: "tok/s", data: b60.byModel.map((m) => m.tokensPerSecond), backgroundColor: COLORS.input }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item) => fmt(item.parsed.x) + " tok/s" } } },
      scales: {
        x: { beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmt(v) } },
        y: { grid: { color: "#232a3a" }, ticks: { color: "#e6e9f2", font: { size: 10 } } },
      },
    },
  });

  const harnessCtx = document.getElementById("throughputHarnessChart").getContext("2d");
  if (throughputHarnessChart) throughputHarnessChart.destroy();
  throughputHarnessChart = new Chart(harnessCtx, {
    type: "bar",
    data: {
      labels: b60.byHarness.map((h) => h.source),
      datasets: [{ label: "tok/s", data: b60.byHarness.map((h) => h.tokensPerSecond), backgroundColor: COLORS.output }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item) => fmt(item.parsed.x) + " tok/s" } } },
      scales: {
        x: { beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmt(v) } },
        y: { grid: { color: "#232a3a" }, ticks: { color: "#e6e9f2" } },
      },
    },
  });

  const sparkCtx = document.getElementById("throughputSparkChart").getContext("2d");
  if (throughputSparkChart) throughputSparkChart.destroy();
  throughputSparkChart = new Chart(sparkCtx, {
    type: "line",
    data: {
      labels: throughputRing.map((p) => fmtClock(p.ts)),
      datasets: [
        {
          label: "tok/s",
          data: throughputRing.map((p) => p.tokensPerSecond),
          borderColor: COLORS.cost,
          backgroundColor: "rgba(167, 139, 250, 0.15)",
          fill: true,
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item) => fmt(item.parsed.y) + " tok/s" } } },
      scales: {
        x: { display: false },
        y: { beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmt(v), font: { size: 10 } } },
      },
    },
  });
}

function drawFilterChips(data) {
  // Model chips — always sourced from current data so they reflect what's
  // present in the active range. If filters.enabledModels is null, all are
  // implicitly active.
  const modelHost = document.getElementById("modelChips");
  modelHost.innerHTML = "";
  for (const m of data.byModel) {
    const chip = document.createElement("span");
    const active = !filters.enabledModels || filters.enabledModels.has(m.model);
    chip.className = "chip" + (active ? " active" : "");
    chip.dataset.model = m.model;
    chip.innerHTML = `<span>${escapeHtml(m.model)}</span><span class="chip-count">${fmt(m.totalTokens)}</span>`;
    chip.addEventListener("click", () => {
      if (!filters.enabledModels) filters.enabledModels = new Set(data.byModel.map((x) => x.model));
      if (filters.enabledModels.has(m.model)) filters.enabledModels.delete(m.model);
      else filters.enabledModels.add(m.model);
      // All-on equivalent → drop the explicit filter.
      if (filters.enabledModels.size === data.byModel.length) filters.enabledModels = null;
      scheduleRefresh(0);
    });
    modelHost.appendChild(chip);
  }

  // Project chips — only shown when more than one.
  const projectBlock = document.getElementById("projectFilterBlock");
  const projectHost = document.getElementById("projectChips");
  if (data.byProject.length > 1) {
    projectBlock.hidden = false;
    projectHost.innerHTML = "";
    for (const p of data.byProject) {
      const chip = document.createElement("span");
      const active = !filters.enabledProjects || filters.enabledProjects.has(p.project);
      chip.className = "chip" + (active ? " active" : "");
      chip.dataset.project = p.project;
      chip.innerHTML = `<span>${escapeHtml(p.project)}</span><span class="chip-count">${fmt(p.totalTokens)}</span>`;
      chip.addEventListener("click", () => {
        if (!filters.enabledProjects)
          filters.enabledProjects = new Set(data.byProject.map((x) => x.project));
        if (filters.enabledProjects.has(p.project)) filters.enabledProjects.delete(p.project);
        else filters.enabledProjects.add(p.project);
        if (filters.enabledProjects.size === data.byProject.length)
          filters.enabledProjects = null;
        scheduleRefresh(0);
      });
      projectHost.appendChild(chip);
    }
  } else {
    projectBlock.hidden = true;
  }
}

function setStatus(text) {
  document.getElementById("footerStatus").textContent = text;
}

function setLive(state, label) {
  const pill = document.getElementById("livePill");
  pill.classList.remove("connected", "pulse", "error");
  if (state === "connected") pill.classList.add("connected");
  else if (state === "pulse") pill.classList.add("pulse");
  else if (state === "error") pill.classList.add("error");
  if (label) document.getElementById("liveLabel").textContent = label;
}

function flashKpi(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.add("flash");
  setTimeout(() => el.classList.remove("flash"), 800);
}

function appendLiveFeed(update) {
  const feed = document.getElementById("liveFeed");
  const empty = feed.querySelector(".live-empty");
  if (empty) empty.remove();
  for (const ev of update.recent) {
    const li = document.createElement("li");
    li.classList.add("lf-new");
    li.innerHTML = `
      <span class="lf-time">${fmtClock(ev.ts)}</span>
      <span class="lf-proj">${escapeHtml(ev.project)} · <span class="model-tag">${escapeHtml(ev.model)}</span></span>
      <span class="lf-tok">${fmt(ev.totalTokens)}</span>
      <span class="lf-cost">${fmtUsd(ev.cost)}</span>
    `;
    feed.insertBefore(li, feed.firstChild);
  }
  while (feed.children.length > 100) feed.removeChild(feed.lastChild);
  liveEventCount += update.newEvents;
  document.getElementById("liveCount").textContent = liveEventCount.toLocaleString() + " events since open";
}

function pulseLive() {
  setLive("pulse", "live · activity");
  if (pulseTimer) clearTimeout(pulseTimer);
  pulseTimer = setTimeout(() => setLive("connected", "live"), 1500);
}

function scheduleRefresh(delayMs = 400) {
  if (refreshTimer) return;
  refreshTimer = setTimeout(() => {
    refreshTimer = null;
    runRefresh();
  }, delayMs);
}

function applySearch() {
  const q = filters.search.toLowerCase().trim();
  for (const tr of document.querySelectorAll("#dailyTable tbody tr.day-row, #sessionTable tbody tr")) {
    if (!q) {
      tr.style.display = "";
      continue;
    }
    tr.style.display = tr.textContent.toLowerCase().includes(q) ? "" : "none";
  }
}

async function runRefresh() {
  if (refreshInFlight) {
    refreshQueued = true;
    return;
  }
  refreshInFlight = true;
  try {
    // fetchSavings and fetchTokenomics are best-effort: failures must not break
    // the main dashboard. They share the same /api/tokenomics data path.
    const [data, rate, savings, tokenomics] = await Promise.all([
      fetchUsage(),
      fetchRate(),
      fetchSavings().catch(() => null),
      fetchTokenomics().catch(() => null),
    ]);
    currentData = data;
    // Invalidate cached day-details since events may have shifted; expanded
    // rows will refetch on next render.
    dayCache.clear();
    setKpis(data);
    drawDaily(data);
    drawDailyTable(data);
    drawModelChart(data);
    drawHarness(data);
    drawProjectChart(data);
    drawSessions(data);
    drawRate(rate);
    drawThroughput(rate);
    setSavings(savings);
    setTokenomics(tokenomics);
    drawFilterChips(data);
    flashKpi("kpiCardTotal");
    flashKpi("kpiCardCost");
    flashKpi("kpiCardToday");
    const scanned = new Date(data.scannedAt).toLocaleTimeString();
    setStatus(`Scanned ${data.files} files · ${data.totals.messages.toLocaleString()} messages · last update ${scanned}`);
  } catch (err) {
    setStatus("Error: " + err.message);
  } finally {
    refreshInFlight = false;
    if (refreshQueued) {
      refreshQueued = false;
      scheduleRefresh(50);
    }
  }
}

function connectSse() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource("/api/live");
  eventSource.addEventListener("hello", () => setLive("connected", "live"));
  eventSource.addEventListener("ping", () => {
    if (!document.getElementById("livePill").classList.contains("pulse")) {
      setLive("connected", "live");
    }
  });
  eventSource.addEventListener("update", (e) => {
    try {
      const update = JSON.parse(e.data);
      pulseLive();
      appendLiveFeed(update);
      if (update.rate) updateThroughputLive(update.rate);
      scheduleRefresh(400);
    } catch (err) {
      console.error("bad SSE payload", err);
    }
  });
  eventSource.onerror = () => {
    setLive("error", "reconnecting…");
    eventSource.close();
    eventSource = null;
    setTimeout(connectSse, 3000);
  };
}

// === Session drawer ===
function ensureDrawer() {
  if (document.getElementById("sessionDrawer")) return;
  const backdrop = document.createElement("div");
  backdrop.className = "drawer-backdrop";
  backdrop.id = "sessionBackdrop";
  backdrop.addEventListener("click", closeDrawer);
  const drawer = document.createElement("aside");
  drawer.className = "drawer";
  drawer.id = "sessionDrawer";
  drawer.innerHTML = `
    <div class="drawer-header">
      <h2 id="drawerTitle">Session</h2>
      <button type="button" class="drawer-close" id="drawerCloseBtn">Close</button>
    </div>
    <div class="drawer-body" id="drawerBody"></div>
  `;
  document.body.appendChild(backdrop);
  document.body.appendChild(drawer);
  document.getElementById("drawerCloseBtn").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeDrawer();
  });
}

function openDrawer() {
  ensureDrawer();
  document.getElementById("sessionBackdrop").classList.add("open");
  document.getElementById("sessionDrawer").classList.add("open");
}

function closeDrawer() {
  const drawer = document.getElementById("sessionDrawer");
  const backdrop = document.getElementById("sessionBackdrop");
  if (drawer) drawer.classList.remove("open");
  if (backdrop) backdrop.classList.remove("open");
}

async function showSession(sessionId) {
  ensureDrawer();
  openDrawer();
  document.getElementById("drawerTitle").textContent = "Session " + sessionId.slice(0, 12) + "…";
  const body = document.getElementById("drawerBody");
  body.innerHTML = '<div class="muted">Loading…</div>';
  let detail;
  try {
    detail = await fetchSession(sessionId);
  } catch (err) {
    body.innerHTML = `<div class="muted">Error: ${escapeHtml(err.message)}</div>`;
    return;
  }
  if (!detail.messages || detail.messages.length === 0) {
    body.innerHTML = '<div class="muted">No messages found for this session.</div>';
    return;
  }
  const t = detail.totals;
  const modelTags = detail.models
    .map((m) => `<span class="model-tag">${escapeHtml(m)}</span>`)
    .join(" ");
  const rows = detail.messages
    .map(
      (m, i) => `
      <tr>
        <td class="r">${i + 1}</td>
        <td>${fmtTime(m.ts)}</td>
        <td><span class="model-tag">${escapeHtml(m.model)}</span></td>
        <td class="r">${fmt(m.inputTokens)}</td>
        <td class="r">${fmt(m.outputTokens)}</td>
        <td class="r">${fmt(m.cacheReadTokens)}</td>
        <td class="r">${fmt(m.cacheWriteTokens)}</td>
        <td class="r">${fmt(m.totalTokens)}</td>
        <td class="r">${fmtUsd(m.cost)}</td>
      </tr>
    `,
    )
    .join("");
  body.innerHTML = `
    <div class="drawer-meta">
      <div class="kpi"><div class="kpi-label">Project</div><div class="kpi-value">${escapeHtml(detail.project)}</div><div class="kpi-sub" title="${escapeHtml(detail.cwd)}">${escapeHtml(detail.cwd)}</div></div>
      <div class="kpi"><div class="kpi-label">Duration</div><div class="kpi-value">${fmtDuration(detail.durationMs)}</div><div class="kpi-sub">${fmtTime(detail.firstTs)} → ${fmtTime(detail.lastTs)}</div></div>
      <div class="kpi"><div class="kpi-label">Messages</div><div class="kpi-value">${t.messages.toLocaleString()}</div></div>
      <div class="kpi"><div class="kpi-label">Tokens</div><div class="kpi-value">${fmt(t.totalTokens)}</div><div class="kpi-sub">cache hit ${(t.totalTokens ? ((t.cacheReadTokens / t.totalTokens) * 100).toFixed(1) : 0)}%</div></div>
      <div class="kpi"><div class="kpi-label">Cost</div><div class="kpi-value">${fmtUsd(t.cost)}</div></div>
    </div>
    <div class="drill-card">
      <h3>Models</h3>
      <div class="chip-row">${modelTags || '<span class="muted">none</span>'}</div>
    </div>
    <div class="drill-card" style="margin-top: 12px;">
      <h3>Cumulative cost</h3>
      <div class="drill-chart-wrap"><canvas id="sessionCumChart"></canvas></div>
    </div>
    <div class="drill-card" style="margin-top: 12px;">
      <h3>Messages (${detail.messages.length})</h3>
      <div class="scroll-wrap">
        <table class="table table-compact">
          <thead><tr><th class="r">#</th><th>Time</th><th>Model</th><th class="r">Input</th><th class="r">Output</th><th class="r">Cache R</th><th class="r">Cache W</th><th class="r">Total</th><th class="r">Cost</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>
  `;

  const cumCanvas = document.getElementById("sessionCumChart");
  if (cumCanvas) {
    new Chart(cumCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels: detail.cumulative.map((c) => fmtClock(c.ts)),
        datasets: [
          {
            label: "Cumulative cost",
            data: detail.cumulative.map((c) => c.cumulativeCost),
            borderColor: COLORS.cost,
            backgroundColor: "rgba(167, 139, 250, 0.15)",
            fill: true,
            tension: 0.2,
            pointRadius: 0,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (item) => fmtUsd(item.parsed.y) } } },
        scales: {
          x: { grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", maxRotation: 0, autoSkip: true, font: { size: 10 } } },
          y: { beginAtZero: true, grid: { color: "#232a3a" }, ticks: { color: "#8c93a8", callback: (v) => fmtUsd(v), font: { size: 10 } } },
        },
      },
    });
  }
}

// === Event wiring ===

document.getElementById("rangeSelect").addEventListener("change", () => {
  // Switching to a preset clears the custom range.
  filters.customFrom = null;
  filters.customTo = null;
  document.getElementById("customFrom").value = "";
  document.getElementById("customTo").value = "";
  scheduleRefresh(0);
});

document.getElementById("refreshBtn").addEventListener("click", () => scheduleRefresh(0));

document.getElementById("toggleTokens").addEventListener("change", () => currentData && drawDaily(currentData));
document.getElementById("toggleCost").addEventListener("change", () => currentData && drawDaily(currentData));

document.getElementById("applyCustomRange").addEventListener("click", () => {
  const f = document.getElementById("customFrom").value;
  const t = document.getElementById("customTo").value;
  if (!f || !t) return;
  filters.customFrom = f;
  filters.customTo = t;
  scheduleRefresh(0);
});

document.getElementById("resetFiltersBtn").addEventListener("click", () => {
  filters.enabledModels = null;
  filters.enabledProjects = null;
  filters.customFrom = null;
  filters.customTo = null;
  filters.search = "";
  document.getElementById("searchInput").value = "";
  document.getElementById("customFrom").value = "";
  document.getElementById("customTo").value = "";
  document.getElementById("rangeSelect").value = "7";
  scheduleRefresh(0);
});

document.getElementById("searchInput").addEventListener("input", (e) => {
  filters.search = e.target.value;
  applySearch();
});

// Per-day row clicks → expand / collapse
document.querySelector("#dailyTable").addEventListener("click", (e) => {
  const row = e.target.closest("tr.day-row");
  if (row) toggleDayRow(row.dataset.date);
});

// Session row clicks → drawer. Handle both the top-level session table and
// the per-day "Top sessions this day" table inside expanded panels.
document.body.addEventListener("click", (e) => {
  const sessionRow = e.target.closest("tr[data-session]");
  if (sessionRow) {
    showSession(sessionRow.dataset.session);
  }
});

scheduleRefresh(0);
connectSse();
setInterval(() => scheduleRefresh(0), 60_000);
