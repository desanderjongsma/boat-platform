const API = `${window.location.protocol}//${window.location.hostname}:8000`;
const WS  = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.hostname}:8000`;

let activeVesselId = null;
let activeSocket   = null;
let telemetryChart = null;
let activeTab      = { topic: 'propulsion/0/revolutions', field: 'rpm', label: 'RPM' };
let currentView    = 'empty'; // 'empty' | 'dashboard' | 'alerts'

const METRIC_LABELS = {
  coolant_temp_c:  'Coolant Temp',
  rpm:             'Engine RPM',
  battery_voltage: 'Battery Voltage',
  battery_soc:     'Battery SOC',
  depth_m:         'Water Depth',
};
const METRIC_UNITS = {
  coolant_temp_c:  '°C',
  rpm:             'rpm',
  battery_voltage: 'V',
  battery_soc:     '%',
  depth_m:         'm',
};
const OP_LABELS = { gt: '>', lt: '<', gte: '≥', lte: '≤' };

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadVessels();
  setInterval(loadVessels, 10_000);
  loadAlertBadge();
  setInterval(loadAlertBadge, 30_000);
  setupTabs();
  setupMobileDrawer();
});

// ── Mobile drawer ─────────────────────────────────────────────────────────────
function setupMobileDrawer() {
  const layout  = document.querySelector('.layout');
  const toggle  = document.getElementById('menu-toggle');
  const close   = document.getElementById('sidebar-close');
  const overlay = document.getElementById('sidebar-overlay');

  const open  = () => layout.classList.add('sidebar-open');
  const shut  = () => layout.classList.remove('sidebar-open');

  toggle?.addEventListener('click', open);
  close?.addEventListener('click', shut);
  overlay?.addEventListener('click', shut);
}

function closeSidebarOnMobile() {
  if (window.innerWidth <= 768) {
    document.querySelector('.layout').classList.remove('sidebar-open');
  }
}

// ── Vessel list ───────────────────────────────────────────────────────────────
async function loadVessels() {
  let vessels;
  try {
    const res = await fetch(`${API}/api/vessels`);
    vessels = await res.json();
  } catch {
    renderVesselList([]);
    return;
  }

  renderVesselList(vessels);

  // If a vessel is active, refresh its status dot
  if (activeVesselId) {
    const v = vessels.find(v => v.vessel_id === activeVesselId);
    if (v) updateStatusBadge(v.online);
  }
}

function renderVesselList(vessels) {
  const ul = document.getElementById('vessel-list');
  ul.innerHTML = '';

  if (!vessels.length) {
    ul.innerHTML = '<li class="vessel-item skeleton">No vessels found</li>';
    return;
  }

  vessels.forEach(v => {
    const li = document.createElement('li');
    li.className = 'vessel-item' + (v.vessel_id === activeVesselId ? ' active' : '');
    li.dataset.id = v.vessel_id;
    li.innerHTML = `
      <span class="vessel-dot ${v.online ? 'online' : 'offline'}"></span>
      <span class="vessel-name">${v.vessel_id}</span>
    `;
    li.addEventListener('click', () => selectVessel(v.vessel_id));
    ul.appendChild(li);
  });
}

// ── Select vessel ─────────────────────────────────────────────────────────────
function selectVessel(id) {
  if (id === activeVesselId && currentView === 'dashboard') return;
  activeVesselId = id;
  currentView = 'dashboard';

  // Highlight sidebar item, clear alerts nav active
  document.querySelectorAll('.vessel-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === id);
  });
  document.getElementById('nav-alerts').classList.remove('active');

  // Show dashboard, hide others
  document.getElementById('empty-state').classList.add('hidden');
  document.getElementById('alerts-view').classList.add('hidden');
  document.getElementById('dashboard').classList.remove('hidden');

  // Update vessel ID header
  document.getElementById('vessel-title').textContent = id;
  document.getElementById('m-vesselid').textContent = id;

  // Close existing WS
  if (activeSocket) {
    activeSocket.onclose = null;
    activeSocket.close();
    activeSocket = null;
  }

  closeSidebarOnMobile();
  connectWebSocket(id);
  loadTelemetryChart(id, activeTab.topic, activeTab.field, activeTab.label);
  loadAlertRules(id);
  loadVesselAlertsBanner(id);
}

// ── WebSocket – live status ───────────────────────────────────────────────────
function connectWebSocket(id) {
  const ws = new WebSocket(`${WS}/ws/${id}`);
  activeSocket = ws;

  ws.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.vessel_id !== activeVesselId) return;
    updateCards(d);
  };

  ws.onclose = () => {
    // Reconnect after 3s if this is still the active vessel
    if (id === activeVesselId) setTimeout(() => connectWebSocket(id), 3000);
  };

  ws.onerror = () => ws.close();
}

// ── Update metric cards ───────────────────────────────────────────────────────
function updateCards(d) {
  setValue('m-rpm',     d.rpm      != null ? `${Math.round(d.rpm)} rpm` : '—');
  setValue('m-coolant', d.coolant_temp_c != null ? `${d.coolant_temp_c.toFixed(1)} °C` : '—');
  setValue('m-voltage', d.battery_voltage != null ? `${d.battery_voltage.toFixed(2)} V` : '—');
  setValue('m-lat',     d.latitude  != null ? d.latitude.toFixed(5)  : '—');
  setValue('m-lon',     d.longitude != null ? d.longitude.toFixed(5) : '—');
  setValue('m-depth',   d.depth_m   != null ? `${d.depth_m.toFixed(1)} m` : '—');

  // SOC with bar
  if (d.battery_soc != null) {
    const pct = Math.min(100, Math.max(0, d.battery_soc));
    setValue('m-soc', `${pct.toFixed(1)} %`);
    document.getElementById('soc-bar').style.width = `${pct}%`;
  } else {
    setValue('m-soc', '—');
  }

  // Online status
  setValue('m-online', d.online ? 'Online' : 'Offline');
  document.getElementById('m-online').style.color = d.online ? '#16A34A' : '#DC2626';

  // Last seen
  const ls = d.last_seen ? formatRelative(d.last_seen) : '—';
  setValue('m-lastseen', ls);
  document.getElementById('vessel-last-seen').textContent = d.last_seen ? `Last seen ${ls}` : '';

  updateStatusBadge(d.online);
}

function updateStatusBadge(online) {
  ['vessel-status-badge', 'topbar-badge'].forEach(id => {
    const badge = document.getElementById(id);
    if (!badge) return;
    badge.textContent = online ? 'Online' : 'Offline';
    badge.className = `status-badge ${online ? 'online' : 'offline'}`;
  });
}

function setValue(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

// ── Telemetry chart ───────────────────────────────────────────────────────────
async function loadTelemetryChart(vesselId, topic, field, label) {
  let rows;
  try {
    const res = await fetch(`${API}/api/vessels/${vesselId}/telemetry?topic=${encodeURIComponent(topic)}&limit=60`);
    rows = await res.json();
  } catch {
    rows = [];
  }

  // rows come newest-first; reverse for chronological order
  rows.reverse();

  const labels = rows.map(r => formatTime(r.time));
  const data   = rows.map(r => {
    try {
      const p = typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload;
      return p != null ? (p['value'] ?? null) : null;
    } catch { return null; }
  });

  renderChart(labels, data, label);
}

function renderChart(labels, data, label) {
  const ctx = document.getElementById('telemetry-chart').getContext('2d');

  if (telemetryChart) {
    telemetryChart.destroy();
    telemetryChart = null;
  }

  telemetryChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label,
        data,
        borderColor: '#1A4AC4',
        backgroundColor: 'rgba(26,74,196,0.06)',
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointBackgroundColor: '#1A4AC4',
        fill: true,
        tension: 0.3,
        spanGaps: true,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#FFFFFF',
          borderColor: '#DDE3EF',
          borderWidth: 1,
          titleColor: '#6E87A8',
          bodyColor: '#0E1E3A',
          padding: 10,
        }
      },
      scales: {
        x: {
          ticks: { color: '#9DAFC8', maxRotation: 0, maxTicksLimit: 8 },
          grid:  { color: '#EBF0FA' },
        },
        y: {
          ticks: { color: '#9DAFC8' },
          grid:  { color: '#EBF0FA' },
        }
      }
    }
  });
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function setupTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeTab = {
        topic: btn.dataset.topic,
        field: btn.dataset.field,
        label: btn.dataset.label,
      };
      if (activeVesselId) {
        loadTelemetryChart(activeVesselId, activeTab.topic, activeTab.field, activeTab.label);
      }
    });
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function formatTime(isoStr) {
  const d = new Date(isoStr);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatRelative(isoStr) {
  const diff = Math.floor((Date.now() - new Date(isoStr)) / 1000);
  if (diff < 5)   return 'just now';
  if (diff < 60)  return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function formatDateTime(isoStr) {
  if (!isoStr) return '—';
  const d = new Date(isoStr);
  return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

// ── Alerts view ───────────────────────────────────────────────────────────────
function showAlertsView() {
  currentView = 'alerts';
  document.querySelectorAll('.vessel-item').forEach(el => el.classList.remove('active'));
  document.getElementById('nav-alerts').classList.add('active');
  document.getElementById('empty-state').classList.add('hidden');
  document.getElementById('dashboard').classList.add('hidden');
  document.getElementById('alerts-view').classList.remove('hidden');
  closeSidebarOnMobile();
  loadAlerts();
}

async function loadAlerts() {
  let active = [], history = [];
  try {
    const [r1, r2] = await Promise.all([
      fetch(`${API}/api/alerts?active=true&limit=100`),
      fetch(`${API}/api/alerts?active=false&limit=30`),
    ]);
    active  = await r1.json();
    history = await r2.json();
  } catch { /* network error - leave empty */ }

  renderAlertStats(active);
  renderActiveAlerts(active);
  renderHistoryAlerts(history);
  updateAlertBadge(active.length);
}

function renderAlertStats(active) {
  const critical = active.filter(a => a.severity === 'critical').length;
  const warning  = active.filter(a => a.severity === 'warning').length;
  const info     = active.filter(a => a.severity === 'info').length;
  document.getElementById('stat-total').textContent    = active.length;
  document.getElementById('stat-critical').textContent = critical;
  document.getElementById('stat-warning').textContent  = warning;
  document.getElementById('stat-info').textContent     = info;
}

function renderActiveAlerts(alerts) {
  const el = document.getElementById('active-alerts-container');
  document.getElementById('active-count-chip').textContent = alerts.length;

  if (!alerts.length) {
    el.innerHTML = '<p class="empty-hint">No active alerts</p>';
    return;
  }

  el.innerHTML = `
    <div class="alerts-table">
      ${alerts.map(a => `
        <div class="alert-row sev-${a.severity} ${a.acknowledged ? 'acked' : ''}">
          <span class="sev-badge sev-${a.severity}">${a.severity}</span>
          <span class="alert-vessel">${a.vessel_id}</span>
          <span class="alert-name">${a.rule_name}</span>
          <span class="alert-value">${METRIC_LABELS[a.metric] || a.metric}: <strong>${a.value.toFixed(2)} ${METRIC_UNITS[a.metric] || ''}</strong> ${OP_LABELS[a.operator] || a.operator} ${a.threshold}</span>
          <span class="alert-time">${formatRelative(a.triggered_at)}</span>
          <div class="alert-actions">
            ${!a.acknowledged ? `<button class="btn-ack" onclick="acknowledgeAlert(${a.id})">Acknowledge</button>` : '<span class="acked-label">Acked</span>'}
            <button class="btn-ghost-sm" onclick="selectVessel('${a.vessel_id}')">View vessel</button>
          </div>
        </div>
      `).join('')}
    </div>`;
}

function renderHistoryAlerts(alerts) {
  const el = document.getElementById('history-alerts-container');
  if (!alerts.length) {
    el.innerHTML = '<p class="empty-hint">No recent resolved alerts</p>';
    return;
  }
  el.innerHTML = `
    <div class="alerts-table">
      ${alerts.map(a => `
        <div class="alert-row resolved">
          <span class="sev-badge sev-${a.severity}">${a.severity}</span>
          <span class="alert-vessel">${a.vessel_id}</span>
          <span class="alert-name">${a.rule_name}</span>
          <span class="alert-value">${METRIC_LABELS[a.metric] || a.metric}: <strong>${a.value.toFixed(2)} ${METRIC_UNITS[a.metric] || ''}</strong></span>
          <span class="alert-time">Triggered ${formatDateTime(a.triggered_at)}</span>
          <span class="alert-time resolved-at">Resolved ${formatDateTime(a.resolved_at)}</span>
        </div>
      `).join('')}
    </div>`;
}

async function acknowledgeAlert(id) {
  try {
    await fetch(`${API}/api/alerts/${id}/acknowledge`, { method: 'POST' });
    loadAlerts();
    loadAlertBadge();
  } catch { /* ignore */ }
}

// ── Alert badge (sidebar) ─────────────────────────────────────────────────────
async function loadAlertBadge() {
  try {
    const res = await fetch(`${API}/api/alerts?active=true&limit=100`);
    const alerts = await res.json();
    updateAlertBadge(alerts.length);
    // If a vessel is selected, also refresh its banner
    if (activeVesselId && currentView === 'dashboard') {
      loadVesselAlertsBanner(activeVesselId);
    }
  } catch { /* offline */ }
}

function updateAlertBadge(count) {
  const badge = document.getElementById('alerts-count-badge');
  if (count > 0) {
    badge.textContent = count;
    badge.classList.remove('hidden');
  } else {
    badge.classList.add('hidden');
  }
}

// ── Vessel alerts banner (in dashboard) ───────────────────────────────────────
async function loadVesselAlertsBanner(vesselId) {
  const banner = document.getElementById('vessel-alerts-banner');
  try {
    const res = await fetch(`${API}/api/alerts?vessel_id=${vesselId}&active=true&limit=10`);
    const alerts = await res.json();
    if (!alerts.length) {
      banner.classList.add('hidden');
      return;
    }
    const critical = alerts.filter(a => a.severity === 'critical').length;
    const warning  = alerts.filter(a => a.severity === 'warning').length;
    const parts = [];
    if (critical) parts.push(`${critical} critical`);
    if (warning)  parts.push(`${warning} warning`);
    const other = alerts.length - critical - warning;
    if (other)    parts.push(`${other} info`);

    banner.className = `vessel-alerts-banner ${critical ? 'sev-critical' : 'sev-warning'}`;
    banner.innerHTML = `
      <span class="banner-icon">${critical ? '🔴' : '🟡'}</span>
      <span>${parts.join(', ')} alert${alerts.length > 1 ? 's' : ''} active for this vessel</span>
      <button class="btn-ghost-sm" onclick="showAlertsView()">View all alerts</button>`;
  } catch {
    banner.classList.add('hidden');
  }
}

// ── Alert rules (vessel dashboard) ───────────────────────────────────────────
async function loadAlertRules(vesselId) {
  let rules = [];
  try {
    const res = await fetch(`${API}/api/vessels/${vesselId}/alert-rules`);
    rules = await res.json();
  } catch { /* offline */ }
  renderAlertRules(rules);
}

function renderAlertRules(rules) {
  const el = document.getElementById('alert-rules-list');
  if (!rules.length) {
    el.innerHTML = '<p class="empty-hint">No alert rules configured</p>';
    return;
  }
  el.innerHTML = `
    <div class="rules-table">
      <div class="rules-header">
        <span>Name</span><span>Metric</span><span>Condition</span><span>Severity</span><span>Status</span><span></span>
      </div>
      ${rules.map(r => `
        <div class="rule-row ${r.enabled ? '' : 'disabled'}">
          <span class="rule-name">${r.name}</span>
          <span>${METRIC_LABELS[r.metric] || r.metric}</span>
          <span class="rule-cond">${OP_LABELS[r.operator] || r.operator} ${r.threshold} ${METRIC_UNITS[r.metric] || ''}</span>
          <span><span class="sev-badge sev-${r.severity}">${r.severity}</span></span>
          <span>
            <label class="toggle">
              <input type="checkbox" ${r.enabled ? 'checked' : ''} onchange="toggleAlertRule(${r.id}, this.checked)" />
              <span class="toggle-slider"></span>
            </label>
          </span>
          <span>
            <button class="btn-danger-sm" onclick="deleteAlertRule(${r.id})">Delete</button>
          </span>
        </div>
      `).join('')}
    </div>`;
}

function showAddRuleForm() {
  document.getElementById('add-rule-form').classList.remove('hidden');
  document.getElementById('rule-name').focus();
}

function hideAddRuleForm() {
  document.getElementById('add-rule-form').classList.add('hidden');
  // Clear fields
  ['rule-name', 'rule-threshold'].forEach(id => document.getElementById(id).value = '');
}

async function saveAlertRule() {
  const name      = document.getElementById('rule-name').value.trim();
  const metric    = document.getElementById('rule-metric').value;
  const operator  = document.getElementById('rule-operator').value;
  const threshold = parseFloat(document.getElementById('rule-threshold').value);
  const severity  = document.getElementById('rule-severity').value;

  if (!name || isNaN(threshold)) {
    alert('Please fill in all fields.');
    return;
  }

  try {
    await fetch(`${API}/api/vessels/${activeVesselId}/alert-rules`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, metric, operator, threshold, severity }),
    });
    hideAddRuleForm();
    loadAlertRules(activeVesselId);
  } catch {
    alert('Failed to save rule. Check the API connection.');
  }
}

async function toggleAlertRule(ruleId, enabled) {
  try {
    await fetch(`${API}/api/alert-rules/${ruleId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled }),
    });
    loadAlertRules(activeVesselId);
  } catch { /* ignore */ }
}

async function deleteAlertRule(ruleId) {
  if (!confirm('Delete this alert rule?')) return;
  try {
    await fetch(`${API}/api/alert-rules/${ruleId}`, { method: 'DELETE' });
    loadAlertRules(activeVesselId);
  } catch {
    alert('Failed to delete rule.');
  }
}
