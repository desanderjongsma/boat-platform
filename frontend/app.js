const API = 'http://localhost:8000';
const WS  = 'ws://localhost:8000';

let activeVesselId = null;
let activeSocket   = null;
let telemetryChart = null;
let activeTab      = { topic: 'engine/rapid', field: 'rpm', label: 'RPM' };

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadVessels();
  setInterval(loadVessels, 10_000); // refresh vessel list every 10s
  setupTabs();
});

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
  if (id === activeVesselId) return;
  activeVesselId = id;

  // Highlight sidebar item
  document.querySelectorAll('.vessel-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === id);
  });

  // Show dashboard
  document.getElementById('empty-state').classList.add('hidden');
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

  connectWebSocket(id);
  loadTelemetryChart(id, activeTab.topic, activeTab.field, activeTab.label);
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
  document.getElementById('m-online').style.color = d.online ? 'var(--green)' : 'var(--red)';

  // Last seen
  const ls = d.last_seen ? formatRelative(d.last_seen) : '—';
  setValue('m-lastseen', ls);
  document.getElementById('vessel-last-seen').textContent = d.last_seen ? `Last seen ${ls}` : '';

  updateStatusBadge(d.online);
}

function updateStatusBadge(online) {
  const badge = document.getElementById('vessel-status-badge');
  badge.textContent = online ? 'Online' : 'Offline';
  badge.className = `status-badge ${online ? 'online' : 'offline'}`;
}

function setValue(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

// ── Telemetry chart ───────────────────────────────────────────────────────────
async function loadTelemetryChart(vesselId, topic, field, label) {
  let rows;
  try {
    const res = await fetch(`${API}/api/vessels/${vesselId}/telemetry?topic=${encodeURIComponent(`vessels/${vesselId}/${topic}`)}&limit=60`);
    rows = await res.json();
  } catch {
    rows = [];
  }

  // rows come newest-first; reverse for chronological order
  rows.reverse();

  const labels = rows.map(r => formatTime(r.time));
  const data   = rows.map(r => {
    const p = r.payload;
    return p != null ? (p[field] ?? null) : null;
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
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59,130,246,0.08)',
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointBackgroundColor: '#3b82f6',
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
          backgroundColor: '#1c2230',
          borderColor: '#2d3748',
          borderWidth: 1,
          titleColor: '#8892a4',
          bodyColor: '#e2e8f0',
          padding: 10,
        }
      },
      scales: {
        x: {
          ticks: { color: '#4a5568', maxRotation: 0, maxTicksLimit: 8 },
          grid:  { color: '#1f2937' },
        },
        y: {
          ticks: { color: '#4a5568' },
          grid:  { color: '#1f2937' },
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
