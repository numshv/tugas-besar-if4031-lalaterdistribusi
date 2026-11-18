(() => {
  "use strict";

  const CATEGORIES = [
    { key: "GEMPA", label: "Gempa", color: "--s1" },
    { key: "TSUNAMI", label: "Tsunami", color: "--s2" },
    { key: "VOLCANIC", label: "Gunung api", color: "--s3" },
    { key: "WILDFIRE", label: "Kebakaran hutan", color: "--s4" },
    { key: "COASTAL", label: "Pesisir", color: "--s5" },
  ];
  const SEVERITIES = ["NORMAL", "WASPADA", "SIAGA", "AWAS"];
  const BUCKET_LABEL = { hour: "per jam", day: "per hari", week: "per minggu", month: "per bulan" };

  const $ = (id) => document.getElementById(id);
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const fmtNum = new Intl.NumberFormat("id-ID");
  Chart.defaults.locale = "id-ID";

  let access = null, refresh = null, refreshTimer = null, refreshing = null;
  let query = { range: "30d" };
  let lastResult = null;
  let areaSort = { key: "count", dir: -1 };
  let timeline = null, severity = null;

  function decodeExp(token) {
    try {
      const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
      return payload.exp * 1000;
    } catch { return Date.now() + 30000; }
  }

  function setTokens(pair) {
    access = pair.access_token;
    refresh = pair.refresh_token;
    clearTimeout(refreshTimer);
    const wait = Math.max(5000, decodeExp(access) - Date.now() - 10000);
    refreshTimer = setTimeout(() => refreshTokens().catch(showLogin), wait);
  }

  function refreshTokens() {
    if (refreshing) return refreshing;
    refreshing = fetch("/refresh", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refresh }),
    }).then(async (res) => {
      if (!res.ok) throw new Error("refresh failed");
      setTokens(await res.json());
    }).finally(() => { refreshing = null; });
    return refreshing;
  }

  function showLogin(message) {
    access = refresh = null;
    clearTimeout(refreshTimer);
    $("login-error").textContent = typeof message === "string" ? message : "";
    $("login").classList.add("show");
    $("username").focus();
  }

  async function api(path) {
    for (let attempt = 0; attempt < 2; attempt++) {
      const res = await fetch(path, { headers: { Authorization: "Bearer " + access } });
      if (res.status === 401 && attempt === 0 && refresh) {
        try { await refreshTokens(); continue; } catch { break; }
      }
      if (res.status === 401) break;
      if (res.status === 403) throw new Error("Akun ini tidak punya akses ke dashboard analitik.");
      if (!res.ok) {
        let msg = "HTTP " + res.status;
        try { msg = (await res.json()).error.message; } catch {}
        throw new Error(msg);
      }
      return res.json();
    }
    showLogin("Sesi berakhir, silakan masuk lagi.");
    throw new Error("unauthorized");
  }

  $("login-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    $("login-error").textContent = "";
    const res = await fetch("/login", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: $("username").value, password: $("password").value }),
    }).catch(() => null);
    if (!res || !res.ok) { $("login-error").textContent = "Username atau password salah."; return; }
    setTokens(await res.json());
    $("password").value = "";
    $("login").classList.remove("show");
    load();
  });

  $("logout").addEventListener("click", () => showLogin(""));

  document.querySelectorAll("[data-range]").forEach((btn) => btn.addEventListener("click", () => {
    query = { range: btn.dataset.range };
    markRange(btn.dataset.range);
    load();
  }));
  $("apply-custom").addEventListener("click", () => {
    const from = $("from").value, to = $("to").value;
    if (!from || !to) return;
    query = { from: new Date(from + "Z").toISOString().replace(".000", ""), to: new Date(to + "Z").toISOString().replace(".000", "") };
    markRange(null);
    load();
  });
  $("bucket").addEventListener("change", load);
  $("area-search").addEventListener("input", renderAreas);
  document.querySelectorAll("[data-sort]").forEach((btn) => btn.addEventListener("click", () => {
    const key = btn.dataset.sort;
    areaSort = { key, dir: areaSort.key === key ? -areaSort.dir : (key === "count" ? -1 : 1) };
    renderAreas();
  }));

  function markRange(range) {
    document.querySelectorAll("[data-range]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.range === range)));
  }

  async function load() {
    if (!access) return;
    const params = new URLSearchParams(query);
    if ($("bucket").value) params.set("bucket", $("bucket").value);
    try {
      const data = await api("/analytics/data?" + params.toString());
      lastResult = data;
      render(data);
    } catch (err) {
      if (err.message !== "unauthorized") setBanner("critical", "!", "Gagal memuat data: " + err.message + ". Angka terakhir tetap ditampilkan.");
    }
  }

  const utc = (opts) => new Intl.DateTimeFormat("id-ID", Object.assign({ timeZone: "UTC" }, opts));
  const FORMAT = {
    hour: utc({ day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }),
    day: utc({ day: "2-digit", month: "short", year: "numeric" }),
    week: utc({ day: "2-digit", month: "short", year: "numeric" }),
    month: utc({ month: "short", year: "numeric" }),
  };

  function setBanner(kind, icon, text) {
    const el = $("banner");
    el.className = "banner" + (kind ? " show " + kind : "");
    $("banner-icon").textContent = icon || "";
    $("banner-text").textContent = text || "";
  }

  function count(n, partial) { return (partial ? "≥ " : "") + fmtNum.format(n); }

  function render(d) {
    const units = (d.unavailable_units || []).join(", ");
    if (d.partial) {
      setBanner("critical", "⚠ Belum lengkap", "Sebagian penyimpanan tidak terjangkau" + (units ? " (" + units + ")" : "") + ". Angka di bawah adalah batas bawah (≥), bukan total final.");
    } else if (units) {
      setBanner("warning", "! Redundansi berkurang", "Unit penyimpanan mati: " + units + ". Data masih lengkap dari salinan lain, angka tetap akurat.");
    } else {
      setBanner(null);
    }
    $("asof").textContent = "Dihitung " + new Date(d.as_of).toLocaleString("id-ID") + " · metode " + d.method;
    $("total").textContent = count(d.total, d.partial);
    $("range-label").textContent = FORMAT.day.format(new Date(d.from)) + " – " + FORMAT.day.format(new Date(d.to)) + " (UTC)";
    $("bucket-label").textContent = BUCKET_LABEL[d.bucket] || d.bucket;

    const catBody = $("category-table");
    catBody.replaceChildren(...CATEGORIES.map((c) => {
      const tr = document.createElement("tr");
      const name = document.createElement("td");
      const key = document.createElement("span");
      key.className = "key";
      key.style.background = css(c.color);
      name.append(key, document.createTextNode(c.label));
      const num = document.createElement("td");
      num.className = "num";
      num.textContent = count((d.by_category || {})[c.key] || 0, d.partial);
      tr.append(name, num);
      return tr;
    }));

    const labels = (d.buckets || []).map((t) => FORMAT[d.bucket].format(new Date(t)));
    const datasets = CATEGORIES.map((c) => ({
      label: c.label, data: (d.series || {})[c.key] || [], borderColor: css(c.color), backgroundColor: css(c.color),
      borderWidth: 2, pointRadius: 0, pointHoverRadius: 5, pointHitRadius: 12, tension: 0, spanGaps: true,
    }));
    if (!timeline) {
      timeline = new Chart($("timeline"), { type: "line", data: { labels, datasets }, options: lineOptions() });
    } else {
      timeline.data.labels = labels;
      timeline.data.datasets.forEach((ds, i) => { ds.data = datasets[i].data; });
      timeline.update("none");
    }

    const sevValues = SEVERITIES.map((s) => (d.by_severity || {})[s] || 0);
    if (!severity) {
      severity = new Chart($("severity"), {
        type: "bar",
        data: { labels: SEVERITIES, datasets: [{ label: "Jumlah kejadian", data: sevValues, backgroundColor: css("--s1"), borderRadius: { topLeft: 4, topRight: 4 }, borderSkipped: "bottom", maxBarThickness: 56 }] },
        options: barOptions(),
      });
    } else {
      severity.data.datasets[0].data = sevValues;
      severity.update("none");
    }
    $("severity-table").replaceChildren(...SEVERITIES.map((s, i) => {
      const tr = document.createElement("tr");
      const a = document.createElement("td"); a.textContent = s;
      const b = document.createElement("td"); b.className = "num"; b.textContent = count(sevValues[i], d.partial);
      tr.append(a, b);
      return tr;
    }));
    renderAreas();
  }

  function renderAreas() {
    if (!lastResult) return;
    const term = $("area-search").value.trim().toLowerCase();
    const rows = (lastResult.by_area || [])
      .filter((a) => !term || (a.area_name || "").toLowerCase().includes(term))
      .slice()
      .sort((x, y) => {
        const k = areaSort.key;
        const cmp = k === "count" ? x.count - y.count : (x.area_name || "").localeCompare(y.area_name || "", "id");
        return cmp * areaSort.dir;
      });
    $("area-table").replaceChildren(...rows.map((a) => {
      const tr = document.createElement("tr");
      const n = document.createElement("td"); n.textContent = a.area_name || "(tanpa wilayah)";
      const c = document.createElement("td"); c.className = "num"; c.textContent = count(a.count, lastResult.partial);
      tr.append(n, c);
      return tr;
    }));
  }

  function axes() {
    return {
      x: { grid: { display: false }, border: { color: css("--axis") }, ticks: { color: css("--muted"), maxRotation: 0, autoSkip: true, autoSkipPadding: 16, maxTicksLimit: 8 } },
      y: { beginAtZero: true, grid: { color: css("--grid") }, border: { display: false }, ticks: { color: css("--muted"), precision: 0 } },
    };
  }

  function tooltip() {
    return {
      backgroundColor: css("--surface"), titleColor: css("--ink"), bodyColor: css("--ink"), borderColor: css("--ring"), borderWidth: 1,
      usePointStyle: true, boxWidth: 14, boxHeight: 2,
      callbacks: { label: (ctx) => " " + fmtNum.format(ctx.parsed.y) + "  " + ctx.dataset.label },
    };
  }

  function lineOptions() {
    return {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "index", intersect: false },
      scales: axes(),
      plugins: {
        legend: { position: "top", align: "start", labels: { color: css("--ink-2"), usePointStyle: true, pointStyle: "line", boxWidth: 16 } },
        tooltip: Object.assign(tooltip(), { itemSort: (a, b) => b.parsed.y - a.parsed.y }),
      },
    };
  }

  function barOptions() {
    return {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "nearest", intersect: true },
      scales: axes(),
      plugins: { legend: { display: false }, tooltip: tooltip() },
    };
  }

  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    [timeline, severity].forEach((c) => c && c.destroy());
    timeline = severity = null;
    if (lastResult) render(lastResult);
  });

  setInterval(() => { if (!document.hidden) load(); }, 10000);
})();
