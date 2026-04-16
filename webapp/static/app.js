const state = {
  pages: [],
  pageIndex: 0,
  items: [],
  selected: new Map(),
  pickerSessionId: null,
  jobStatusById: new Map(),
  cacheStateById: new Map(),
  cacheProgressById: new Map(),
  cachePollTimer: null,
  jobsPollTimer: null,
  latestJobRow: null,
};
const videoRetryCountById = new Map();

function $(sel) {
  return document.querySelector(sel);
}

function setTab(name) {
  document.querySelectorAll(".tab").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll(".panel").forEach((p) => {
    p.classList.toggle("active", p.id === `panel-${name}`);
  });
}

document.querySelectorAll(".tab").forEach((b) => {
  b.addEventListener("click", () => setTab(b.dataset.tab));
});

async function authStatus() {
  const r = await fetch("/api/auth/status");
  const j = await r.json();
  const label = $("#auth-label");
  const connect = $("#connect-btn");
  if (j.connected) {
    label.textContent = "Google Photos verbunden";
    connect.style.display = "none";
  } else {
    label.textContent = "Nicht verbunden";
    connect.style.display = "inline-block";
  }
}

function itemKey(it) {
  return it.id;
}

function itemBaseUrl(it) {
  return (
    (it.mediaFile && it.mediaFile.baseUrl) ||
    it.baseUrl ||
    ""
  );
}

function itemMimeType(it) {
  return (
    (it.mediaFile && it.mediaFile.mimeType) ||
    it.mimeType ||
    ""
  );
}

function itemType(it) {
  return String(it.type || "").toUpperCase();
}

function itemFilename(it) {
  return (
    (it.mediaFile && it.mediaFile.filename) ||
    it.filename ||
    null
  );
}

function itemProcessingStatus(it) {
  return (
    (it.mediaFile &&
      it.mediaFile.mediaFileMetadata &&
      it.mediaFile.mediaFileMetadata.videoMetadata &&
      it.mediaFile.mediaFileMetadata.videoMetadata.processingStatus) ||
    ""
  );
}

function isStartableVideo(it) {
  const mime = itemMimeType(it).toLowerCase();
  const isVideo = mime.startsWith("video/") || itemType(it) === "VIDEO";
  if (!isVideo || !itemBaseUrl(it)) return false;
  const processing = itemProcessingStatus(it).toUpperCase();
  return !processing || processing === "READY";
}

function updateProcessingStatus() {
  const el = $("#processing-status");
  if (!el) return;
  const processingCount = state.items.filter(
    (it) => itemProcessingStatus(it).toUpperCase() === "PROCESSING"
  ).length;
  if (processingCount > 0) {
    const noun = processingCount === 1 ? "Video ist" : "Videos sind";
    el.textContent = `${processingCount} ${noun} noch nicht startbar (Google verarbeitet noch). Bitte kurz warten und neu laden.`;
    el.classList.remove("hidden");
  } else {
    el.textContent = "";
    el.classList.add("hidden");
  }
}

function pickerProxyUrl(baseUrl, kind) {
  if (!baseUrl) return "";
  const u = new URL("/api/picker/proxy", window.location.origin);
  u.searchParams.set("base_url", baseUrl);
  u.searchParams.set("kind", kind);
  return u.toString();
}

function cachedVideoUrl(it, cacheBust = false) {
  const id = itemKey(it);
  const base = itemBaseUrl(it);
  if (!id || !base) return "";
  const u = new URL("/api/cache/video", window.location.origin);
  u.searchParams.set("media_item_id", id);
  u.searchParams.set("base_url", base);
  const filename = itemFilename(it);
  if (filename) u.searchParams.set("filename", filename);
  if (cacheBust) u.searchParams.set("_ts", String(Date.now()));
  return u.toString();
}

async function fetchCachedStatus(it) {
  const id = itemKey(it);
  if (!id) return { ready: false, size_bytes: 0 };
  const u = new URL("/api/cache/status", window.location.origin);
  u.searchParams.set("media_item_id", id);
  const filename = itemFilename(it);
  if (filename) u.searchParams.set("filename", filename);
  try {
    const r = await fetch(u.toString());
    if (!r.ok) return { ready: false, size_bytes: 0 };
    return await r.json();
  } catch (_) {
    return { ready: false, size_bytes: 0 };
  }
}

function setCacheState(id, text, stateClass) {
  const node = document.querySelector(`.cache-state[data-cache-id="${CSS.escape(id)}"]`);
  state.cacheStateById.set(id, stateClass || "pending");
  if (node) {
    node.textContent = text;
    node.classList.remove("pending", "loading", "ready", "error");
    if (stateClass) node.classList.add(stateClass);
  }
  updateCacheSummary();
}

function setCacheProgress(id, pct) {
  const clamped = Math.max(0, Math.min(100, Math.round(Number(pct) || 0)));
  state.cacheProgressById.set(id, clamped);
  const bar = document.querySelector(`.cache-item-progress-bar[data-cache-id="${CSS.escape(id)}"]`);
  const text = document.querySelector(`.cache-item-progress-value[data-cache-id="${CSS.escape(id)}"]`);
  if (bar) bar.style.width = `${clamped}%`;
  if (text) text.textContent = `${clamped}%`;
  updateCacheSummary();
}

function updateCacheSummary() {
  const box = $("#cache-summary");
  const text = $("#cache-summary-text");
  const bar = $("#cache-summary-bar");
  if (!box || !text || !bar) return;

  const items = visibleItems().filter((it) => {
    const mime = itemMimeType(it).toLowerCase();
    return (mime.startsWith("video/") || itemType(it) === "VIDEO") && itemBaseUrl(it);
  });
  const total = items.length;
  if (total === 0) {
    box.classList.add("hidden");
    bar.style.width = "0%";
    text.textContent = "";
    return;
  }

  let ready = 0;
  let loading = 0;
  let pending = 0;
  let error = 0;
  for (const it of items) {
    const id = itemKey(it);
    const s = state.cacheStateById.get(id) || "pending";
    if (s === "ready") ready += 1;
    else if (s === "loading") loading += 1;
    else if (s === "error") error += 1;
    else pending += 1;
  }

  let weighted = 0;
  for (const it of items) {
    const id = itemKey(it);
    const stateName = state.cacheStateById.get(id) || "pending";
    if (stateName === "ready") {
      weighted += 100;
      continue;
    }
    weighted += state.cacheProgressById.get(id) || 0;
  }
  const pct = Math.round(weighted / total);
  box.classList.remove("hidden");
  bar.style.width = `${pct}%`;
  text.innerHTML = `
    <div class="cache-summary-head">
      <span class="cache-summary-title">Lokaler Download</span>
      <span class="cache-summary-main-number">${ready}/${total}</span>
      <span class="cache-summary-main-label">bereit</span>
      <span class="cache-summary-main-pct">${pct}%</span>
    </div>
    <div class="cache-badges">
      <span class="cache-badge ready">Ready ${ready}</span>
      <span class="cache-badge loading">Lädt ${loading}</span>
      <span class="cache-badge pending">Wartend ${pending}</span>
      <span class="cache-badge error">Fehler ${error}</span>
    </div>
  `;
}

function visibleItems() {
  const q = ($("#filter-input").value || "").trim().toLowerCase();
  if (!q) return state.items;
  return state.items.filter((it) => {
    const name = (itemFilename(it) || "").toLowerCase();
    const mime = (itemMimeType(it) || "").toLowerCase();
    return name.includes(q) || mime.includes(q);
  });
}

function renderGrid() {
  const root = $("#media-grid");
  root.innerHTML = "";
  const list = visibleItems();
  for (const it of list) {
    const id = itemKey(it);
    if (!state.cacheStateById.has(id)) {
      state.cacheStateById.set(id, "pending");
    }
  }
  for (const it of list) {
    const id = itemKey(it);
    const mime = itemMimeType(it).toLowerCase();
    const base = itemBaseUrl(it);
    const thumb = pickerProxyUrl(base, "thumb");
    const preview = cachedVideoUrl(it);
    const card = document.createElement("div");
    card.className = "card";
    const checked = state.selected.has(id);
    card.innerHTML = `
      <div class="card-row">
        <div class="card-check">
          <input type="checkbox" data-id="${id}" ${checked ? "checked" : ""} />
        </div>
        <div style="flex:1;min-width:0">
          ${
            base && mime.includes("video")
              ? `<video class="preview" src="${escapeHtml(preview)}" poster="${escapeHtml(thumb)}" preload="metadata" controls playsinline></video>`
              : thumb
              ? `<img src="${escapeHtml(thumb)}" alt="" loading="lazy" />`
              : ""
          }
          <div class="meta">${escapeHtml(itemFilename(it) || id)}</div>
          <div class="meta muted">${escapeHtml(
            itemMimeType(it) || ""
          )}</div>
          <div class="meta muted">Quelle: lokaler Cache (kein Live-Streaming)</div>
          <div class="cache-state pending" data-cache-id="${escapeHtml(id)}">Pruefe lokalen Status...</div>
          <div class="cache-item-progress">
            <div class="cache-item-progress-track"><div class="cache-item-progress-bar" data-cache-id="${escapeHtml(id)}" style="width:0%"></div></div>
            <div class="cache-item-progress-value" data-cache-id="${escapeHtml(id)}">0%</div>
          </div>
          ${
            itemProcessingStatus(it)
              ? `<div class="meta muted">Status: ${escapeHtml(itemProcessingStatus(it))}</div>`
              : ""
          }
        </div>
      </div>`;
    root.appendChild(card);
  }
  root.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
    cb.addEventListener("change", () => {
      const id = cb.dataset.id;
      const full = state.items.find((x) => itemKey(x) === id);
      if (!full) return;
      if (cb.checked) state.selected.set(id, full);
      else state.selected.delete(id);
    });
  });

  root.querySelectorAll("video.preview").forEach((video) => {
    const id = (video.closest(".card-row") || video.closest(".card"))?.querySelector('input[type="checkbox"]')?.dataset?.id;
    const item = id ? list.find((it) => itemKey(it) === id) : null;
    if (id) {
      setCacheState(id, "Noch nicht lokal. Beim Start wird heruntergeladen...", "pending");
      setCacheProgress(id, 0);
    }
    video.addEventListener("loadedmetadata", () => {
      if (id) {
        setCacheState(id, "Lokal bereit (abspielbar)", "ready");
        setCacheProgress(id, 100);
      }
      if (video.videoWidth > 0 && video.videoHeight > 0) {
        video.style.aspectRatio = `${video.videoWidth} / ${video.videoHeight}`;
      }
    });
    video.addEventListener("loadstart", () => {
      if (id) {
        setCacheState(id, "Download laeuft...", "loading");
        setCacheProgress(id, Math.max(5, state.cacheProgressById.get(id) || 0));
      }
    });
    video.addEventListener("progress", () => {
      if (!id) return;
      if (!Number.isFinite(video.duration) || video.duration <= 0 || video.buffered.length === 0) return;
      const end = video.buffered.end(video.buffered.length - 1);
      const pct = (end / video.duration) * 100;
      if (pct > 0) setCacheProgress(id, pct);
    });
    video.addEventListener("canplay", () => {
      if (id) {
        setCacheState(id, "Lokal bereit (abspielbar)", "ready");
        setCacheProgress(id, 100);
      }
    });
    video.addEventListener("error", async () => {
      if (!id || !item) return;
      const tries = videoRetryCountById.get(id) || 0;
      const status = String(itemProcessingStatus(item) || "").toUpperCase();
      const local = await fetchCachedStatus(item);
      if (local.ready) {
        setCacheState(id, "Lokal bereit (abspielbar)", "ready");
        setCacheProgress(id, 100);
        return;
      }

      // Google can still settle signed URLs; retry a few times before showing error.
      if (tries < 3) {
        videoRetryCountById.set(id, tries + 1);
        setCacheState(id, `Download laeuft... (Versuch ${tries + 1}/3)`, "loading");
        setCacheProgress(id, Math.max(10, state.cacheProgressById.get(id) || 0));
        setTimeout(() => {
          const fresh = cachedVideoUrl(item, true);
          if (!fresh) return;
          video.src = fresh;
          video.load();
        }, 1200);
        return;
      }

      if (status === "PROCESSING") {
        setCacheState(id, "Noch nicht bereit (Google verarbeitet noch).", "pending");
        return;
      }
      setCacheProgress(id, 0);
      setCacheState(id, "Download fehlgeschlagen. Bitte spaeter erneut versuchen.", "error");
    });
    video.addEventListener("play", async () => {
      video.muted = false;
      if (video.volume === 0) video.volume = 1;
      try {
        if (document.fullscreenElement) return;
        if (typeof video.requestFullscreen === "function") {
          await video.requestFullscreen();
          return;
        }
        // Safari fallback
        if (typeof video.webkitEnterFullscreen === "function") {
          video.webkitEnterFullscreen();
        }
      } catch (_) {}
    });
  });

  list.forEach(async (it) => {
    const id = itemKey(it);
    if (!id) return;
    const st = await fetchCachedStatus(it);
    if (st.ready) {
      setCacheState(id, "Lokal bereit (abspielbar)", "ready");
      setCacheProgress(id, 100);
    } else {
      setCacheState(id, "Noch nicht lokal. Beim Start wird heruntergeladen...", "pending");
      setCacheProgress(id, 0);
    }
  });
  updateCacheSummary();
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatProgress(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  const p = Math.max(0, Math.min(100, Math.round(Number(value) * 100)));
  return `${p}%`;
}

function trimMethodLabel(m) {
  const map = {
    silence_all: "Stille · alle Profile",
    silence_conservative: "Stille · Conservative",
    silence_balanced: "Stille · Balanced",
    silence_aggressive: "Stille · Aggressive",
    openai_speech: "OpenAI · Sprache",
  };
  return map[m] || m || "";
}

function formatUsdAmount(n) {
  if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
  return new Intl.NumberFormat("de-DE", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  }).format(Number(n));
}

async function loadStats() {
  const statusEl = $("#stats-status");
  try {
    const r = await fetch("/api/stats");
    const data = await r.json();
    const disc = $("#stats-disclaimer");
    if (disc) {
      const rate = data.openai_usd_per_minute_assumed;
      const rateHint =
        rate != null && !Number.isNaN(Number(rate))
          ? ` Aktuell angenommen: ${formatUsdAmount(rate)} pro Minute Audio.`
          : "";
      disc.textContent = `${data.disclaimer_de || ""}${rateHint}`;
    }
    const tbody = $("#stats-body");
    if (tbody) {
      tbody.innerHTML = "";
      for (const row of data.by_method || []) {
        const isOpenai = row.method_key === "openai_speech";
        const tr = document.createElement("tr");
        const mins =
          isOpenai && Number(row.openai_audio_minutes) > 0
            ? Number(row.openai_audio_minutes).toLocaleString("de-DE", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })
            : isOpenai
              ? "0,00"
              : "—";
        tr.innerHTML = `<td>${escapeHtml(row.label_de || row.method_key || "")}</td><td>${Number(
          row.jobs_done || 0
        )}</td><td>${Number(row.outputs_created || 0)}</td><td>${mins}</td><td>${
          isOpenai ? formatUsdAmount(row.openai_cost_usd) : "—"
        }</td><td>${isOpenai ? formatUsdAmount(row.openai_usage_credits_usd) : "—"}</td>`;
        tbody.appendChild(tr);
      }
    }
    const tot = $("#stats-totals");
    if (tot) {
      const t = data.totals || {};
      const openaiUsd = formatUsdAmount(t.openai_cost_usd);
      tot.innerHTML = `<strong>Gesamt (nur „done“):</strong> ${Number(
        t.jobs_done || 0
      )} Jobs · ${Number(t.outputs_created || 0)} Ausgaben/Clips. OpenAI geschätzt: ${openaiUsd} (Credits/Nutzung in USD: ${formatUsdAmount(
        t.openai_usage_credits_usd
      )}).`;
    }
    if (statusEl) statusEl.textContent = "Nur abgeschlossene Jobs (Status „done“).";
  } catch {
    if (statusEl) statusEl.textContent = "Statistik konnte nicht geladen werden.";
  }
}

function parseJobOptionsSummary(optionsRaw) {
  try {
    const parsed = JSON.parse(optionsRaw || "{}");
    const parts = [];
    if (parsed.trim_method) parts.push(String(parsed.trim_method));
    const profiles = Array.isArray(parsed.profiles) ? parsed.profiles : [];
    if (profiles.length) parts.push(profiles.join(", "));
    return parts.length ? parts.join(" · ") : "-";
  } catch (_) {
    return "-";
  }
}

function renderJobTypeBadge(jobType) {
  const value = String(jobType || "clip_pipeline");
  let css = "job-badge-type-pipeline";
  if (value === "silence_remove") css = "job-badge-type-silence";
  else if (value === "openai_speech_trim") css = "job-badge-type-openai";
  return `<span class="job-badge ${css}">${escapeHtml(value)}</span>`;
}

function renderTrimMethodBadge(optionsRaw) {
  try {
    const parsed = JSON.parse(optionsRaw || "{}");
    const m = String(parsed.trim_method || "");
    const label = trimMethodLabel(m) || m || "-";
    const css =
      m === "openai_speech"
        ? "job-badge-method-openai"
        : m.startsWith("silence_")
        ? "job-badge-method-silence"
        : "job-badge-neutral";
    return `<span class="job-badge ${css}">${escapeHtml(label)}</span>`;
  } catch (_) {
    return '<span class="job-badge job-badge-neutral">-</span>';
  }
}

function renderProfileBadges(optionsRaw) {
  try {
    const parsed = JSON.parse(optionsRaw || "{}");
    const profiles = Array.isArray(parsed.profiles) ? parsed.profiles : [];
    if (!profiles.length) return "";
    return profiles
      .map((p) => {
        const name = String(p || "");
        const css =
          name === "conservative"
            ? "job-badge-profile-conservative"
            : name === "balanced"
            ? "job-badge-profile-balanced"
            : name === "aggressive"
            ? "job-badge-profile-aggressive"
            : "job-badge-neutral";
        return `<span class="job-badge ${css}">${escapeHtml(name)}</span>`;
      })
      .join(" ");
  } catch (_) {
    return "";
  }
}

function renderStatusBadge(status) {
  const value = String(status || "").toLowerCase();
  const css =
    value === "done"
      ? "job-badge-status-done"
      : value === "running"
      ? "job-badge-status-running"
      : value === "queued"
      ? "job-badge-status-queued"
      : value === "failed"
      ? "job-badge-status-failed"
      : "job-badge-neutral";
  return `<span class="job-badge ${css}">${escapeHtml(value || "-")}</span>`;
}

function showPage(idx) {
  const p = state.pages[idx];
  if (!p) return;
  state.pageIndex = idx;
  state.items = p.items;
  $("#media-status").textContent = `${state.items.length} Videos auf dieser Seite.`;
  $("#next-page").disabled = !p.nextPageToken;
  $("#prev-page").disabled = idx <= 0;
  renderGrid();
  updateProcessingStatus();
}

function selectAllCurrentItems() {
  for (const it of state.items) {
    state.selected.set(itemKey(it), it);
  }
}

async function fetchPage(pageToken) {
  if (!state.pickerSessionId) {
    $("#media-status").textContent = "Kein Picker-Session aktiv.";
    return null;
  }
  $("#media-status").textContent = "Lade…";
  const url = new URL("/api/picker/media", window.location.origin);
  url.searchParams.set("session_id", state.pickerSessionId);
  if (pageToken) url.searchParams.set("page_token", pageToken);
  const r = await fetch(url);
  if (!r.ok) {
    let detail = "";
    try {
      detail = (await r.json()).detail || "";
    } catch (_) {}
    $("#media-status").textContent = `Fehler: ${r.status} ${detail}`;
    return null;
  }
  return r.json();
}

function parseSeconds(durationLike, fallbackSec = 2) {
  if (!durationLike) return fallbackSec;
  const m = String(durationLike).match(/^(\d+)(?:\.(\d+))?s$/);
  if (!m) return fallbackSec;
  const whole = Number(m[1] || 0);
  const frac = Number(`0.${m[2] || "0"}`);
  return Math.max(1, Math.floor(whole + frac));
}

async function pollSessionUntilReady(sessionId, pollIntervalSec = 2, timeoutSec = 120) {
  const start = Date.now();
  while (Date.now() - start < timeoutSec * 1000) {
    const r = await fetch(`/api/picker/session/${encodeURIComponent(sessionId)}`);
    if (!r.ok) {
      const t = await r.text();
      throw new Error(`Session-Status fehlgeschlagen: ${r.status} ${t}`);
    }
    const s = await r.json();
    if (s.mediaItemsSet) {
      return s;
    }
    const nextWait = parseSeconds(s.pollingConfig && s.pollingConfig.pollInterval, pollIntervalSec);
    $("#media-status").textContent = "Warte auf Auswahl im Google Picker…";
    await new Promise((resolve) => setTimeout(resolve, nextWait * 1000));
  }
  throw new Error("Timeout: Keine Auswahl im Picker bestätigt.");
}

$("#refresh-media").addEventListener("click", async () => {
  $("#media-status").textContent = "Starte Picker-Session…";
  try {
    await fetch("/api/cache/clear", { method: "POST" });
  } catch (_) {}
  state.cacheStateById.clear();
  const createResp = await fetch("/api/picker/session", { method: "POST" });
  if (!createResp.ok) {
    let detail = "";
    try {
      detail = (await createResp.json()).detail || "";
    } catch (_) {}
    $("#media-status").textContent = `Fehler: ${createResp.status} ${detail}`;
    return;
  }

  const session = await createResp.json();
  state.pickerSessionId = session.id;
  const pickerUri = session.pickerUri;
  if (pickerUri) {
    window.open(pickerUri, "_blank", "noopener,noreferrer");
  }

  try {
    const pollDefault = parseSeconds(
      session.pollingConfig && session.pollingConfig.pollInterval,
      2
    );
    const timeoutDefault = parseSeconds(
      session.pollingConfig && session.pollingConfig.timeoutIn,
      120
    );
    await pollSessionUntilReady(session.id, pollDefault, timeoutDefault);
    const data = await fetchPage(null);
    if (!data) return;
    state.pages = [
      {
        items: data.mediaItems || [],
        nextPageToken: data.nextPageToken || null,
      },
    ];
    state.pageIndex = 0;
    state.selected.clear();
    state.items = state.pages[0].items || [];
    selectAllCurrentItems();
    showPage(0);
    $("#media-status").textContent = `${state.items.length} ausgewählte Medien geladen.`;
  } catch (err) {
    $("#media-status").textContent = `Fehler: ${err.message || err}`;
  }
});

async function restoreLastPickerSession() {
  try {
    const r = await fetch("/api/picker/last-session");
    if (!r.ok) return;
    const data = await r.json();
    const sid = data && data.sessionId;
    if (!sid) return;
    state.pickerSessionId = sid;
    $("#media-status").textContent = "Lade letzte Picker-Auswahl…";
    const first = await fetchPage(null);
    if (!first) return;
    state.pages = [
      {
        items: first.mediaItems || [],
        nextPageToken: first.nextPageToken || null,
      },
    ];
    state.pageIndex = 0;
    state.selected.clear();
    state.items = state.pages[0].items || [];
    selectAllCurrentItems();
    showPage(0);
  } catch (_) {}
}

$("#next-page").addEventListener("click", async () => {
  const cur = state.pages[state.pageIndex];
  if (!cur || !cur.nextPageToken) return;
  let data;
  if (state.pages[state.pageIndex + 1]) {
    showPage(state.pageIndex + 1);
    return;
  }
  data = await fetchPage(cur.nextPageToken);
  if (!data) return;
  state.pages.push({
    items: data.mediaItems || [],
    nextPageToken: data.nextPageToken || null,
  });
  showPage(state.pages.length - 1);
});

$("#prev-page").addEventListener("click", () => {
  if (state.pageIndex <= 0) return;
  showPage(state.pageIndex - 1);
});

$("#filter-input").addEventListener("input", () => renderGrid());

$("#select-all").addEventListener("click", () => {
  for (const it of visibleItems()) {
    state.selected.set(itemKey(it), it);
  }
  renderGrid();
});

$("#clear-sel").addEventListener("click", () => {
  state.selected.clear();
  renderGrid();
});

$("#run-selected").addEventListener("click", async () => {
  $("#media-status").textContent = "Starte Verarbeitung...";
  const trimMethod = ($("#trim-method")?.value || "silence_balanced").toLowerCase();

  const selectedItems = Array.from(state.selected.values());
  const sourceItems = selectedItems.length ? selectedItems : visibleItems();
  const skippedNotReady = sourceItems.filter(
    (it) => itemProcessingStatus(it).toUpperCase() === "PROCESSING"
  ).length;
  const items = sourceItems
    .filter((it) => isStartableVideo(it))
    .map((it) => ({
    id: it.id,
    baseUrl: itemBaseUrl(it),
    filename: itemFilename(it),
    productUrl: it.productUrl || null,
    creationTime: null,
    processingStatus: itemProcessingStatus(it) || null,
  }));
  if (!items.length) {
    $("#media-status").textContent = "Keine startbaren Videos gefunden (noch nicht READY).";
    return;
  }
  const r = await fetch("/api/jobs/silence-remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items, trim_method: trimMethod }),
  });
  const j = await r.json();
  const localSkipped = Math.max(0, sourceItems.length - items.length);
  const notReadyInfo = skippedNotReady > 0 ? ` · nicht READY: ${skippedNotReady}` : "";
  setTab("jobs");
  loadJobs();
  const jobsStatus = $("#jobs-status");
  if (jobsStatus) {
    jobsStatus.textContent = `Verarbeitung (${trimMethod}): ${j.queued_job_ids.length} eingereiht, ${Math.max(j.skipped_media_ids.length, localSkipped)} übersprungen${notReadyInfo}`;
  }
});

async function pollVisibleCacheReady() {
  const list = visibleItems();
  for (const it of list) {
    const id = itemKey(it);
    if (!id) continue;
    const current = state.cacheStateById.get(id) || "pending";
    if (current === "ready" || current === "loading") continue;
    const st = await fetchCachedStatus(it);
    if (!st.ready) continue;
    setCacheState(id, "Lokal bereit (abspielbar)", "ready");
    setCacheProgress(id, 100);
    const video = document.querySelector(`video.preview[src*="media_item_id=${encodeURIComponent(id)}"]`);
    if (video) {
      video.src = cachedVideoUrl(it, true);
      video.load();
    }
  }
}

function startCachePolling() {
  if (state.cachePollTimer) return;
  state.cachePollTimer = setInterval(() => {
    pollVisibleCacheReady().catch(() => {});
  }, 3000);
}

async function loadJobs() {
  const r = await fetch("/api/jobs");
  const rows = await r.json();
  state.latestJobRow = rows.length ? rows[0] : null;
  const tb = $("#jobs-body");
  tb.innerHTML = "";
  const jobsStatus = $("#jobs-status");
  let runningCount = 0;
  let queuedCount = 0;
  let failedTransitions = 0;
  let doneTransitions = 0;

  for (const row of rows) {
    const prev = state.jobStatusById.get(row.id);
    if (prev && prev !== row.status) {
      if (row.status === "failed") failedTransitions += 1;
      if (row.status === "done") doneTransitions += 1;
    }
    state.jobStatusById.set(row.id, row.status);
    if (row.status === "running") runningCount += 1;
    if (row.status === "queued") queuedCount += 1;

    const tr = document.createElement("tr");
    const phaseLabel = row.phase_message || row.phase || "";
    const jobType = row.job_type || "clip_pipeline";
    const optionsSummary = parseJobOptionsSummary(row.job_options);
    const profExtra = renderProfileBadges(row.job_options);
    const methodBlock = `<div class="job-badges">${renderTrimMethodBadge(row.job_options)}</div>${
      profExtra ? `<div class="job-badges" style="margin-top:0.25rem">${profExtra}</div>` : ""
    }`;
    const statusBadge = renderStatusBadge(row.status);
    tr.innerHTML = `<td>${row.id}</td><td title="${escapeHtml(
      row.media_item_id || ""
    )}">${escapeHtml((row.filename || row.media_item_id || "").slice(0, 40))}</td><td>${renderJobTypeBadge(jobType)}</td><td>${methodBlock}<div class="muted" style="font-size:0.72rem;margin-top:0.15rem">${escapeHtml(optionsSummary)}</div></td><td>${statusBadge}</td><td>${escapeHtml(phaseLabel)}</td><td>${escapeHtml(formatProgress(row.progress))}</td><td>${escapeHtml(row.output_dir || "")}</td><td>${escapeHtml(
      row.error || ""
    )}</td>`;
    tb.appendChild(tr);
  }

  if (jobsStatus) {
    const active = runningCount + queuedCount;
    if (active > 0) {
      jobsStatus.textContent = `Aktiv: ${runningCount} running, ${queuedCount} queued`;
    } else {
      jobsStatus.textContent = "Keine aktiven Jobs.";
    }
    if (failedTransitions > 0) {
      jobsStatus.textContent += ` · ${failedTransitions} neuer Fehler`;
    } else if (doneTransitions > 0) {
      jobsStatus.textContent += ` · ${doneTransitions} fertig`;
    }
  }
}

$("#refresh-jobs").addEventListener("click", loadJobs);

async function copyLatestJob() {
  const row = state.latestJobRow;
  if (!row) {
    alert("Kein Job-Eintrag vorhanden.");
    return;
  }
  const payload = [
    `id=${row.id}`,
    `media_item_id=${row.media_item_id || ""}`,
    `filename=${row.filename || ""}`,
    `job_type=${row.job_type || ""}`,
    `job_options=${row.job_options || ""}`,
    `status=${row.status || ""}`,
    `phase=${row.phase || ""}`,
    `phase_message=${row.phase_message || ""}`,
    `progress=${formatProgress(row.progress) || ""}`,
    `output_dir=${row.output_dir || ""}`,
    `error=${row.error || ""}`,
  ].join("\n");

  try {
    await navigator.clipboard.writeText(payload);
  } catch (_) {
    const ta = document.createElement("textarea");
    ta.value = payload;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }
  const jobsStatus = $("#jobs-status");
  if (jobsStatus) jobsStatus.textContent = "Letzter Job in Zwischenablage kopiert.";
}

$("#copy-latest-job").addEventListener("click", copyLatestJob);

function startJobsPolling() {
  if (state.jobsPollTimer) return;
  state.jobsPollTimer = setInterval(async () => {
    try {
      await loadJobs();
    } catch (_) {}
  }, 2000);
}

function stopJobsPolling() {
  if (!state.jobsPollTimer) return;
  clearInterval(state.jobsPollTimer);
  state.jobsPollTimer = null;
}

async function loadGallery() {
  const r = await fetch("/api/gallery");
  const data = await r.json();
  const root = $("#gallery-root");
  root.innerHTML = "";
  for (const entry of data) {
    const el = document.createElement("div");
    el.className = "gallery-item";
    const src = entry.source || {};
    const link = src.productUrl
      ? `<p><a href="${escapeHtml(src.productUrl)}" target="_blank" rel="noopener">In Google Photos öffnen</a></p>`
      : "";
    let clipsHtml = "";
    for (const c of entry.clips || []) {
      const tv = c.transcript_url
        ? `<div class="transcript" data-url="${escapeHtml(c.transcript_url)}">…</div>`
        : "";
      clipsHtml += `<div class="clip-block">
        <p><strong>Clip ${c.index}</strong> (${c.begin_sec}s – ${c.finish_sec}s)</p>
        ${
          c.video_url
            ? `<video src="${escapeHtml(c.video_url)}" controls playsinline></video>`
            : ""
        }
        ${tv}
      </div>`;
    }
    el.innerHTML = `<h3>${escapeHtml(entry.folder)}</h3>
      <p class="muted">${escapeHtml(src.filename || "")} · ${escapeHtml(
      src.creationTime || ""
    )}</p>
      ${link}
      ${entry.error ? `<p class="muted">Fehler: ${escapeHtml(entry.error)}</p>` : ""}
      ${clipsHtml}`;
    root.appendChild(el);
  }
  root.querySelectorAll(".transcript[data-url]").forEach(async (node) => {
    const u = node.getAttribute("data-url");
    try {
      const t = await fetch(u);
      node.textContent = await t.text();
    } catch {
      node.textContent = "(Transkript konnte nicht geladen werden)";
    }
  });
}

document.querySelector('[data-tab="gallery"]').addEventListener("click", () => {
  loadGallery();
});

document.querySelector('[data-tab="jobs"]').addEventListener("click", () => {
  startJobsPolling();
  loadJobs();
});

document.querySelector('[data-tab="stats"]').addEventListener("click", () => {
  loadStats();
});

const refreshStats = $("#refresh-stats");
if (refreshStats) refreshStats.addEventListener("click", () => loadStats());

authStatus();
loadJobs();
restoreLastPickerSession();
startJobsPolling();
startCachePolling();
