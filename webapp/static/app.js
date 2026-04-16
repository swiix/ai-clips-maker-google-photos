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
  tinderClips: [],
  tinderIndex: 0,
  tinderLikes: new Map(),
  tinderDownloaded: new Map(),
  tinderLikeFilter: "all",
  tinderDecisions: new Map(),
  settingsClearDays: 30,
};
const videoRetryCountById = new Map();
const STORAGE_KEYS = {
  cutMergeGapSec: "ai_clips_cut_merge_gap_sec",
  cutMinDurationSec: "ai_clips_cut_min_duration_sec",
  noiseReductionMode: "ai_clips_noise_reduction_mode",
  trimMethod: "ai_clips_trim_method",
  tinderLikes: "ai_clips_tinder_likes",
  tinderDownloaded: "ai_clips_tinder_downloaded",
  tinderDecisions: "ai_clips_tinder_decisions",
};

function $(sel) {
  return document.querySelector(sel);
}

function isValidTabName(name) {
  if (!name) return false;
  return Boolean(document.querySelector(`.tab[data-tab="${name}"]`));
}

function tabFromUrl() {
  try {
    const url = new URL(window.location.href);
    const queryTab = (url.searchParams.get("tab") || "").trim();
    if (isValidTabName(queryTab)) return queryTab;
    const hashTab = String(window.location.hash || "").replace(/^#/, "").trim();
    if (isValidTabName(hashTab)) return hashTab;
  } catch (_) {}
  return null;
}

function syncUrlToTab(tabName, replaceHistory = false) {
  if (!isValidTabName(tabName)) return;
  try {
    const url = new URL(window.location.href);
    if (url.searchParams.get("tab") === tabName) return;
    url.searchParams.set("tab", tabName);
    const method = replaceHistory ? "replaceState" : "pushState";
    window.history[method]({}, "", url.toString());
  } catch (_) {}
}

function setTab(name, options = {}) {
  const { syncUrl = true, replaceHistory = false } = options;
  const safeName = isValidTabName(name) ? name : "sources";
  document.querySelectorAll(".tab").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === safeName);
  });
  document.querySelectorAll(".panel").forEach((p) => {
    p.classList.toggle("active", p.id === `panel-${safeName}`);
  });
  if (syncUrl) syncUrlToTab(safeName, replaceHistory);
}

function isTabActive(name) {
  return document.querySelector(`.tab[data-tab="${name}"]`)?.classList.contains("active");
}

function readJsonStorage(key, fallback) {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : fallback;
  } catch (_) {
    return fallback;
  }
}

function saveJsonStorage(key, data) {
  try {
    window.localStorage.setItem(key, JSON.stringify(data));
  } catch (_) {}
}

function setTinderwatchBadge(count) {
  const badge = $("#tinderwatch-unseen-badge");
  if (!badge) return;
  const n = Math.max(0, Number(count) || 0);
  badge.textContent = String(n);
  badge.classList.toggle("hidden", n <= 0);
}

function computeUnseenFromClips(clips) {
  let unseen = 0;
  for (const clip of clips || []) {
    if (!state.tinderDecisions.get(clip.key)) unseen += 1;
  }
  return unseen;
}

async function refreshTinderwatchBadgeFromServer() {
  try {
    // Badge can skip legacy orphan scan for fast updates.
    const r = await fetch("/api/gallery?include_orphans=0");
    if (!r.ok) return;
    const data = await r.json();
    const clips = flattenGalleryClips(data);
    setTinderwatchBadge(computeUnseenFromClips(clips));
  } catch (_) {}
}

function restoreCutTuningFromStorage() {
  const trimMethodSelect = $("#trim-method");
  if (trimMethodSelect) {
    const savedTrimMethod = (window.localStorage.getItem(STORAGE_KEYS.trimMethod) || "").toLowerCase();
    if (
      savedTrimMethod &&
      ["silence_conservative", "silence_balanced", "silence_aggressive", "openai_speech", "all_methods_testing"].includes(savedTrimMethod)
    ) {
      trimMethodSelect.value = savedTrimMethod;
    }
  }
  const gapInput = $("#openai-merge-gap-sec");
  const minInput = $("#openai-min-segment-sec");
  if (gapInput) {
    const savedGap = window.localStorage.getItem(STORAGE_KEYS.cutMergeGapSec);
    if (savedGap !== null && savedGap !== "") gapInput.value = savedGap;
  }
  if (minInput) {
    const savedMin = window.localStorage.getItem(STORAGE_KEYS.cutMinDurationSec);
    if (savedMin !== null && savedMin !== "") minInput.value = savedMin;
  }
  const modeSelect = $("#noise-reduction-mode");
  if (modeSelect) {
    const savedMode = (window.localStorage.getItem(STORAGE_KEYS.noiseReductionMode) || "").toLowerCase();
    if (savedMode && ["auto", "mild", "strong"].includes(savedMode)) {
      modeSelect.value = savedMode;
    }
  }
}

function persistCutTuningToStorage(gapSec, minDurationSec) {
  const normalizePositiveNumber = (value) => {
    if (value === null || value === undefined) return null;
    const normalized = String(value).trim().replace(",", ".");
    if (!normalized) return null;
    const parsed = Number(normalized);
    if (!Number.isFinite(parsed) || parsed <= 0) return null;
    return parsed;
  };
  try {
    const safeGap = normalizePositiveNumber(gapSec);
    if (safeGap !== null) {
      window.localStorage.setItem(STORAGE_KEYS.cutMergeGapSec, String(safeGap));
    }
    const safeMin = normalizePositiveNumber(minDurationSec);
    if (safeMin !== null) {
      window.localStorage.setItem(STORAGE_KEYS.cutMinDurationSec, String(safeMin));
    }
  } catch (_) {}
}

function parsePositiveTuningValue(value, fallback) {
  const normalized = String(value ?? "").trim().replace(",", ".");
  const parsed = Number(normalized);
  if (Number.isFinite(parsed) && parsed > 0) return parsed;
  return fallback;
}

function persistNoiseModeToStorage(mode) {
  const normalized = String(mode || "auto").toLowerCase();
  if (!["auto", "mild", "strong"].includes(normalized)) return;
  try {
    window.localStorage.setItem(STORAGE_KEYS.noiseReductionMode, normalized);
  } catch (_) {}
}

function persistTrimMethodToStorage(method) {
  const normalized = String(method || "").toLowerCase();
  if (!["silence_conservative", "silence_balanced", "silence_aggressive", "openai_speech", "all_methods_testing"].includes(normalized)) return;
  try {
    window.localStorage.setItem(STORAGE_KEYS.trimMethod, normalized);
  } catch (_) {}
}

function updateOpenAiTuningVisibility() {
  const box = $("#openai-tuning");
  if (!box) return;
  box.classList.remove("hidden");
}

document.querySelectorAll(".tab").forEach((b) => {
  b.addEventListener("click", () => setTab(b.dataset.tab, { syncUrl: true, replaceHistory: false }));
});
window.addEventListener("popstate", () => {
  const routeTab = tabFromUrl();
  if (routeTab) setTab(routeTab, { syncUrl: false });
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

function itemCreationTime(it) {
  return (
    (it.mediaFile &&
      it.mediaFile.mediaFileMetadata &&
      it.mediaFile.mediaFileMetadata.creationTime) ||
    it.creationTime ||
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

function formatSeconds(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

function formatPercent(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return `${Number(v).toLocaleString("de-DE", { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`;
}

function formatDateTime(value, fallbackEpochSeconds) {
  const asDate = value
    ? new Date(value)
    : (fallbackEpochSeconds ? new Date(Number(fallbackEpochSeconds) * 1000) : null);
  if (!asDate || Number.isNaN(asDate.getTime())) return "—";
  return asDate.toLocaleString("de-DE", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatRelativeFromEpoch(epochSeconds) {
  if (epochSeconds === null || epochSeconds === undefined || Number.isNaN(Number(epochSeconds))) return "";
  const tsMs = Number(epochSeconds) * 1000;
  const diffSec = Math.round((Date.now() - tsMs) / 1000);
  const abs = Math.abs(diffSec);
  const rtf = new Intl.RelativeTimeFormat("de-DE", { numeric: "auto" });
  if (abs < 60) return rtf.format(-Math.round(diffSec), "second");
  if (abs < 3600) return rtf.format(-Math.round(diffSec / 60), "minute");
  if (abs < 86400) return rtf.format(-Math.round(diffSec / 3600), "hour");
  if (abs < 2592000) return rtf.format(-Math.round(diffSec / 86400), "day");
  return rtf.format(-Math.round(diffSec / 2592000), "month");
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

function formatBytes(bytes) {
  const n = Number(bytes || 0);
  if (!Number.isFinite(n) || n <= 0) return "0 MB";
  const mb = n / (1024 * 1024);
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  return `${(mb / 1024).toFixed(2)} GB`;
}

function updateSettingsDaysButtons() {
  document.querySelectorAll(".settings-day-btn").forEach((btn) => {
    btn.classList.toggle("active", Number(btn.dataset.days) === state.settingsClearDays);
  });
}

async function loadSettingsCacheSummary() {
  const status = $("#settings-status");
  try {
    const r = await fetch("/api/cache/summary");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const by = data.by_type || {};
    const videosBytes = Number((by.videos && by.videos.bytes) || 0);
    const imagesBytes = Number((by.images && by.images.bytes) || 0);
    const audioBytes = Number((by.audio && by.audio.bytes) || 0);
    const otherBytes = Number((by.other_files && by.other_files.bytes) || 0);
    const totalBytes = Number(data.total_bytes || 0);
    $("#settings-total-size").textContent = formatBytes(totalBytes);
    $("#settings-videos-size").textContent = formatBytes(videosBytes);
    $("#settings-images-size").textContent = formatBytes(imagesBytes);
    $("#settings-audio-size").textContent = formatBytes(audioBytes);
    $("#settings-other-size").textContent = formatBytes(otherBytes);
    const total = Math.max(totalBytes, 1);
    const videosPct = Math.round((videosBytes * 100) / total);
    const imagesPct = Math.round((imagesBytes * 100) / total);
    const audioPct = Math.round((audioBytes * 100) / total);
    const otherPct = Math.max(0, 100 - videosPct - imagesPct - audioPct);
    const ring = $("#settings-cache-ring");
    if (ring) {
      ring.style.background = `conic-gradient(
        #5b4bdb 0 ${videosPct}%,
        #4aa3ff ${videosPct}% ${videosPct + imagesPct}%,
        #55d39a ${videosPct + imagesPct}% ${videosPct + imagesPct + audioPct}%,
        #9aa0a6 ${videosPct + imagesPct + audioPct}% ${videosPct + imagesPct + audioPct + otherPct}%
      )`;
    }
    if (status) status.textContent = `Cache-Dateien: ${Number(data.total_files || 0)}`;
  } catch (err) {
    if (status) status.textContent = `Cache-Statistik konnte nicht geladen werden: ${err.message || err}`;
  }
}

async function clearSettingsCacheAdvanced() {
  const status = $("#settings-status");
  const payload = {
    older_than_days: state.settingsClearDays,
    images: Boolean($("#settings-clear-images")?.checked),
    videos: Boolean($("#settings-clear-videos")?.checked),
    audio: Boolean($("#settings-clear-audio")?.checked),
    other_files: Boolean($("#settings-clear-other")?.checked),
  };
  if (!payload.images && !payload.videos && !payload.audio && !payload.other_files) {
    if (status) status.textContent = "Bitte mindestens einen Dateityp auswählen.";
    return;
  }
  if (status) status.textContent = "Lösche Cache-Dateien...";
  try {
    const r = await fetch("/api/cache/clear-advanced", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (status) {
      status.textContent = `Entfernt: ${Number(data.removed_files || 0)} · Übersprungen (zu neu): ${Number(data.skipped_recent_files || 0)} · Fehler: ${Number(data.failed_files || 0)}`;
    }
    await loadSettingsCacheSummary();
  } catch (err) {
    if (status) status.textContent = `Löschen fehlgeschlagen: ${err.message || err}`;
  }
}

async function clearSettingsCacheAll() {
  const status = $("#settings-status");
  if (status) status.textContent = "Lösche gesamten Cache...";
  try {
    const r = await fetch("/api/cache/clear", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (status) status.textContent = `Alle Cache-Dateien entfernt: ${Number(data.removed_files || 0)} (Fehler: ${Number(data.failed_files || 0)})`;
    await loadSettingsCacheSummary();
  } catch (err) {
    if (status) status.textContent = `Clear All fehlgeschlagen: ${err.message || err}`;
  }
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

async function openJobVideo(jobId) {
  const jobsStatus = $("#jobs-status");
  try {
    const r = await fetch(`/api/jobs/${jobId}/latest-video`);
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || "Kein Video verfügbar");
    }
    const data = await r.json();
    if (!data.video_url) throw new Error("Kein Video verfügbar");
    window.open(data.video_url, "_blank", "noopener");
    if (jobsStatus) jobsStatus.textContent = `Öffne Video: ${data.filename || "preview.mp4"}`;
  } catch (err) {
    if (jobsStatus) jobsStatus.textContent = `Play fehlgeschlagen: ${err.message || "Unbekannter Fehler"}`;
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
$("#trim-method").addEventListener("change", () => {
  updateOpenAiTuningVisibility();
  persistTrimMethodToStorage($("#trim-method")?.value);
});
const mergeGapInput = $("#openai-merge-gap-sec");
const minDurationInput = $("#openai-min-segment-sec");
const persistCurrentCutTuning = () => {
  persistCutTuningToStorage(mergeGapInput?.value, minDurationInput?.value);
};
if (mergeGapInput) {
  mergeGapInput.addEventListener("change", persistCurrentCutTuning);
  mergeGapInput.addEventListener("blur", persistCurrentCutTuning);
}
if (minDurationInput) {
  minDurationInput.addEventListener("change", persistCurrentCutTuning);
  minDurationInput.addEventListener("blur", persistCurrentCutTuning);
}

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
  const cutMergeGapSec = parsePositiveTuningValue($("#openai-merge-gap-sec")?.value, 0.35);
  const cutMinDurationSec = parsePositiveTuningValue($("#openai-min-segment-sec")?.value, 0.04);
  const noiseReductionEnabled = Boolean($("#noise-reduction-enabled")?.checked);
  const noiseReductionMode = ($("#noise-reduction-mode")?.value || "auto").toLowerCase();
  persistCutTuningToStorage(cutMergeGapSec, cutMinDurationSec);
  persistNoiseModeToStorage(noiseReductionMode);
  persistTrimMethodToStorage(trimMethod);

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
    creationTime: itemCreationTime(it),
    processingStatus: itemProcessingStatus(it) || null,
  }));
  if (!items.length) {
    $("#media-status").textContent = "Keine startbaren Videos gefunden (noch nicht READY).";
    return;
  }
  let queuedCount = 0;
  let skippedCount = 0;
  if (trimMethod === "all_methods_testing") {
    const methods = [
      "silence_conservative",
      "silence_balanced",
      "silence_aggressive",
      "openai_speech",
    ];
    for (const method of methods) {
      const taggedItems = items.map((it) => ({
        ...it,
        // Dedicated synthetic IDs allow 4 parallel jobs per source video.
        id: `${method}__${it.id}`,
      }));
      const r = await fetch("/api/jobs/silence-remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: taggedItems,
          trim_method: method,
          cut_merge_gap_sec: cutMergeGapSec,
          cut_min_duration_sec: cutMinDurationSec,
          noise_reduction: noiseReductionEnabled,
          noise_reduction_mode: noiseReductionMode,
        }),
      });
      const j = await r.json();
      queuedCount += Number((j.queued_job_ids || []).length || 0);
      skippedCount += Number((j.skipped_media_ids || []).length || 0);
    }
  } else {
    const r = await fetch("/api/jobs/silence-remove", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        items,
        trim_method: trimMethod,
        cut_merge_gap_sec: cutMergeGapSec,
        cut_min_duration_sec: cutMinDurationSec,
        noise_reduction: noiseReductionEnabled,
        noise_reduction_mode: noiseReductionMode,
      }),
    });
    const j = await r.json();
    queuedCount = Number((j.queued_job_ids || []).length || 0);
    skippedCount = Number((j.skipped_media_ids || []).length || 0);
  }
  const localSkipped = Math.max(0, sourceItems.length - items.length);
  const notReadyInfo = skippedNotReady > 0 ? ` · nicht READY: ${skippedNotReady}` : "";
  setTab("jobs");
  loadJobs();
  const jobsStatus = $("#jobs-status");
  if (jobsStatus) {
    jobsStatus.textContent = `Verarbeitung (${trimMethod}, Noise Reduction: ${noiseReductionEnabled ? `an (${noiseReductionMode})` : "aus"}): ${queuedCount} eingereiht, ${Math.max(skippedCount, localSkipped)} übersprungen${notReadyInfo}`;
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
  let rows = await r.json();
  const sortKey = ($("#jobs-sort")?.value || "updated_desc").toLowerCase();
  const minSavedSec = Number($("#jobs-min-saved-sec")?.value || "");
  const minSavedPct = Number($("#jobs-min-saved-pct")?.value || "");

  rows = rows.filter((row) => {
    const savedSec = Number(row.cut_saved_seconds || 0);
    const savedPct = Number(row.cut_saved_percent || 0);
    if (!Number.isNaN(minSavedSec) && $("#jobs-min-saved-sec")?.value !== "" && savedSec < minSavedSec) {
      return false;
    }
    if (!Number.isNaN(minSavedPct) && $("#jobs-min-saved-pct")?.value !== "" && savedPct < minSavedPct) {
      return false;
    }
    return true;
  });

  const num = (v) => (v === null || v === undefined || Number.isNaN(Number(v)) ? Number.NEGATIVE_INFINITY : Number(v));
  rows.sort((a, b) => {
    const createdMs = (row) => {
      if (row.creation_time) {
        const t = Date.parse(row.creation_time);
        if (!Number.isNaN(t)) return t;
      }
      return Number(row.created_at || 0) * 1000;
    };
    if (sortKey === "created_desc") return createdMs(b) - createdMs(a);
    if (sortKey === "created_asc") return createdMs(a) - createdMs(b);
    if (sortKey === "duration_desc") return num(b.cut_input_seconds) - num(a.cut_input_seconds);
    if (sortKey === "duration_asc") return num(a.cut_input_seconds) - num(b.cut_input_seconds);
    if (sortKey === "saved_sec_desc") return num(b.cut_saved_seconds) - num(a.cut_saved_seconds);
    if (sortKey === "saved_sec_asc") return num(a.cut_saved_seconds) - num(b.cut_saved_seconds);
    if (sortKey === "saved_pct_desc") return num(b.cut_saved_percent) - num(a.cut_saved_percent);
    if (sortKey === "saved_pct_asc") return num(a.cut_saved_percent) - num(b.cut_saved_percent);
    const au = Number(a.updated_at || 0);
    const bu = Number(b.updated_at || 0);
    if (sortKey === "updated_asc") return au - bu;
    return bu - au;
  });

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
    const hasOutputDir = Boolean(row.output_dir);
    const playBtn = row.status === "done"
      ? `<button type="button" class="btn job-play-btn" data-play-job-id="${row.id}">▶ Play</button>`
      : "";
    const folderCell = `<div class="job-folder-cell">${playBtn}</div>${
      hasOutputDir ? `<div class="muted" style="font-size:0.72rem;margin-top:0.2rem">${escapeHtml(row.output_dir || "")}</div>` : ""
    }`;
    const relUpdated = formatRelativeFromEpoch(row.updated_at);
    const durationCell = `<span title="${escapeHtml(relUpdated)}">${escapeHtml(formatSeconds(row.cut_input_seconds))}</span>`;
    const savedCell = `<span title="${escapeHtml(relUpdated)}">${escapeHtml(formatSeconds(row.cut_saved_seconds))}</span>`;
    const createdCell = escapeHtml(formatDateTime(row.creation_time, row.created_at));
    tr.innerHTML = `<td>${folderCell}</td><td>${durationCell}</td><td>${savedCell}</td><td>${escapeHtml(formatPercent(row.cut_saved_percent))}</td><td>${createdCell}</td><td>${row.id}</td><td title="${escapeHtml(
      row.media_item_id || ""
    )}">${escapeHtml((row.filename || row.media_item_id || "").slice(0, 40))}</td><td>${renderJobTypeBadge(jobType)}</td><td>${methodBlock}<div class="muted" style="font-size:0.72rem;margin-top:0.15rem">${escapeHtml(optionsSummary)}</div></td><td>${statusBadge}</td><td>${escapeHtml(phaseLabel)}</td><td>${escapeHtml(formatProgress(row.progress))}</td><td>${escapeHtml(
      row.error || ""
    )}</td>`;
    tb.appendChild(tr);
  }

  tb.querySelectorAll("button[data-play-job-id]").forEach((btn) => {
    btn.addEventListener("click", () => openJobVideo(btn.dataset.playJobId));
  });

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
      refreshTinderwatchBadgeFromServer();
    }
  }
}

async function loadCutsView() {
  const status = $("#cuts-status");
  const root = $("#cuts-list");
  if (!root) return;
  const r = await fetch("/api/jobs");
  const rows = await r.json();
  const done = rows
    .filter((x) => x.status === "done" && Number(x.cut_saved_seconds || 0) > 0)
    .sort((a, b) => Number(b.cut_saved_seconds || 0) - Number(a.cut_saved_seconds || 0));
  root.innerHTML = "";
  if (!done.length) {
    root.innerHTML = `<div class="muted">Noch keine erkannten Schnitt-Metriken vorhanden.</div>`;
    if (status) status.textContent = "Schnitt-Metriken pro Job (DB, sonst Dateiname-Fallback).";
    return;
  }
  for (const row of done) {
    const card = document.createElement("div");
    card.className = "cut-card";
    const source = row.cut_metrics_source === "filename" ? "Dateiname-Fallback" : "Datenbank";
    card.innerHTML = `<div class="cut-card-title">${escapeHtml(row.filename || row.media_item_id || "")}</div>
      <div class="cut-card-meta">Job #${row.id} · ${escapeHtml(row.job_type || "")} · Quelle: ${escapeHtml(source)}</div>
      <div class="cut-card-grid">
        <div><span class="muted">Vorher</span><strong>${formatSeconds(row.cut_input_seconds)} s</strong></div>
        <div><span class="muted">Nachher</span><strong>${formatSeconds(row.cut_output_seconds)} s</strong></div>
        <div><span class="muted">Eingespart</span><strong>${formatSeconds(row.cut_saved_seconds)} s</strong></div>
        <div><span class="muted">Geschnitten</span><strong>${formatPercent(row.cut_saved_percent)}</strong></div>
      </div>`;
    root.appendChild(card);
  }
  if (status) status.textContent = `${done.length} Jobs mit erkannten Schnitt-Metriken.`;
}

$("#refresh-jobs").addEventListener("click", loadJobs);
$("#jobs-sort").addEventListener("change", loadJobs);
$("#jobs-min-saved-sec").addEventListener("input", loadJobs);
$("#jobs-min-saved-pct").addEventListener("input", loadJobs);

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

async function retryFailedCachedJobs() {
  const jobsStatus = $("#jobs-status");
  if (jobsStatus) jobsStatus.textContent = "Pruefe fehlgeschlagene Jobs mit lokalem Cache...";
  try {
    const r = await fetch("/api/jobs/retry-failed-cached", { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    const retried = Number((data.retried_job_ids || []).length || 0);
    const noCache = Number((data.skipped_no_cache_ids || []).length || 0);
    const notRq = Number((data.skipped_not_requeueable_ids || []).length || 0);
    if (jobsStatus) {
      jobsStatus.textContent = `Neu gestartet: ${retried} · Ohne Cache: ${noCache} · Nicht requeuebar: ${notRq}`;
    }
    await loadJobs();
  } catch (err) {
    if (jobsStatus) jobsStatus.textContent = `Neu starten fehlgeschlagen: ${err.message || "Unbekannter Fehler"}`;
  }
}

$("#copy-latest-job").addEventListener("click", copyLatestJob);
const retryFailedCachedBtn = $("#retry-failed-cached-jobs");
if (retryFailedCachedBtn) retryFailedCachedBtn.addEventListener("click", retryFailedCachedJobs);

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
  const r = await fetch("/api/gallery?include_orphans=1");
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

function loadTinderStateFromStorage() {
  state.tinderLikes = new Map(Object.entries(readJsonStorage(STORAGE_KEYS.tinderLikes, {})));
  state.tinderDownloaded = new Map(Object.entries(readJsonStorage(STORAGE_KEYS.tinderDownloaded, {})));
  state.tinderDecisions = new Map(Object.entries(readJsonStorage(STORAGE_KEYS.tinderDecisions, {})));
}

function persistTinderState() {
  saveJsonStorage(STORAGE_KEYS.tinderLikes, Object.fromEntries(state.tinderLikes.entries()));
  saveJsonStorage(STORAGE_KEYS.tinderDownloaded, Object.fromEntries(state.tinderDownloaded.entries()));
  saveJsonStorage(STORAGE_KEYS.tinderDecisions, Object.fromEntries(state.tinderDecisions.entries()));
}

async function upsertTinderReviewOnServer(clip, patch = {}) {
  if (!clip || !clip.key) return;
  const payload = {
    clip_key: clip.key,
    job_id: Number.isInteger(Number(clip.jobId)) ? Number(clip.jobId) : null,
    media_item_id: clip.mediaItemId || null,
    trim_mode: clip.trimMode || "unknown",
    source_filename: clip.sourceFilename || "",
    folder: clip.folder || "",
    video_url: clip.video_url || "",
    begin_sec: Number.isFinite(Number(clip.begin_sec)) ? Number(clip.begin_sec) : null,
    finish_sec: Number.isFinite(Number(clip.finish_sec)) ? Number(clip.finish_sec) : null,
    ...patch,
  };
  try {
    await fetch("/api/tinder/reviews", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (_) {}
}

async function syncLocalTinderStateToServer(serverRows) {
  const known = new Set((serverRows || []).map((r) => String(r.clip_key || "")));
  for (const [key, review] of state.tinderDecisions.entries()) {
    if (known.has(key)) continue;
    const clip = {
      key,
      jobId: review.jobId || null,
      mediaItemId: review.mediaItemId || null,
      trimMode: review.trimMode || "unknown",
      sourceFilename: review.sourceFilename || "",
      folder: review.folder || "",
      video_url: review.video_url || "",
      begin_sec: review.begin_sec,
      finish_sec: review.finish_sec,
    };
    const downloaded = Boolean(state.tinderDownloaded.get(key)?.downloaded);
    await upsertTinderReviewOnServer(clip, { decision: review.decision, downloaded });
  }
  for (const [key, like] of state.tinderLikes.entries()) {
    if (known.has(key) || state.tinderDecisions.has(key)) continue;
    const clip = {
      key,
      jobId: like.jobId || null,
      mediaItemId: like.mediaItemId || null,
      trimMode: like.trimMode || "unknown",
      sourceFilename: like.sourceFilename || "",
      folder: like.folder || "",
      video_url: like.video_url || "",
      begin_sec: like.begin_sec,
      finish_sec: like.finish_sec,
    };
    const downloaded = Boolean(state.tinderDownloaded.get(key)?.downloaded);
    await upsertTinderReviewOnServer(clip, { decision: "like", downloaded });
  }
}

function applyServerTinderReviews(rows) {
  for (const row of rows || []) {
    const key = String(row.clip_key || "");
    if (!key) continue;
    const decision = String(row.decision || "").toLowerCase();
    const downloaded = Boolean(Number(row.downloaded || 0));
    if (decision === "like") {
      state.tinderLikes.set(key, {
        key,
        jobId: row.job_id || null,
        mediaItemId: row.media_item_id || null,
        folder: row.folder || "",
        sourceFilename: row.source_filename || "",
        index: 0,
        begin_sec: row.begin_sec ?? 0,
        finish_sec: row.finish_sec ?? 0,
        video_url: row.video_url || "",
        liked_at: new Date(Number(row.updated_at || Date.now() / 1000) * 1000).toISOString(),
      });
    }
    if (decision === "like" || decision === "dislike") {
      state.tinderDecisions.set(key, {
        key,
        decision,
        jobId: row.job_id || null,
        mediaItemId: row.media_item_id || null,
        trimMode: row.trim_mode || "unknown",
        sourceFilename: row.source_filename || "",
        folder: row.folder || "",
        decided_at: new Date(Number(row.updated_at || Date.now() / 1000) * 1000).toISOString(),
      });
    }
    if (downloaded) {
      state.tinderDownloaded.set(key, {
        downloaded: true,
        downloaded_at: new Date(Number(row.updated_at || Date.now() / 1000) * 1000).toISOString(),
      });
    }
  }
}

async function hydrateTinderStateFromServer() {
  try {
    const r = await fetch("/api/tinder/reviews");
    if (!r.ok) return;
    const rows = await r.json();
    await syncLocalTinderStateToServer(rows);
    applyServerTinderReviews(rows);
    persistTinderState();
  } catch (_) {}
}

function detectTrimMode(folder, filename) {
  const hay = `${String(folder || "").toLowerCase()} ${String(filename || "").toLowerCase()}`;
  if (hay.includes("openai_speech") || hay.includes("openai")) return "openai_speech";
  if (hay.includes("silence_conservative") || hay.includes("conservative")) return "silence_conservative";
  if (hay.includes("silence_balanced") || hay.includes("balanced")) return "silence_balanced";
  if (hay.includes("silence_aggressive") || hay.includes("aggressive")) return "silence_aggressive";
  return "unknown";
}

function trimModeLabelDe(mode) {
  const key = String(mode || "").toLowerCase();
  if (key === "openai_speech") return "OpenAI Speech";
  if (key === "silence_conservative") return "Silence Conservative";
  if (key === "silence_balanced") return "Silence Balanced";
  if (key === "silence_aggressive") return "Silence Aggressive";
  return "Unbekannt";
}

function parseDurationTagsFromText(text) {
  const m = String(text || "").match(/_(\d+(?:d\d+)?)s_to_(\d+(?:d\d+)?)s_/i);
  if (!m) return null;
  const parseTag = (v) => {
    const s = String(v || "").toLowerCase();
    if (s.includes("d")) return Number(s.replace("d", "."));
    return Number(s);
  };
  const before = parseTag(m[1]);
  const after = parseTag(m[2]);
  if (!Number.isFinite(before) || !Number.isFinite(after)) return null;
  return { before, after };
}

function getClipDurations(clip) {
  const urlDur = parseDurationTagsFromText(clip?.video_url || "");
  if (urlDur) return urlDur;
  const srcDur = parseDurationTagsFromText(clip?.sourceFilename || "");
  if (srcDur) return srcDur;
  return null;
}

function flattenGalleryClips(entries) {
  const clips = [];
  for (const entry of entries || []) {
    const sourceType = entry.folder === "legacy_outputs" ? "legacy" : "jobs";
    for (const clip of entry.clips || []) {
      if (!clip.video_url) continue;
      clips.push({
        key: `${entry.folder || "folder"}::${clip.index || 0}::${clip.video_url}`,
        sourceType,
        jobId: Number((entry.source && entry.source.jobId) || null) || null,
        mediaItemId:
          (entry.source && (entry.source.mediaItemId || entry.source.media_item_id)) || null,
        folder: entry.folder || "",
        sourceFilename: (entry.source && entry.source.filename) || "",
        trimMode: detectTrimMode(entry.folder, (entry.source && entry.source.filename) || ""),
        index: clip.index,
        begin_sec: clip.begin_sec,
        finish_sec: clip.finish_sec,
        video_url: clip.video_url,
        transcript_url: clip.transcript_url || null,
      });
    }
  }
  return clips;
}

function getCurrentTinderClip() {
  if (!state.tinderClips.length) return null;
  return state.tinderClips[state.tinderIndex] || null;
}

function markTinderLiked(clip) {
  if (!clip) return;
  state.tinderLikes.set(clip.key, {
    key: clip.key,
    jobId: clip.jobId || null,
    mediaItemId: clip.mediaItemId || null,
    folder: clip.folder,
    sourceFilename: clip.sourceFilename,
    index: clip.index,
    begin_sec: clip.begin_sec,
    finish_sec: clip.finish_sec,
    video_url: clip.video_url,
    liked_at: new Date().toISOString(),
  });
  persistTinderState();
  upsertTinderReviewOnServer(clip, { decision: "like" });
}

function markTinderDecision(clip, decision) {
  if (!clip || (decision !== "like" && decision !== "dislike")) return;
  state.tinderDecisions.set(clip.key, {
    key: clip.key,
    jobId: clip.jobId || null,
    mediaItemId: clip.mediaItemId || null,
    decision,
    trimMode: clip.trimMode || "unknown",
    sourceFilename: clip.sourceFilename || "",
    folder: clip.folder || "",
    decided_at: new Date().toISOString(),
  });
  persistTinderState();
  upsertTinderReviewOnServer(clip, { decision });
}

function markTinderDownloaded(clip) {
  if (!clip) return;
  state.tinderDownloaded.set(clip.key, {
    downloaded: true,
    downloaded_at: new Date().toISOString(),
  });
  persistTinderState();
  upsertTinderReviewOnServer(clip, { downloaded: true });
}

function triggerClipDownload(clip) {
  if (!clip || !clip.video_url) return;
  const a = document.createElement("a");
  a.href = clip.video_url;
  a.download = `${clip.folder || "clip"}_${clip.index || 0}.mp4`;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  markTinderDownloaded(clip);
}

function renderTinderLikesList() {
  const root = $("#tinder-likes-list");
  if (!root) return;
  const likes = Array.from(state.tinderLikes.values())
    .filter((like) => {
      const downloaded = Boolean(state.tinderDownloaded.get(like.key)?.downloaded);
      if (state.tinderLikeFilter === "downloaded") return downloaded;
      if (state.tinderLikeFilter === "not_downloaded") return !downloaded;
      return true;
    })
    .sort((a, b) => String(b.liked_at || "").localeCompare(String(a.liked_at || "")));
  if (!likes.length) {
    root.innerHTML = `<p class="muted">Noch keine Likes.</p>`;
    return;
  }
  root.innerHTML = "";
  for (const like of likes) {
    const downloaded = Boolean(state.tinderDownloaded.get(like.key)?.downloaded);
    const row = document.createElement("div");
    row.className = "tinder-like-item";
    row.innerHTML = `<div><strong>#${escapeHtml(String(like.index || 0))}</strong> ${escapeHtml(like.sourceFilename || like.folder || "")}</div>
      <div class="muted">${escapeHtml(formatSeconds(like.begin_sec))}s - ${escapeHtml(formatSeconds(like.finish_sec))}s</div>
      <div class="tinder-like-actions">
        <span class="tinder-download-state ${downloaded ? "done" : "todo"}">${downloaded ? "Downloaded" : "Offen"}</span>
        <button type="button" class="btn" data-tinder-download-key="${escapeHtml(like.key)}">Download</button>
      </div>`;
    root.appendChild(row);
  }
  root.querySelectorAll("button[data-tinder-download-key]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-tinder-download-key");
      const clip = state.tinderLikes.get(key);
      if (!clip) return;
      triggerClipDownload(clip);
      renderTinderLikesList();
      updateTinderStatus();
    });
  });
}

function renderTinderStats() {
  const root = $("#tinder-stats-root");
  if (!root) return;
  const decisions = Array.from(state.tinderDecisions.values());
  const likes = decisions.filter((d) => d.decision === "like").length;
  const dislikes = decisions.filter((d) => d.decision === "dislike").length;
  const total = likes + dislikes;
  const likePct = total > 0 ? Math.round((likes * 100) / total) : 0;
  const ring = `conic-gradient(#27d7a0 0 ${likePct}%, #ff6a95 ${likePct}% 100%)`;

  const modes = ["silence_conservative", "silence_balanced", "silence_aggressive", "openai_speech", "unknown"];
  const labels = {
    silence_conservative: "Silence Conservative",
    silence_balanced: "Silence Balanced",
    silence_aggressive: "Silence Aggressive",
    openai_speech: "OpenAI Speech",
    unknown: "Unknown",
  };
  const rows = modes
    .map((mode) => {
      const subset = decisions.filter((d) => (d.trimMode || "unknown") === mode);
      const mLikes = subset.filter((d) => d.decision === "like").length;
      const mDislikes = subset.filter((d) => d.decision === "dislike").length;
      const mTotal = mLikes + mDislikes;
      const mLikePct = mTotal > 0 ? Math.round((mLikes * 100) / mTotal) : 0;
      return { mode, mLikes, mDislikes, mTotal, mLikePct };
    })
    .filter((x) => x.mTotal > 0);

  const best = rows.length ? rows.slice().sort((a, b) => b.mLikePct - a.mLikePct || b.mTotal - a.mTotal)[0] : null;
  root.innerHTML = `
    <div class="tinder-stats-grid">
      <div class="tinder-stat-card">
        <div class="tinder-ring" style="background:${ring}">
          <div class="tinder-ring-inner">${likePct}%<span>Like Rate</span></div>
        </div>
        <div class="tinder-stat-legend">
          <div><span class="dot like"></span>Likes: <strong>${likes}</strong></div>
          <div><span class="dot dislike"></span>Dislikes: <strong>${dislikes}</strong></div>
          <div class="muted">Bewertet gesamt: ${total}</div>
        </div>
      </div>
      <div class="tinder-stat-card">
        <div class="tinder-mode-title">Like-Rate nach Cutting-Modus</div>
        <div class="tinder-mode-bars">
          ${
            rows.length
              ? rows
                  .map(
                    (r) => `<div class="tinder-mode-row">
                <div class="tinder-mode-label">${labels[r.mode] || r.mode}</div>
                <div class="tinder-mode-track">
                  <div class="tinder-mode-like" style="width:${r.mLikePct}%"></div>
                </div>
                <div class="tinder-mode-value">${r.mLikePct}% (${r.mLikes}/${r.mTotal})</div>
              </div>`
                  )
                  .join("")
              : `<div class="muted">Noch keine Bewertungen vorhanden.</div>`
          }
        </div>
        ${
          best
            ? `<div class="tinder-best-mode">Bester Modus aktuell: <strong>${labels[best.mode] || best.mode}</strong> (${best.mLikePct}% Like-Rate)</div>`
            : ""
        }
      </div>
    </div>
  `;
}

function updateTinderLikeFilterButton() {
  const btn = $("#tinder-like-filter-toggle");
  if (!btn) return;
  if (state.tinderLikeFilter === "downloaded") {
    btn.textContent = "Filter: Downloaded";
  } else if (state.tinderLikeFilter === "not_downloaded") {
    btn.textContent = "Filter: Offen";
  } else {
    btn.textContent = "Filter: Alle";
  }
}

function cycleTinderLikeFilter() {
  if (state.tinderLikeFilter === "all") {
    state.tinderLikeFilter = "downloaded";
  } else if (state.tinderLikeFilter === "downloaded") {
    state.tinderLikeFilter = "not_downloaded";
  } else {
    state.tinderLikeFilter = "all";
  }
  updateTinderLikeFilterButton();
  renderTinderLikesList();
}

function updateTinderStatus() {
  const el = $("#tinder-status");
  if (!el) return;
  const total = state.tinderClips.length;
  const idx = total > 0 ? state.tinderIndex + 1 : 0;
  let jobsClips = 0;
  let legacyClips = 0;
  for (const clip of state.tinderClips) {
    if (clip.sourceType === "legacy") legacyClips += 1;
    else jobsClips += 1;
  }
  const likes = state.tinderLikes.size;
  let downloadedLikes = 0;
  for (const key of state.tinderLikes.keys()) {
    if (state.tinderDownloaded.get(key)?.downloaded) downloadedLikes += 1;
  }
  el.textContent = `${idx}/${total} Clips · Jobs: ${jobsClips} · Legacy: ${legacyClips} · Likes: ${likes} · Downloads: ${downloadedLikes}/${likes}`;
}

function bindTinderVideoState(root) {
  const video = root.querySelector(".tinder-video");
  const status = root.querySelector("#tinder-player-state");
  if (!video || !status) return;
  const setState = (text) => {
    status.textContent = `Player: ${text}`;
  };
  setState("laedt...");
  video.addEventListener("loadstart", () => setState("laedt..."));
  video.addEventListener("loadeddata", () => setState("bereit"));
  video.addEventListener("canplay", () => setState("abspielbar"));
  video.addEventListener("playing", () => setState("spielt"));
  video.addEventListener("waiting", () => setState("buffering..."));
  video.addEventListener("stalled", () => setState("verzoegert"));
  video.addEventListener("error", () => setState("fehler"));
}

function renderTinderCard() {
  const root = $("#tinder-card");
  if (!root) return;
  const clip = getCurrentTinderClip();
  if (!clip) {
    root.innerHTML = `<div class="tinder-empty">Keine Clips gefunden.</div>`;
    updateTinderStatus();
    renderTinderLikesList();
    return;
  }
  const durations = getClipDurations(clip);
  const beforeAfterText = durations
    ? `${escapeHtml(formatSeconds(durations.before))}s / ${escapeHtml(formatSeconds(durations.after))}s`
    : "n/a";
  root.innerHTML = `<div class="tinder-chip">Clip #${escapeHtml(String(clip.index || 0))}</div>
    <video class="tinder-video" src="${escapeHtml(clip.video_url)}" controls playsinline autoplay loop></video>
    <div class="tinder-meta">
      <div class="tinder-title">${escapeHtml(clip.sourceFilename || clip.folder || "Clip")}</div>
      <div id="tinder-player-state" class="tinder-player-state">Player: laedt...</div>
      <div class="tinder-meta-grid">
        <div class="tinder-meta-item">
          <span class="tinder-meta-label">Cutting-Modus</span>
          <span class="tinder-meta-value">${escapeHtml(trimModeLabelDe(clip.trimMode))}</span>
        </div>
        <div class="tinder-meta-item">
          <span class="tinder-meta-label">Dauer vorher/nachher</span>
          <span class="tinder-meta-value">${beforeAfterText}</span>
        </div>
        <div class="tinder-meta-item tinder-meta-item-wide">
          <span class="tinder-meta-label">Clip-Segment</span>
          <span class="tinder-meta-value">${escapeHtml(formatSeconds(clip.begin_sec))}s - ${escapeHtml(formatSeconds(clip.finish_sec))}s</span>
        </div>
      </div>
    </div>`;
  bindTinderVideoState(root);
  updateTinderStatus();
  renderTinderLikesList();
  renderTinderStats();
  setTinderwatchBadge(computeUnseenFromClips(state.tinderClips));
}

function tinderNext() {
  if (!state.tinderClips.length) return;
  state.tinderIndex = (state.tinderIndex + 1) % state.tinderClips.length;
  renderTinderCard();
}

function tinderDislike() {
  const clip = getCurrentTinderClip();
  if (clip) markTinderDecision(clip, "dislike");
  tinderNext();
}

function tinderLike() {
  const clip = getCurrentTinderClip();
  if (!clip) return;
  markTinderLiked(clip);
  markTinderDecision(clip, "like");
  tinderNext();
}

function downloadAllLikedClips() {
  const likes = Array.from(state.tinderLikes.values());
  if (!likes.length) {
    alert("Noch keine Likes vorhanden.");
    return;
  }
  const notDownloaded = likes.filter((clip) => !state.tinderDownloaded.get(clip.key)?.downloaded);
  let batch = notDownloaded;
  if (!notDownloaded.length) {
    const repeat = window.confirm("Alle gelikten Clips wurden bereits geladen. Nochmal alle herunterladen?");
    if (!repeat) return;
    batch = likes;
  }
  for (const clip of batch) triggerClipDownload(clip);
  renderTinderLikesList();
  updateTinderStatus();
  renderTinderStats();
}

function exportTinderLikes() {
  const likes = Array.from(state.tinderLikes.values());
  const payload = {
    exported_at: new Date().toISOString(),
    count: likes.length,
    likes,
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "tinderwatch-likes.json";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function loadTinderWatch(forceFresh = false) {
  try {
    // Fast first paint: load a small clip window first.
    const quickUrl = `/api/gallery?include_orphans=1&use_cache=${forceFresh ? 0 : 1}&max_clips=20`;
    const quickResponse = await fetch(quickUrl);
    const quickData = await quickResponse.json();
    state.tinderClips = flattenGalleryClips(quickData);
    if (state.tinderIndex >= state.tinderClips.length) state.tinderIndex = 0;
    setTinderwatchBadge(computeUnseenFromClips(state.tinderClips));
    renderTinderCard();

    // Then hydrate full list in background without blocking first video.
    const fullUrl = `/api/gallery?include_orphans=1&use_cache=${forceFresh ? 0 : 1}`;
    fetch(fullUrl)
      .then((r) => (r.ok ? r.json() : []))
      .then((fullData) => {
        const current = getCurrentTinderClip();
        const currentKey = current?.key || null;
        const allClips = flattenGalleryClips(fullData);
        if (!allClips.length) return;
        state.tinderClips = allClips;
        if (currentKey) {
          const idx = state.tinderClips.findIndex((c) => c.key === currentKey);
          state.tinderIndex = idx >= 0 ? idx : 0;
        } else if (state.tinderIndex >= state.tinderClips.length) {
          state.tinderIndex = 0;
        }
        setTinderwatchBadge(computeUnseenFromClips(state.tinderClips));
        updateTinderStatus();
      })
      .catch(() => {});
  } catch (_) {
    const root = $("#tinder-card");
    if (root) root.innerHTML = `<div class="tinder-empty">Fehler beim Laden der Clips.</div>`;
  }
}

document.querySelector('[data-tab="gallery"]').addEventListener("click", () => {
  loadGallery();
});
document.querySelector('[data-tab="tinderwatch"]').addEventListener("click", () => {
  loadTinderWatch();
});

document.querySelector('[data-tab="jobs"]').addEventListener("click", () => {
  startJobsPolling();
  loadJobs();
});

document.querySelector('[data-tab="stats"]').addEventListener("click", () => {
  loadStats();
});
document.querySelector('[data-tab="cuts"]').addEventListener("click", () => {
  loadCutsView();
});
document.querySelector('[data-tab="settings"]').addEventListener("click", () => {
  loadSettingsCacheSummary();
});

const refreshStats = $("#refresh-stats");
if (refreshStats) refreshStats.addEventListener("click", () => loadStats());
const refreshCuts = $("#refresh-cuts");
if (refreshCuts) refreshCuts.addEventListener("click", () => loadCutsView());

const tinderRefresh = $("#tinder-refresh");
if (tinderRefresh) tinderRefresh.addEventListener("click", () => loadTinderWatch(true));
const tinderLikeBtn = $("#tinder-like");
if (tinderLikeBtn) tinderLikeBtn.addEventListener("click", () => tinderLike());
const tinderDislikeBtn = $("#tinder-dislike");
if (tinderDislikeBtn) tinderDislikeBtn.addEventListener("click", () => tinderDislike());
const tinderDownloadAllBtn = $("#tinder-download-all");
if (tinderDownloadAllBtn) tinderDownloadAllBtn.addEventListener("click", () => downloadAllLikedClips());
const tinderExportLikesBtn = $("#tinder-export-likes");
if (tinderExportLikesBtn) tinderExportLikesBtn.addEventListener("click", () => exportTinderLikes());
const tinderLikeFilterToggleBtn = $("#tinder-like-filter-toggle");
if (tinderLikeFilterToggleBtn) tinderLikeFilterToggleBtn.addEventListener("click", () => cycleTinderLikeFilter());
document.querySelectorAll(".settings-day-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    state.settingsClearDays = Number(btn.dataset.days || 30);
    updateSettingsDaysButtons();
  });
});
const settingsClearSelectedBtn = $("#settings-clear-selected");
if (settingsClearSelectedBtn) settingsClearSelectedBtn.addEventListener("click", clearSettingsCacheAdvanced);
const settingsClearAllBtn = $("#settings-clear-all-btn");
if (settingsClearAllBtn) settingsClearAllBtn.addEventListener("click", clearSettingsCacheAll);

document.addEventListener("keydown", (ev) => {
  if (!isTabActive("tinderwatch")) return;
  if (ev.key === "ArrowLeft") {
    ev.preventDefault();
    tinderDislike();
  } else if (ev.key === "ArrowRight") {
    ev.preventDefault();
    tinderLike();
  }
});

authStatus();
setTab(tabFromUrl() || "sources", { syncUrl: true, replaceHistory: true });
loadJobs();
restoreLastPickerSession();
startJobsPolling();
startCachePolling();
restoreCutTuningFromStorage();
updateOpenAiTuningVisibility();
loadTinderStateFromStorage();
hydrateTinderStateFromServer().then(() => {
  renderTinderStats();
  refreshTinderwatchBadgeFromServer();
});
updateTinderLikeFilterButton();
renderTinderStats();
updateSettingsDaysButtons();
refreshTinderwatchBadgeFromServer();
