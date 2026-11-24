const api = "/api/v1/mining";
const find = (id) => document.getElementById(id);
const deviceId = location.pathname.match(/^\/devices\/([0-9]+)\/?$/)?.[1];
const globalPause = find("global-pause");
const deviceActions = [...find("device-actions").querySelectorAll("button")];
const search = find("device-search");
const approvalFilter = find("approval-filter");
const workFilter = find("work-filter");
const rows = find("device-rows");
const previous = find("previous-page");
const next = find("next-page");
const pageSize = 50;
const pollMs = 5000;
const absent = "—";
const dateFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });
const clockFormat = new Intl.DateTimeFormat(undefined, { timeStyle: "medium" });
const rateFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

const coordinatorStates = {
  running: ["Mining running", "good"],
  waiting: ["Waiting for available devices", "attention"],
  paused: ["Mining paused", "attention"],
  exhausted: ["Search complete", ""],
  "storage-limited": ["Storage limit reached", "critical"],
};

const approvalStates = {
  approved: ["Approved", "good"],
  pending: ["Pending", "attention"],
  rejected: ["Rejected", "critical"],
  revoked: ["Revoked", "critical"],
};

const auditActions = {
  approve: "Approved",
  reject: "Rejected",
  pause: "Paused",
  resume: "Resumed",
  revoke: "Revoked access",
};

let summary;
let device;
let fresh = false;
let writing = false;
let reader;
let searchTimer;
let page = 1;
let total = 0;

function element(tag, label, className) {
  const node = document.createElement(tag);
  if (label !== undefined) node.textContent = label;
  if (className) node.className = className;
  return node;
}

function setText(node, label) {
  if (node.textContent !== label) node.textContent = label;
}

function setBadge(node, [label, tone]) {
  node.className = tone ? `badge ${tone}` : "badge";
  setText(node, label);
}

function setTime(node, seconds, format = dateFormat) {
  if (seconds == null) {
    node.removeAttribute("datetime");
    setText(node, absent);
    return;
  }
  const date = new Date(seconds * 1000);
  node.dateTime = date.toISOString();
  setText(node, format.format(date));
}

function integer(value) {
  return value == null ? absent : BigInt(value).toLocaleString();
}

function workState(worker) {
  if (worker.state !== "approved") return ["Unavailable", ""];
  if (!worker.paused) return ["Enabled", "good"];
  if (worker.assignment) return ["Finishing current block", "attention"];
  return ["Paused", "attention"];
}

function benchmarkState(benchmark) {
  if (!benchmark) return ["Awaiting result", ""];
  return benchmark.passed ? ["Passed", "good"] : ["Failed", "critical"];
}

async function request(path, { body, signal } = {}) {
  const timeout = AbortSignal.timeout(10000);
  const init = {
    signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    headers: { Accept: "application/json" },
    redirect: "manual",
  };
  if (body !== undefined) {
    init.method = "POST";
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  let response;
  try {
    response = await fetch(api + path, init);
  } catch (error) {
    if (error.name === "TimeoutError") throw new Error("Atlas did not respond in time.");
    throw error;
  }
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    const auth = response.status === 401 || response.type === "opaqueredirect";
    const message = auth
      ? "Your Admin session may have expired. Reload this page to sign in."
      : data.error || `Atlas returned HTTP ${response.status}.`;
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function renderOverview(data) {
  summary = data;
  renderResources(data.resources);
  setBadge(find("coordinator-status"), coordinatorStates[data.state] ?? [data.state, ""]);
  if (data.paused && data.state === "storage-limited") {
    setText(find("coordinator-status"), "Mining paused · storage limit reached");
  }
  setText(globalPause, data.paused ? "Resume mining" : "Pause mining");
  setText(find("verified-through"), integer(data.verified_through));
  setText(find("verified-primes"), integer(data.verified_primes));
  setText(find("verified-rate"), rateFormat.format(data.recent_primes_per_second));
  setText(find("active-blocks"), integer(data.active_blocks));
  setText(find("fleet-total"), `${integer(data.total_devices)} registered`);
  setText(find("pending-count"), integer(data.pending_devices));
  setText(find("online-count"), integer(data.online_approved_devices));
  setText(find("paused-count"), integer(data.paused_devices));
}

function cpuLabel(value) {
  return value == null ? absent : rateFormat.format(value) + "%";
}

function memoryLabel(value) {
  return value == null ? absent : rateFormat.format(Number(value) / (1024 ** 2)) + " MiB";
}

function storageLabel(value) {
  if (value == null) return absent;
  const bytes = Number(value);
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
  const power = Math.min(units.length - 1, Math.floor(Math.log2(Math.max(bytes, 1)) / 10));
  return rateFormat.format(bytes / (1024 ** power)) + " " + units[power];
}

function renderResources(sample) {
  setTime(find("resource-sampled"), sample?.sampled_at, clockFormat);
  setText(find("resource-cpu"), cpuLabel(sample?.cpu_percent));
  setText(find("resource-memory"), memoryLabel(sample?.memory_bytes));
  setText(find("resource-archive"), storageLabel(sample?.archive_bytes));
  setText(find("resource-quota"), "Quota " + storageLabel(sample?.quota_bytes));
  setText(find("resource-volume-label"), sample?.volume_shared ? "Shared volume usage" : "Archive volume usage");
  setText(find("resource-volume"), storageLabel(sample?.volume_used_bytes) + " / " + storageLabel(sample?.volume_total_bytes));
  setText(find("resource-writable"), storageLabel(sample?.writable_bytes) + " writable after reserve");
}

const trendMetrics = [
  { id: "cpu", field: "cpu_percent", format: cpuLabel },
  { id: "memory", field: "memory_bytes", format: memoryLabel },
  { id: "archive", field: "archive_bytes", format: storageLabel },
];
const dayMinutes = 1440;
let minuteReadings = new Map();
let firstMinute = 0;
let newestMinute = 0;

function drawTrends() {
  if (!minuteReadings.size) return;
  const index = Number(find("resource-minute").value);
  for (const metric of trendMetrics) {
    const canvas = find("trend-" + metric.id);
    const width = canvas.clientWidth, height = canvas.clientHeight;
    if (!width || !height) continue;
    const scale = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * scale);
    canvas.height = Math.round(height * scale);
    const ctx = canvas.getContext("2d");
    if (!ctx) continue;
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    const values = [...minuteReadings.values()].map(row => row[metric.field])
      .filter(value => value != null).map(Number);
    const label = find("trend-" + metric.id + "-range");
    if (!values.length) {
      setText(label, "No readings");
      continue;
    }
    const low = Math.min(...values), high = Math.max(...values);
    setText(label, low === high ? metric.format(low) : metric.format(low) + " – " + metric.format(high));
    ctx.strokeStyle = getComputedStyle(canvas).color;
    ctx.lineWidth = 1.5;
    ctx.lineCap = "round";
    ctx.beginPath();
    let previous = -2;
    for (const [minute, row] of minuteReadings) {
      const value = row[metric.field];
      if (value == null) continue;
      const x = 1 + (minute - firstMinute) * (width - 2) / (dayMinutes - 1);
      const y = high === low ? height / 2 : 3 + (height - 6) * (high - Number(value)) / (high - low);
      if (minute !== previous + 1) ctx.moveTo(x - .25, y);
      ctx.lineTo(x, y);
      previous = minute;
    }
    ctx.stroke();
    const selectedX = 1 + index * (width - 2) / (dayMinutes - 1);
    ctx.fillStyle = "rgba(249,205,120,.55)";
    ctx.fillRect(selectedX, 0, 1, height);
  }
}

function inspectMinute() {
  const slider = find("resource-minute");
  const minute = firstMinute + Number(slider.value);
  const row = minuteReadings.get(minute);
  const time = dateFormat.format(new Date(minute * 60000));
  slider.setAttribute("aria-valuetext", time);
  setText(find("resource-minute-values"), row
    ? time + " · CPU " + cpuLabel(row.cpu_percent) + " · Memory " + memoryLabel(row.memory_bytes) +
      " · Archive " + storageLabel(row.archive_bytes)
    : time + " · No reading");
  drawTrends();
}

async function refreshResourceHistory() {
  const status = find("resource-history-status");
  try {
    const data = await request("/admin/resources/history");
    const end = Math.floor((data.server_time ?? summary?.server_time ?? Date.now() / 1000) / 60);
    const slider = find("resource-minute");
    const selected = firstMinute + Number(slider.value);
    const follow = !minuteReadings.size || selected === newestMinute;
    firstMinute = end - dayMinutes + 1;
    minuteReadings = new Map();
    for (const row of data.samples) {
      const minute = Math.floor(row.sampled_at / 60);
      if (minute >= firstMinute && minute <= end) minuteReadings.set(minute, row);
    }
    newestMinute = minuteReadings.size ? Math.max(...minuteReadings.keys()) : end;
    slider.value = Math.max(0, Math.min(dayMinutes - 1, (follow ? newestMinute : selected) - firstMinute));
    setText(find("resource-span"), dateFormat.format(new Date(firstMinute * 60000)) + " – " + dateFormat.format(new Date(end * 60000)));
    find("resource-history").hidden = !minuteReadings.size;
    status.hidden = Boolean(minuteReadings.size);
    if (minuteReadings.size) inspectMinute();
    else setText(status, "No minute readings recorded yet.");
  } catch (error) {
    status.hidden = false;
    setText(status, "Resource history unavailable. " + error.message);
  }
}

function createRow(id) {
  const row = element("tr");
  row.setAttribute("role", "row");
  row.dataset.id = id;
  const columns = ["Device", "Benchmark", "Approval", "Work status", "Last contact", "Accepted blocks"];
  const cells = columns.map((label, index) => {
    const cell = element("td");
    cell.setAttribute("role", "cell");
    if (index > 0) {
      const mobileLabel = element("span", label, "mobile-label");
      mobileLabel.setAttribute("aria-hidden", "true");
      cell.append(mobileLabel);
    }
    row.append(cell);
    return cell;
  });

  const link = element("a");
  link.href = `/devices/${id}`;
  cells[0].append(link, element("span", `ID ${id}`, "secondary"));
  cells[1].append(element("span"));
  cells[2].append(element("span"));
  cells[3].append(element("span"));
  cells[4].append(element("span"), element("time", undefined, "secondary"));
  cells[5].classList.add("numeric");
  cells[5].append(element("span"));
  return row;
}

function updateRow(row, worker) {
  const cells = row.cells;
  setText(cells[0].firstElementChild, worker.name);
  setBadge(cells[1].lastElementChild, benchmarkState(worker.benchmark));
  setBadge(cells[2].lastElementChild, approvalStates[worker.state]);
  setBadge(cells[3].lastElementChild, workState(worker));
  setText(cells[4].children[1], worker.online ? "Recently seen" : "No recent contact");
  setTime(cells[4].lastElementChild, worker.last_seen);
  setText(cells[5].lastElementChild, integer(worker.accepted));
}

function renderDevices(data) {
  total = data.total;
  const focused = document.activeElement;
  const keepFocus = rows.contains(focused);
  const existing = new Map([...rows.children].map((row) => [row.dataset.id, row]));
  const ordered = data.devices.map((worker) => {
    const id = String(worker.id);
    const row = existing.get(id) ?? createRow(id);
    existing.delete(id);
    updateRow(row, worker);
    return row;
  });
  for (const row of existing.values()) row.remove();
  let cursor = rows.firstElementChild;
  for (const row of ordered) {
    if (row === cursor) {
      cursor = cursor.nextElementSibling;
    } else {
      rows.insertBefore(row, cursor);
    }
  }
  if (keepFocus && document.activeElement !== focused) {
    (focused.isConnected ? focused : search).focus({ preventScroll: true });
  }

  const table = find("device-table");
  const status = find("device-list-status");
  table.hidden = !ordered.length;
  status.hidden = Boolean(ordered.length);
  if (!ordered.length) {
    setText(status, summary.total_devices === 0 ? "No devices have enrolled." : "No devices match these filters.");
  }

  const first = (page - 1) * pageSize + 1;
  const last = first + ordered.length - 1;
  const pagination = document.querySelector(".pagination");
  if (total <= pageSize && pagination.contains(document.activeElement)) {
    search.focus({ preventScroll: true });
  }
  pagination.hidden = total <= pageSize;
  setText(find("page-summary"), `${integer(first)}–${integer(last)} of ${integer(total)}`);
  previous.setAttribute("aria-disabled", String(page <= 1));
  next.setAttribute("aria-disabled", String(last >= total));
}

function renderFields(id, entries) {
  const list = find(id);
  const data = JSON.stringify(entries);
  if (list.dataset.snapshot === data) return;
  list.dataset.snapshot = data;
  list.replaceChildren(...entries.map(([label, value, style]) => {
    const field = element("div", undefined, style);
    const display = element("dd");
    if (style) display.classList.add("mono");
    if (value instanceof Object && value.seconds != null) {
      const time = element("time");
      setTime(time, value.seconds);
      display.append(time);
    } else {
      display.textContent = value ?? absent;
    }
    field.append(element("dt", label), display);
    return field;
  }));
}

function renderAudit(decisions) {
  const list = find("audit-entries");
  const data = JSON.stringify(decisions);
  if (list.dataset.snapshot === data) return;
  list.dataset.snapshot = data;
  find("audit-empty").hidden = decisions.length > 0;
  list.replaceChildren(...decisions.map((decision) => {
    const item = element("li");
    const time = element("time", undefined, "muted");
    setTime(time, decision.decided_at);
    item.append(
      element("strong", auditActions[decision.action] ?? decision.action),
      element("span", decision.actor),
      time,
    );
    return item;
  }));
}

function renderDevice(data) {
  device = data;
  const title = find("device-title");
  setText(title, data.name);
  document.title = `${data.name} · Prime Admin`;
  setBadge(find("device-approval"), approvalStates[data.state]);
  setBadge(find("device-work"), workState(data));
  setBadge(find("device-contact"), data.online ? ["Recently seen", "good"] : ["No recent contact", ""]);
  find("device-status").hidden = false;
  find("detail-status").hidden = true;
  find("device-content").hidden = false;

  const seen = (seconds) => seconds == null ? null : { seconds };
  const benchmark = data.benchmark;
  renderFields("identity-fields", [
    ["Device ID", integer(data.id)],
    ["Last contact", seen(data.last_seen)],
    ["Platform", benchmark?.platform],
    ["Client version", benchmark?.client_version],
  ]);
  renderFields("benchmark-fields", [
    ["Result", benchmarkState(benchmark)[0]],
    ["Received", seen(data.benchmark_at)],
    ["Prime count", integer(data.benchmark_prime_count)],
    ["Reported speed", benchmark ? `${rateFormat.format(benchmark.count * 1000 / benchmark.elapsed_ms)} primes/s` : absent],
    ["Benchmark version", benchmark?.version],
    ["SHA-256 digest", benchmark?.digest, "wide"],
  ]);
  renderFields("assignment-fields", [
    ["Candidate range", data.assignment ? `${integer(data.assignment.lo)}–${integer(data.assignment.hi)}` : "No active assignment", "wide"],
    ["Lease expires", seen(data.lease_expires_at)],
  ]);
  renderFields("result-fields", [
    ["Accepted blocks", integer(data.accepted)],
    ["Invalid blocks", integer(data.invalid)],
    ["Expired leases", integer(data.expired)],
    ["Latest verification", data.verification_ms == null ? absent : `${integer(data.verification_ms)} ms`],
  ]);
  renderAudit(data.decisions);
}

function updateControls() {
  const blocked = !fresh || writing;
  globalPause.setAttribute("aria-disabled", String(
    blocked || !summary || summary.state === "exhausted" ||
    (summary.state === "storage-limited" && summary.paused),
  ));
  if (!device) return;

  let actions = [];
  let description = "This device needs a new enrollment to return to the pool.";
  if (device.state === "pending") {
    actions = ["approve", "reject"];
    description = device.benchmark?.passed
      ? "This device passed its benchmark and is ready for review."
      : "A passing benchmark is required for approval.";
  } else if (device.state === "approved") {
    actions = [device.paused ? "resume" : "pause", "revoke"];
    description = device.paused
      ? (device.assignment ? "The current block can finish. New assignments are paused." : "New assignments are paused for this device.")
      : "Pausing lets an active block finish and holds new assignments.";
  }

  const focused = deviceActions.includes(document.activeElement) ? document.activeElement : null;
  setText(find("controls-description"), description);
  for (const button of deviceActions) {
    const action = button.dataset.action;
    button.hidden = !actions.includes(action);
    button.setAttribute("aria-disabled", String(blocked || (action === "approve" && !device.benchmark?.passed)));
  }
  if (focused?.hidden) {
    const replacement = deviceActions.find((button) => !button.hidden && button.getAttribute("aria-disabled") !== "true");
    (replacement ?? find("device-title")).focus({ preventScroll: true });
  }
}

function showFeedback(message, error = false) {
  const notice = find("action-feedback");
  notice.classList.toggle("error", error);
  notice.hidden = !message;
  setText(notice, message);
}

function showRefreshError(error) {
  fresh = false;
  const message = find("refresh-error");
  const label = find("refresh-status");
  label.classList.toggle("stale", Boolean(summary || device));
  setText(label, summary || device ? "Stale data · last refreshed" : "Unable to connect");
  setText(message, error.message);
  message.hidden = false;
  find("retry-refresh").hidden = false;
  if (deviceId && !device) {
    setText(find("detail-status"), error.status === 404 ? "Device not found." : "Device details are unavailable.");
  }
  if (!deviceId && !summary) setText(find("device-list-status"), "Device data is unavailable.");
  updateControls();
}

function showFresh() {
  fresh = true;
  const label = find("refresh-status");
  label.classList.remove("stale");
  setText(label, "Last refreshed");
  const refreshTime = find("last-refresh");
  setTime(refreshTime, Date.now() / 1000, clockFormat);
  refreshTime.hidden = false;
  find("refresh-error").hidden = true;
  const retry = find("retry-refresh");
  if (document.activeElement === retry) {
    (deviceId ? find("device-title") : search).focus({ preventScroll: true });
  }
  retry.hidden = true;
  updateControls();
}

function listQuery() {
  const query = new URLSearchParams();
  if (page > 1) query.set("page", page);
  if (search.value.trim()) query.set("q", search.value.trim());
  if (approvalFilter.value) query.set("approval", approvalFilter.value);
  if (workFilter.value) query.set("work", workFilter.value);
  return query;
}

function replaceListUrl() {
  const query = listQuery().toString();
  history.replaceState(null, "", query ? `/?${query}` : "/");
}

async function refresh({ replace = false } = {}) {
  if (writing || (reader && !replace)) return;
  reader?.abort();
  const controller = new AbortController();
  reader = controller;
  const options = { signal: controller.signal };
  try {
    if (deviceId) {
      const data = await request(`/admin/devices/${deviceId}`, options);
      if (controller.signal.aborted) return;
      renderDevice(data);
    } else {
      const query = listQuery();
      const [stats, list] = await Promise.all([
        request("/admin/overview", options),
        request(`/admin/devices?${query}`, options),
      ]);
      if (controller.signal.aborted) return;
      const lastPage = Math.max(1, Math.ceil(list.total / pageSize));
      if (page > lastPage) {
        page = lastPage;
        replaceListUrl();
        return refresh({ replace: true });
      }
      renderOverview(stats);
      renderDevices(list);
    }
    showFresh();
  } catch (error) {
    if (!controller.signal.aborted) showRefreshError(error);
  } finally {
    if (reader === controller) reader = undefined;
  }
}

async function mutate(path, body, success) {
  if (!fresh || writing) return;
  writing = true;
  reader?.abort();
  reader = undefined;
  updateControls();
  showFeedback("");
  try {
    await request(path, { body });
    showFeedback(success);
  } catch (error) {
    const message = error.status === 409 ? `Device or mining state changed. ${error.message}` : error.message;
    showFeedback(message, true);
  } finally {
    writing = false;
    fresh = false;
    updateControls();
    await refresh();
  }
}

function filterDevices() {
  page = 1;
  replaceListUrl();
  void refresh({ replace: true });
}

function changePage(delta) {
  const requested = page + delta;
  if (requested < 1 || (requested - 1) * pageSize >= total) return;
  page = requested;
  replaceListUrl();
  void refresh({ replace: true });
}

function enabled(button) {
  return button.getAttribute("aria-disabled") !== "true";
}

find("retry-refresh").addEventListener("click", () => void refresh({ replace: true }));
globalPause.addEventListener("click", () => {
  if (!enabled(globalPause)) return;
  const paused = !summary.paused;
  if (paused && !confirm("Pause new mining assignments? Active blocks can still finish.")) return;
  void mutate("/admin/pause", { paused }, paused ? "Mining paused." : "Mining resumed.");
});

for (const button of deviceActions) {
  button.addEventListener("click", () => {
    if (!enabled(button)) return;
    const action = button.dataset.action;
    const confirmations = {
      reject: `Reject ${device.name}? A new enrollment will be required.`,
      revoke: `Revoke access for ${device.name}? An unfinished lease will be released and a new enrollment will be required.`,
    };
    const messages = {
      approve: "Device approved.",
      reject: "Device rejected.",
      pause: "Device paused for new assignments.",
      resume: "Device resumed.",
      revoke: "Device access revoked.",
    };
    if (confirmations[action] && !confirm(confirmations[action])) return;
    void mutate(`/admin/workers/${deviceId}/${action}`, {}, messages[action]);
  });
}

if (deviceId) {
  find("device").hidden = false;
} else {
  const query = new URLSearchParams(location.search);
  const initialPage = Number(query.get("page"));
  if (Number.isSafeInteger(initialPage) && initialPage > 0) page = initialPage;
  search.value = (query.get("q") ?? "").slice(0, search.maxLength);
  approvalFilter.value = query.get("approval") ?? "";
  workFilter.value = query.get("work") ?? "";
  for (const filter of [approvalFilter, workFilter]) {
    if (filter.selectedIndex === -1) filter.value = "";
  }
  globalPause.disabled = false;
  globalPause.setAttribute("aria-disabled", "true");
  find("overview").hidden = false;
  find("resource-minute").addEventListener("input", inspectMinute);
  window.addEventListener("resize", drawTrends);
  void refreshResourceHistory();
  setInterval(() => void refreshResourceHistory(), 60000);

  find("device-filters").addEventListener("submit", (event) => {
    event.preventDefault();
    clearTimeout(searchTimer);
    filterDevices();
  });
  search.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(filterDevices, 250);
  });
  for (const filter of [approvalFilter, workFilter]) {
    filter.addEventListener("change", () => {
      clearTimeout(searchTimer);
      filterDevices();
    });
  }
  previous.addEventListener("click", () => changePage(-1));
  next.addEventListener("click", () => changePage(1));
}

void refresh();
setInterval(() => void refresh(), pollMs);
