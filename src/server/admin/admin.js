const base = "/api/v1/mining";
const pool = document.querySelector("#workers");
const feedback = document.querySelector("#feedback");
const pause = document.querySelector("#pause");
let updating = false;
let currentWorkers = "";
let latestState;

const number = (value) => BigInt(value).toLocaleString();
const time = (value) => value ? new Date(value * 1000).toLocaleString() : "—";
const range = (value) => value ? `${number(value.lo)} to ${number(value.hi)}` : "—";

async function request(path, body) {
  const response = await fetch(base + path, body === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.error || `Atlas returned HTTP ${response.status}`);
  }
  return response.json();
}

function node(tag, value) {
  const element = document.createElement(tag);
  element.textContent = value;
  return element;
}

function fields(list, entries) {
  list.replaceChildren(...entries.map(([label, value]) => {
    const field = document.createElement("div");
    field.append(node("dt", label), node("dd", value));
    return field;
  }));
}

function workerCard(worker) {
  const card = document.createElement("article");
  const details = document.createElement("dl");
  const benchmark = worker.benchmark;
  card.append(node("h3", `${worker.name} · ${worker.state}`), details);
  fields(details, [
    ["Device ID", worker.id],
    ["Last seen on Atlas", time(worker.last_seen)],
    ["Benchmark", benchmark ? (benchmark.passed ? "Passed" : "Failed") : "Awaiting result"],
    ["Benchmark received", time(worker.benchmark_at)],
    ["Benchmark prime count", benchmark ? number(benchmark.count) : "—"],
    ["Speed reported by worker", benchmark ? `${Math.round(benchmark.count * 1000 / benchmark.elapsed_ms).toLocaleString()} primes/s` : "—"],
    ["Platform", benchmark?.platform ?? "—"],
    ["Client version", benchmark?.client_version ?? "—"],
    ["Current block", range(worker.assignment)],
    ["Accepted", number(worker.accepted)],
    ["Invalid", number(worker.invalid)],
    ["Expired leases", number(worker.expired)],
    ["Last verification", worker.verification_ms == null ? "—" : `${worker.verification_ms} ms`],
    ["Benchmark SHA-256", benchmark?.digest ?? "—"],
  ]);
  const actions = worker.state === "pending" ? ["approve", "reject"] :
    worker.state === "approved" ? ["revoke"] : [];
  for (const action of actions) {
    const button = node("button", action[0].toUpperCase() + action.slice(1));
    button.disabled = action === "approve" && !benchmark?.passed;
    button.addEventListener("click", () => mutate(`/admin/workers/${worker.id}/${action}`, {}));
    card.append(button);
  }
  return card;
}

async function refresh() {
  const [stats, workers] = await Promise.all([request("/progress"), request("/admin/workers")]);
  latestState = stats.state;
  fields(document.querySelector("#progress"), [
    ["Status", stats.state],
    ["Verified through", number(stats.verified_through)],
    ["Confirmed primes", number(stats.verified_primes)],
    ["Next candidates", range(stats.next_candidates)],
    ["Online approved workers", number(stats.online_workers)],
    ["Active blocks", number(stats.active.length)],
  ]);
  const serialized = JSON.stringify(workers);
  if (currentWorkers !== serialized) {
    const focused = pool.contains(document.activeElement) ? document.activeElement : null;
    if (!focused) {
      pool.replaceChildren(...workers.map(workerCard));
      currentWorkers = serialized;
    }
  }
  document.querySelector("#empty").hidden = workers.length !== 0;
  pause.textContent = stats.state === "paused" ? "Resume mining" : "Pause mining";
  pause.disabled = stats.state === "exhausted" || stats.state === "storage-limited";
}

async function run(update) {
  if (updating) return;
  updating = true;
  try {
    await update();
    feedback.textContent = "";
  } catch (error) {
    feedback.textContent = error.message;
  } finally {
    updating = false;
  }
}

function mutate(path, body) {
  return run(async () => {
    await request(path, body);
    document.activeElement?.blur();
    await refresh();
  });
}

pause.addEventListener("click", () => mutate("/admin/pause", { paused: latestState !== "paused" }));
run(refresh);
setInterval(() => run(refresh), 5000);
