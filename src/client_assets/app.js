'use strict';

const id = (name) => document.getElementById(name);
const sameOrigin = ['localhost', '127.0.0.1', '[::1]', 'prime-dev-api.notallow.cc'].includes(location.hostname);
const apiOrigin = sameOrigin ? location.origin : 'https://prime-api.notallow.cc';
const maxInteger = (1n << 64n) - 1n;
const numberFormat = new Intl.NumberFormat();
const rateFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const formatInteger = (value) => numberFormat.format(BigInt(value));
const formatRange = ({ lo, hi }) => `${formatInteger(lo)}–${formatInteger(hi)}`;
const stateText = {
  running: 'Mining',
  waiting: 'Waiting for approved workers',
  paused: 'Assignments paused',
  'storage-limited': 'Waiting for archive space',
  '64-bit-exhausted': '64-bit search complete',
  exhausted: '64-bit search complete',
};

async function read(path, params = {}, signal) {
  const url = new URL(path, apiOrigin);
  url.search = new URLSearchParams(params);
  const response = await fetch(url, {
    signal,
    cache: 'no-store',
    credentials: 'omit',
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw new Error(`Atlas returned HTTP ${response.status}.`);
  }
  return response.json();
}

function listItems(target, values, empty) {
  const items = values.length ? values : [empty];
  target.replaceChildren(...items.map((value) => {
    const item = document.createElement('li');
    item.textContent = value;
    return item;
  }));
}

async function refreshProgress() {
  try {
    const progress = await read('/api/v1/mining/progress', {}, AbortSignal.timeout(12000));
    const state = progress.state.replaceAll('_', '-');
    id('connection').textContent = stateText[state] || 'Connected to Atlas';
    id('frontier').textContent = progress.verified_through === null || BigInt(progress.verified_through) < 2n
      ? '—'
      : formatInteger(progress.verified_through);
    id('prime-count').textContent = formatInteger(progress.verified_primes);
    id('rate').textContent = rateFormat.format(progress.recent_primes_per_second);
    id('workers').textContent = numberFormat.format(progress.online_workers);
    id('next-range').textContent = progress.next_candidates ? formatRange(progress.next_candidates) : '—';
    listItems(id('confirmed'), progress.latest_primes.map(formatInteger), 'No confirmed primes yet');
    listItems(id('active-blocks'), progress.active.map(formatRange), 'No blocks assigned');

    if (!id('range-hi').value && !id('range-lo').value && progress.verified_through && BigInt(progress.verified_through) >= 2n) {
      const hi = BigInt(progress.verified_through);
      id('range-lo').value = hi > 1048576n ? String(hi - 1048575n) : '2';
      id('range-hi').value = progress.verified_through;
      id('archive-form').requestSubmit();
    }
  } catch {
    id('connection').textContent = 'Atlas unavailable · retrying';
  } finally {
    setTimeout(refreshProgress, 5000);
  }
}

let currentSearch;

function displayChart(search) {
  const tallest = search.buckets.reduce((max, bucket) => bucket.count > max ? bucket.count : max, 1n);
  const columns = search.buckets.map((bucket) => {
    const bar = document.createElement('div');
    const scale = Number(bucket.count * 10000n / tallest) / 100;
    bar.style.height = `${scale}%`;
    bar.dataset.covered = String(bucket.covered);
    bar.title = `${formatRange(bucket)} · ${formatInteger(bucket.count)} primes · ${formatInteger(bucket.covered)} checked integers`;
    return bar;
  });
  id('chart').replaceChildren(...columns);
  id('chart').hidden = false;
  id('bucket-details').hidden = false;
  listItems(id('bucket-list'), columns.map((column) => column.title), 'No checked intervals');

  const count = search.buckets.reduce((total, bucket) => total + bucket.count, 0n);
  const covered = search.buckets.reduce((total, bucket) => total + bucket.covered, 0n);
  const width = BigInt(search.params.hi) - BigInt(search.params.lo) + 1n;
  const partial = search.chartCursor === null ? '' : ' · More archive data available';
  id('archive-status').textContent = `${formatInteger(count)} primes · ${formatInteger(covered)} of ${formatInteger(width)} integers checked${partial}`;
  id('more-chart').hidden = search.chartCursor === null;
}

async function loadChart(search) {
  const button = id('more-chart');
  button.disabled = true;
  try {
    const params = { ...search.params, buckets: 40 };
    if (search.revision !== undefined) params.revision = search.revision;
    if (search.chartCursor !== null) params.cursor = search.chartCursor;
    const page = await read('/api/v1/summary', params, search.signal);
    if (currentSearch !== search) return;
    search.revision = page.revision;
    search.chartCursor = page.next_cursor;
    search.buckets = page.buckets.map((bucket, index) => ({
      lo: bucket.lo,
      hi: bucket.hi,
      covered: BigInt(bucket.covered) + (search.buckets[index]?.covered ?? 0n),
      count: BigInt(bucket.count) + (search.buckets[index]?.count ?? 0n),
    }));
    displayChart(search);
  } catch (error) {
    if (currentSearch === search) {
      id('archive-status').textContent = `Could not load chart data. ${error.message}`;
      button.hidden = false;
    }
  } finally {
    if (currentSearch === search) button.disabled = false;
  }
}

async function loadPrimes(search) {
  const button = id('more-primes');
  button.disabled = true;
  try {
    const params = { ...search.params, limit: 50 };
    if (search.revision !== undefined) params.revision = search.revision;
    if (search.primeCursor !== null) params.after = search.primeCursor;
    const page = await read('/api/v1/primes', params, search.signal);
    if (currentSearch !== search) return;
    search.revision = page.revision;
    search.primeCursor = page.next_after;
    for (const prime of page.primes) {
      const item = document.createElement('li');
      item.textContent = formatInteger(prime);
      id('prime-list').append(item);
    }
    id('prime-list-status').textContent = id('prime-list').childElementCount === 0
      ? 'No archived primes in this interval.'
      : '';
    button.hidden = search.primeCursor === null;
  } catch (error) {
    if (currentSearch === search) {
      id('prime-list-status').textContent = `Could not load primes. ${error.message}`;
      button.hidden = false;
    }
  } finally {
    if (currentSearch === search) button.disabled = false;
  }
}

id('archive-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const start = id('range-lo').value.trim();
  const end = id('range-hi').value.trim();
  if (!/^\d{1,20}$/.test(start) || !/^\d{1,20}$/.test(end)) {
    id('archive-status').textContent = 'Enter whole numbers using digits 0–9.';
    return;
  }
  const lo = BigInt(start);
  const hi = BigInt(end);
  if (lo < 2n || lo > hi || hi > maxInteger) {
    id('archive-status').textContent = 'Enter an increasing interval from 2 through 18,446,744,073,709,551,615.';
    return;
  }
  currentSearch?.abort.abort();
  const abort = new AbortController();
  const search = {
    params: { lo: String(lo), hi: String(hi) },
    abort,
    signal: abort.signal,
    chartCursor: null,
    primeCursor: null,
    buckets: [],
  };
  currentSearch = search;
  id('archive-status').textContent = 'Reading archive coverage…';
  id('prime-list-status').textContent = '';
  id('prime-list').replaceChildren();
  for (const name of ['chart', 'bucket-details', 'list-panel', 'more-chart', 'more-primes']) {
    id(name).hidden = true;
  }
  await loadChart(search);
  if (currentSearch === search && search.revision !== undefined) {
    id('list-panel').hidden = false;
    await loadPrimes(search);
  }
});

id('more-chart').addEventListener('click', async () => {
  const search = currentSearch;
  await loadChart(search);
  if (currentSearch === search && id('list-panel').hidden && search.revision !== undefined) {
    id('list-panel').hidden = false;
    await loadPrimes(search);
  }
});
id('more-primes').addEventListener('click', () => loadPrimes(currentSearch));
refreshProgress();
