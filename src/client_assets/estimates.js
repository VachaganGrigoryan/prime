'use strict';

const element = id => document.getElementById(id);
const integer = value => BigInt(value).toLocaleString('en-US');
const format = (value, digits = 2) => Number(value).toLocaleString('en-US', {
  maximumFractionDigits: digits,
});
const modelNames = {
  pnt: 'x / ln x', corrected: 'x / (ln x − 1)', li: 'Logarithmic integral',
  smooth: 'Riemann R', z64: 'R with 64 zero pairs',
};
const pageSize = 12;
let data, blocks, selectedCell, manifest;
let windowPage = 0;
let currentScore, localScore = false, siteScore;

// Subtract before converting to a plotting number, even beyond 2^53.
const unit = 10n ** 24n;
function fixed(value) {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(String(value));
  if (!match) throw new Error('Invalid decimal in measurements.');
  const fraction = (match[3] || '').slice(0, 24).padEnd(24, '0');
  return (match[1] ? -1n : 1n) * (BigInt(match[2]) * unit + BigInt(fraction));
}
function showFixed(value, signed = false, digits = 2) {
  const magnitude = value < 0n ? -value : value;
  const scale = 10n ** BigInt(digits);
  const rounded = (magnitude * scale + unit / 2n) / unit;
  const fraction = String(rounded % scale).padStart(digits, '0').replace(/0+$/, '');
  const sign = rounded === 0n ? '' : value < 0n ? '−' : signed ? '+' : '';
  return sign + integer(rounded / scale) + (fraction ? '.' + fraction : '');
}
function node(tag, text) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  return item;
}
async function readData(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error('HTTP ' + response.status);
  return response.json();
}
function archiveLink(lo, hi) {
  const url = new URL('/api/v1/summary', data.origin);
  url.search = new URLSearchParams({ lo, hi, buckets: '1', revision: data.revision });
  return url.href;
}
function plot(id, actual, estimates) {
  const rows = Object.entries(modelNames).map(([key, label]) => {
    const error = actual * unit - estimates[key];
    const length = Math.log1p(Math.abs(Number(error) / Number(unit)));
    return { key, label, error, length };
  });
  const max = Math.max(...rows.map(row => row.length), Number.EPSILON);
  element(id).replaceChildren(...rows.map(({ key, label, error, length }) => {
    const row = node('div');
    row.className = 'model';
    const title = node('div');
    title.className = 'model-label';
    title.append(node('span', label), node('strong', showFixed(error, true)));
    const axis = node('div');
    axis.className = 'axis';
    axis.setAttribute('aria-hidden', 'true');
    const bar = node('span');
    bar.className = 'bar ' + (error >= 0n ? 'under' : 'over');
    bar.style[error >= 0n ? 'left' : 'right'] = '50%';
    bar.style.width = (50 * length / max) + '%';
    axis.append(bar);
    const estimate = node('div', 'Estimate ' + showFixed(estimates[key]));
    estimate.className = 'model-data';
    row.append(title, axis, estimate);
    return row;
  }));
}
function compare() {
  const first = Number(element('from-block').value);
  const last = Number(element('to-block').value);
  const span = blocks.slice(first, last + 1);
  const after = blocks.slice(last + 1);
  const total = rows => rows.reduce((sum, row) => sum + BigInt(row.count), 0n);
  const predicted = (rows, key) => rows.reduce((sum, row) => sum + fixed(row.estimates[key]), 0n);
  const through = BigInt(data.frontier.count) - total(after);
  const interval = total(span);
  const lo = span[0].lo;
  const hi = span[span.length - 1].hi;
  const cumulative = {}, selected = {};
  for (const key of Object.keys(modelNames)) {
    cumulative[key] = fixed(data.frontier.estimates[key]) - predicted(after, key);
    selected[key] = predicted(span, key);
  }
  element('cumulative-caption').textContent = integer(through) + ' primes through ' + integer(hi);
  element('interval-caption').textContent = integer(interval) + ' primes from ' + integer(lo) + ' through ' + integer(hi);
  plot('cumulative', through, cumulative);
  plot('interval', interval, selected);
  element('interval-source').href = archiveLink(lo, hi);
}
function selectionChange(event) {
  const from = element('from-block'), to = element('to-block');
  if (Number(from.value) > Number(to.value)) {
    if (event.target === from) to.value = from.value;
    else from.value = to.value;
  }
  compare();
}
function heatmap() {
  const quantity = element('quantity').value;
  const model = 'z' + element('zeros').value;
  const locations = [...new Set(data.cells.map(cell => cell.location))];
  const widths = [...new Set(data.cells.map(cell => cell.width))].sort((a,b) => a-b);
  const difference = cell => cell.mae[quantity].smooth - cell.mae[quantity][model];
  const maxDifference = Math.max(...data.cells.map(cell => Math.abs(difference(cell))), Number.EPSILON);
  const head = node('thead'), body = node('tbody'), header = node('tr');
  for (const title of ['Location / width', ...widths.map(integer)]) {
    const th = node('th', title);
    th.scope = 'col';
    header.append(th);
  }
  head.append(header);
  for (const location of locations) {
    const tr = node('tr'), th = node('th', location);
    th.scope = 'row';
    tr.append(th);
    for (const width of widths) {
      const cell = data.cells.find(c => c.location === location && c.width === width);
      const td = node('td');
      if (cell) {
        const delta = difference(cell);
        const button = node('button', (delta > 0 ? '+' : '') + format(delta, 4));
        button.type = 'button';
        button.className = 'heat-cell';
        button.dataset.tone = delta > 0 ? 'up' : 'down';
        button.style.setProperty('--strength', (100 * Math.sqrt(Math.abs(delta)/maxDifference)) + '%');
        button.setAttribute('aria-pressed', String(cell === selectedCell));
        button.setAttribute('aria-label', location + ', width ' + integer(width) + ', change ' + format(delta, 4));
        button.addEventListener('click', () => {
          selectedCell = cell;
          windowPage = 0;
          for (const other of element('heat').querySelectorAll('button')) other.setAttribute('aria-pressed', String(other === button));
          evidence();
        });
        td.append(button);
      } else {
        td.textContent = '—';
        td.setAttribute('aria-label', 'No measurements');
      }
      tr.append(td);
    }
    body.append(tr);
  }
  const caption = element('heat-caption');
  caption.textContent = 'Decrease in mean absolute error · ' + (quantity === 'count' ? 'primes' : 'log-weight units');
  element('heat').replaceChildren(caption, head, body);
  phaseGroups(locations, quantity, model);
  evidence();
}
function phaseGroups(locations, quantity, model) {
  const bands = new Map();
  for (const cell of data.cells) {
    for (const row of cell.windows) {
      const angle = row.phase[model];
      if (!(angle > 0)) continue;
      const band = Math.floor(Math.log10(angle));
      if (!bands.has(band)) bands.set(band, new Map());
      const groups = bands.get(band);
      const group = groups.get(cell.location) || { count: 0, total: 0 };
      const observed = fixed(quantity === 'count' ? row.count : row.psi);
      const estimates = quantity === 'count' ? row.estimates : row.psi_estimates;
      const error = key => Math.abs(Number(observed - fixed(estimates[key])) / Number(unit));
      group.total += error('smooth') - error(model);
      group.count++;
      groups.set(cell.location, group);
    }
  }
  const header = node('tr');
  for (const title of ['Phase band (rad)', ...locations]) {
    const th = node('th', title);
    th.scope = 'col';
    header.append(th);
  }
  element('phase-head').replaceChildren(header);
  element('phase-body').replaceChildren(...[...bands.keys()].sort((a,b) => a-b).map(band => {
    const row = node('tr');
    const th = node('th', '10^' + band + ' to <10^' + (band + 1));
    th.scope = 'row';
    row.append(th);
    for (const location of locations) {
      const group = bands.get(band).get(location);
      row.append(node('td', group ? format(group.total / group.count, 4) + ' (' + group.count + ' windows)' : '—'));
    }
    return row;
  }));
  element('phase-summary').textContent = bands.size
    ? 'Windows are grouped by powers of ten in radians. A shared band allows comparison across locations. Different window widths can cover the same integers.'
    : 'Choose 16 or 64 pairs to compare phase advances.';
}
function evidence() {
  const cell = selectedCell;
  const quantity = element('quantity').value, model = 'z' + element('zeros').value;
  const mae = cell.mae[quantity];
  const units = quantity === 'count' ? 'primes' : 'log-weight units';
  element('cell-title').textContent = cell.location + ' · width ' + integer(cell.width);
  element('cell-summary').textContent = cell.windows.length + ' disjoint windows at revision ' + data.revision +
    '. Mean absolute error in ' + units + ' was ' + format(mae.smooth, 4) + ' for the baseline and ' + format(mae[model], 4) + ' for the zero model.';
  const start = windowPage * pageSize;
  element('windows').replaceChildren(...cell.windows.slice(start, start + pageSize).map(window => {
    const tr = node('tr'), first = node('td');
    const link = node('a', integer(window.lo) + '–' + integer(window.hi));
    link.href = archiveLink(window.lo, window.hi);
    first.append(link);
    const count = quantity === 'count';
    const estimates = count ? window.estimates : window.psi_estimates;
    tr.append(first, node('td', count ? integer(window.count) : showFixed(fixed(window.psi))),
      node('td', showFixed(fixed(estimates.smooth))), node('td', showFixed(fixed(estimates[model]))),
      node('td', model === 'z0' ? '—' : Number(window.phase[model]).toPrecision(4)));
    return tr;
  }));
  element('page-status').textContent = (start + 1) + '–' + Math.min(start + pageSize, cell.windows.length) + ' of ' + cell.windows.length;
  element('prev').disabled = windowPage === 0;
  element('next').disabled = start + pageSize >= cell.windows.length;
}
function display(dataset) {
  data = dataset;
  selectedCell = data.cells.find(c => String(c.hi) === String(data.frontier.x) && Number(c.width) === 1048576);
  if (!selectedCell) throw new Error('The pinned frontier blocks are missing.');
  blocks = selectedCell.windows;
  element('frontier').textContent = integer(data.frontier.x);
  element('total').textContent = integer(data.frontier.count);
  element('revision').textContent = data.revision;
  element('provenance').textContent = 'Model ' + data.model_version + ' · zero table SHA-256 ' + data.zero_sha256;
  for (const id of ['from-block', 'to-block']) {
    const select = element(id);
    blocks.forEach((block, i) => select.add(new Option(integer(id === 'from-block' ? block.lo : block.hi), i)));
    select.value = String(blocks.length - 1);
    select.addEventListener('change', selectionChange);
  }
  compare();
  heatmap();
  element('load-status').hidden = true;
  element('experiment').hidden = false;
}
for (const control of ['quantity','zeros']) element(control).addEventListener('change', () => { windowPage = 0; heatmap(); });
element('prev').addEventListener('click', () => { windowPage--; evidence(); });
element('next').addEventListener('click', () => { windowPage++; evidence(); });
readData('prime-experiment.json').then(display).catch(error => {
  element('load-status').textContent = 'Could not load the archive comparison. ' + error.message;
});

function displayScore(score, fromFile) {
  if (!manifest || score.kind !== 'score' || score.manifest_sha256 !== manifest.sha256 ||
      score.model_version !== manifest.model_version || score.zero_sha256 !== manifest.zero_sha256 ||
      score.origin !== manifest.origin || BigInt(score.revision) < BigInt(manifest.revision)) {
    throw new Error('The score must reference this prediction manifest.');
  }
  const lookup = new Map(manifest.cells.map(cell => [cell.lo + '/' + cell.hi + '/' + cell.width, cell]));
  if (score.cells.length !== lookup.size) throw new Error('Incomplete list of prediction cells.');
  const quantity = element('score-quantity').value;
  const pairs = element('score-zeros').value;
  const model = 'z' + pairs;
  const count = quantity === 'count';
  const units = count ? 'primes' : 'log-weight units';
  const status = element('score-status');
  const title = node('p', (fromFile ? 'Local score file' : 'Site score snapshot') + ' · archive revision ' + score.revision);
  const table = node('table'), caption = node('caption', 'Prospective window results');
  const headings = node('tr'), head = node('thead'), body = node('tbody');
  for (const name of ['Interval', 'Observed (' + units + ')', 'Smooth estimate', pairs + '-pair estimate', 'Actual minus estimate']) {
    const th = node('th', name);
    th.scope = 'col';
    headings.append(th);
  }
  head.append(headings);
  let complete = 0, pending = 0;
  const summaries = [];
  for (const cell of score.cells) {
    const key = cell.lo + '/' + cell.hi + '/' + cell.width;
    const source = lookup.get(key);
    if (!source) throw new Error('Unknown prediction cell.');
    lookup.delete(key);
    let smoothError = 0n, zeroError = 0n;
    const predictions = new Map(source.windows.map(w => [w.lo + '/' + w.hi, w]));
    for (const [state, windows] of [['complete', cell.windows], ['pending', cell.pending || []]]) {
      for (const w of windows) {
        const key = w.lo + '/' + w.hi;
        const prediction = predictions.get(key);
        if (!prediction) throw new Error('Unexpected window in score.');
        predictions.delete(key);
        const estimates = count ? prediction.estimates : prediction.psi_estimates;
        const baseline = fixed(estimates.smooth), forecast = fixed(estimates[model]);
        let observed = 'Pending', difference = '—';
        const row = node('tr');
        row.append(node('td', integer(w.lo) + '–' + integer(w.hi)));
        if (state === 'complete') {
          const width = BigInt(w.hi) - BigInt(w.lo) + 1n;
          if (BigInt(w.covered) !== width || BigInt(w.count) > width || BigInt(w.count) < 0n || fixed(w.psi) < 0n) {
            throw new Error('A scored window lacks complete coverage or a valid count.');
          }
          complete++;
          const actual = fixed(w[quantity]);
          const error = actual - forecast;
          observed = showFixed(actual, false, 6);
          difference = showFixed(error, true, 6);
          smoothError += actual >= baseline ? actual - baseline : baseline - actual;
          zeroError += error >= 0n ? error : -error;
        } else {
          pending++;
        }
        row.append(node('td', observed), node('td', showFixed(baseline, false, 6)),
          node('td', showFixed(forecast, false, 6)), node('td', difference));
        body.append(row);
      }
    }
    if (cell.windows.length) {
      const n = BigInt(cell.windows.length);
      summaries.push(node('p', 'Width ' + integer(cell.width) + ' · ' + n + ' scored. Mean absolute error in ' + units +
        ' was ' + showFixed(smoothError / n, false, 6) + ' for the baseline and ' + showFixed(zeroError / n, false, 6) +
        ' for ' + pairs + ' pairs.'));
    }
    if (predictions.size) throw new Error('Score omits forecast windows.');
  }
  table.append(caption, head, body);
  const scroll = node('div');
  scroll.className = 'scroll';
  scroll.append(table);
  status.replaceChildren(title, node('p', complete + ' fully scored windows · ' + pending + ' pending'), ...summaries, scroll);
  currentScore = score;
  localScore = fromFile;
  element('score-controls').hidden = false;
  element('site-score').hidden = !fromFile || !siteScore;
  if (!fromFile) {
    const download = node('a', 'Download published score ↓');
    download.href = 'prime-score.json';
    download.download = '';
    status.append(download);
  }
  const witness = score.publication;
  if (!fromFile && witness && witness.manifest_sha256 === manifest.sha256) {
    const line = node('p', 'Manifest witnessed on the public site at ' + witness.observed_at + ', archive revision ' + witness.revision + '. ');
    if (/^[a-f0-9]{40}$/.test(witness.pages_commit)) {
      const link = node('a', 'Publication commit ↗');
      link.href = 'https://github.com/VachaganGrigoryan/prime/commit/' + witness.pages_commit;
      line.append(link);
    }
    status.append(line);
  }
}
// Match the CLI's json.dumps(sort_keys=True, ensure_ascii=True, separators=(',', ':')).
// Predictions store nonintegral quantities and u64 endpoints as decimal strings.
function canonicalPrediction(value) {
  if (Array.isArray(value)) return '[' + value.map(canonicalPrediction).join(',') + ']';
  if (value !== null && typeof value === 'object') {
    // All schema keys are ASCII, whose lexical order matches Python's Unicode order.
    const keys = Object.keys(value);
    if (keys.some(key => /[^a-zA-Z0-9_]/.test(key))) throw new Error('Invalid prediction field name.');
    return '{' + keys.sort().map(key => JSON.stringify(key) + ':' + canonicalPrediction(value[key])).join(',') + '}';
  }
  if (typeof value === 'number' && !Number.isSafeInteger(value)) throw new Error('Invalid prediction number.');
  return JSON.stringify(value).replace(/[\u007f-\uffff]/g, char =>
    '\\u' + char.charCodeAt(0).toString(16).padStart(4, '0'));
}
async function checkedManifest(candidate) {
  if (candidate?.kind !== 'prediction' || !candidate.cells?.length || !/^[0-9a-f]{64}$/.test(candidate.sha256)) {
    throw new Error('Invalid prediction manifest.');
  }
  if (!globalThis.crypto?.subtle) throw new Error('Checksum verification requires HTTPS or localhost.');
  const { sha256, ...payload } = candidate;
  const bytes = new TextEncoder().encode(canonicalPrediction(payload));
  const hash = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const actual = Array.from(hash, b => b.toString(16).padStart(2, '0')).join('');
  if (sha256 !== actual) throw new Error('Prediction checksum mismatch.');
  return candidate;
}
async function loadForecast() {
  manifest = await checkedManifest(await readData('prime-prediction.json'));
  element('manifest-link').hidden = false;
  element('score-file').disabled = false;
  const lo = manifest.cells.reduce((n, cell) => BigInt(cell.lo) < n ? BigInt(cell.lo) : n, BigInt(manifest.cells[0].lo));
  const hi = manifest.cells.reduce((n, cell) => BigInt(cell.hi) > n ? BigInt(cell.hi) : n, BigInt(manifest.cells[0].hi));
  element('manifest-status').textContent = 'Frozen range ' + integer(lo) + '–' + integer(hi) +
    ' · 0, 16, and 64 zero pairs. Generated ' + manifest.created_at + ' at archive revision ' + manifest.revision + '.';
  const response = await fetch('prime-score.json');
  if (response.status === 404) {
    element('score-status').textContent = 'Awaiting a published score.';
    return;
  }
  if (!response.ok) throw new Error('Could not retrieve the published score.');
  siteScore = await response.json();
  if (!localScore) displayScore(siteScore, false);
  else element('site-score').hidden = false;
}
for (const id of ['score-quantity', 'score-zeros']) {
  element(id).addEventListener('change', () => displayScore(currentScore, localScore));
}
element('site-score').addEventListener('click', () => displayScore(siteScore, false));
element('score-file').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  event.target.value = '';
  try {
    displayScore(JSON.parse(await file.text()), true);
  } catch (error) {
    if (currentScore) {
      displayScore(currentScore, localScore);
      element('score-status').prepend(node('p', 'Could not open this score. ' + error.message));
    } else element('score-status').textContent = 'Could not open this score. ' + error.message;
  }
});
loadForecast().catch(error => {
  if (manifest?.kind === 'prediction') element('score-status').textContent = 'Could not read the score. ' + error.message;
  else {
    element('manifest-status').textContent = 'Prediction manifest unavailable. ' + error.message;
    element('manifest-link').hidden = true;
  }
});
