'use strict';

/**
 * VLESS + WebSocket node runner for Pterodactyl-style hosts (e.g. Bot-Hosting).
 *
 * Supported cores:  both (default: sing-box + xray side by side) | sing-box | xray
 * Optional:         Cloudflare quick tunnel (cloudflared)
 *
 * In "both" mode each core listens on a private loopback port and a tiny
 * built-in WebSocket router on the public port forwards each connection to
 * the right core by its WS path (hosts like Pterodactyl expose one port only).
 *
 * Zero npm dependencies: downloads, archive extraction and process
 * supervision are implemented with Node.js built-ins only (Node >= 18).
 */

const { spawn } = require('child_process');
const crypto = require('crypto');
const dns = require('dns').promises;
const fs = require('fs');
const http = require('http');
const https = require('https');
const net = require('net');
const os = require('os');
const path = require('path');
const zlib = require('zlib');

// ---------------------------------------------------------------------------
// Constants / defaults
// ---------------------------------------------------------------------------

const ROOT = __dirname;
const CONFIG_PATH = path.join(ROOT, 'config.json');
const BIN_DIR = path.join(ROOT, 'bin');
const RUNTIME_DIR = path.join(ROOT, '.runtime');
const PID_FILE = path.join(RUNTIME_DIR, 'pids.json');

// Latest stable releases at the time of writing. Set an env var to "latest"
// to always resolve the newest stable release from GitHub at startup.
const DEFAULT_VERSIONS = {
  'sing-box': '1.14.4',
  xray: '26.10.29',
  cloudflared: '2026.10.6',
};

const CORE_NAMES = ['sing-box', 'xray'];
const SUPPORTED_CORES = ['both', ...CORE_NAMES];
const CONFIG_VERSION = 2;
// UUIDs that are always accepted, no env var needed. The first one is also
// the default UUID when nothing else is configured, so existing client links
// keep working after a redeploy / reinstall.
const BUILTIN_UUIDS = ['58a080f7-5d54-4e41-8468-4272e2a56c93'];
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const log = (tag, ...args) => console.log(`[${tag}]`, ...args);
const warn = (tag, ...args) => console.warn(`[${tag}]`, ...args);

process.on('unhandledRejection', (reason) => warn('Error', reason && reason.stack ? reason.stack : reason));

// ---------------------------------------------------------------------------
// Settings (config.json + environment variables)
// ---------------------------------------------------------------------------

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    if (e.code !== 'ENOENT') warn('Config', `Cannot parse ${path.basename(file)}: ${e.message}`);
    return null;
  }
}

/** Extract a UUID from the new flat format or from legacy sing-box / xray configs. */
function extractUuid(cfg) {
  if (!cfg || typeof cfg !== 'object') return '';
  if (typeof cfg.uuid === 'string') return cfg.uuid.trim();
  const inbound = Array.isArray(cfg.inbounds) ? cfg.inbounds[0] : null;
  if (!inbound) return '';
  const fromSingBox = inbound.users && inbound.users[0] && inbound.users[0].uuid;
  const fromXray = inbound.settings && inbound.settings.clients && inbound.settings.clients[0] && inbound.settings.clients[0].id;
  return String(fromSingBox || fromXray || '').trim();
}

/** Parse a UUID list from an array or a comma/space separated string. */
function parseUuidList(value) {
  const items = Array.isArray(value) ? value : String(value || '').split(/[\s,;]+/);
  return items.map((u) => String(u || '').trim().toLowerCase()).filter((u) => UUID_RE.test(u));
}

/** Every UUID found in a (flat or legacy) config file. */
function extractAllUuids(cfg) {
  if (!cfg || typeof cfg !== 'object') return [];
  const out = [extractUuid(cfg)];
  out.push(...parseUuidList(cfg.extra_uuids), ...parseUuidList(cfg.previous_uuids));
  const inbound = Array.isArray(cfg.inbounds) ? cfg.inbounds[0] : null;
  if (inbound) {
    for (const u of inbound.users || []) out.push(u && u.uuid);
    for (const c of (inbound.settings && inbound.settings.clients) || []) out.push(c && c.id);
  }
  return parseUuidList(out);
}

/**
 * Deterministic UUID derived from a stable host identity. Only used when a
 * freshly generated UUID cannot be saved, so it does not change on every
 * restart (which breaks every client with "unknown UUID").
 */
function stableFallbackUuid() {
  const seed = process.env.P_SERVER_UUID || `${os.hostname()}|${ROOT}`;
  const b = crypto.createHash('sha256').update(`vless-node-runner|${seed}`).digest().subarray(0, 16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function uniq(list) {
  return [...new Set(list)];
}

function extractWsPath(cfg) {
  if (!cfg || typeof cfg !== 'object') return '';
  if (typeof cfg.ws_path === 'string') return cfg.ws_path;
  const inbound = Array.isArray(cfg.inbounds) ? cfg.inbounds[0] : null;
  if (!inbound) return '';
  return (inbound.transport && inbound.transport.path)
    || (inbound.streamSettings && inbound.streamSettings.wsSettings && inbound.streamSettings.wsSettings.path)
    || '';
}

function normalizeCore(value) {
  const c = String(value).trim().toLowerCase();
  if (c === 'singbox' || c === 'sb') return 'sing-box';
  if (c === 'all' || c === 'dual') return 'both';
  return c;
}

function normalizePath(value) {
  let p = String(value).trim() || '/';
  if (!p.startsWith('/')) p = `/${p}`;
  return p;
}

/** Path used for routing: no query string, no trailing slash. */
function pathKey(p) {
  const pathname = String(p).split('?')[0];
  return pathname.length > 1 ? pathname.replace(/\/+$/, '') : pathname;
}

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  return !/^(0|false|no|off)$/i.test(String(value).trim());
}

function loadSettings() {
  const raw = readJson(CONFIG_PATH) || {};
  const isLegacy = Array.isArray(raw.inbounds);
  const env = process.env;

  const port = Number.parseInt(env.SERVER_PORT || env.PORT || raw.port || '3000', 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`Invalid port: ${env.SERVER_PORT || env.PORT || raw.port}`);
  }

  // Old versions of this script wrote "core": "sing-box" into config.json on
  // their own, which silently disabled xray forever. Configs without
  // "config_version" come from that script, so they are migrated to "both".
  const needsMigration = !isLegacy && raw.config_version !== CONFIG_VERSION;
  const fileCore = needsMigration ? 'both' : raw.core;
  const core = normalizeCore(env.CORE || fileCore || 'both');
  if (!SUPPORTED_CORES.includes(core)) {
    throw new Error(`Unsupported core "${core}". Use one of: ${SUPPORTED_CORES.join(', ')}`);
  }

  const fileUuid = extractUuid(raw).toLowerCase();
  const envUuid = String(env.UUID || '').trim().toLowerCase();
  if (envUuid && !UUID_RE.test(envUuid)) warn('Config', `UUID env var "${envUuid}" is not a valid UUID, ignoring it.`);
  if (fileUuid && !UUID_RE.test(fileUuid)) warn('Config', `"${fileUuid}" in config.json is not a valid UUID, ignoring it.`);

  // No random UUID any more: a random one changes whenever config.json is
  // lost or not writable, which breaks every client with "unknown UUID".
  let uuid = UUID_RE.test(envUuid) ? envUuid : (UUID_RE.test(fileUuid) ? fileUuid : '');
  let generated = false;
  if (!uuid) {
    uuid = parseUuidList(BUILTIN_UUIDS)[0] || stableFallbackUuid();
    generated = true;
  }

  // Every other UUID we know about stays valid, so clients configured with an
  // older link (or with the config.json UUID while UUID env is set, or the
  // other way round) are not rejected with "unknown UUID".
  const extraUuids = uniq([
    ...parseUuidList(env.EXTRA_UUIDS || env.UUIDS),
    ...extractAllUuids(raw),
    ...parseUuidList(BUILTIN_UUIDS),
  ]).filter((u) => u !== uuid);

  // Automatic UUID history: every UUID this server has ever used is saved in
  // config.json ("previous_uuids") and stays valid, so old share links never
  // stop working when the UUID changes.
  const known = new Set([fileUuid, ...extractAllUuids(raw)]);
  const historyChanged = [uuid, ...parseUuidList(env.EXTRA_UUIDS || env.UUIDS)].some((u) => !known.has(u));

  const wsPath = normalizePath(env.WS_PATH || extractWsPath(raw) || '/vless-ws');
  const xrayWsPath = normalizePath(env.XRAY_WS_PATH || raw.xray_ws_path || `${wsPath.replace(/\/+$/, '')}-xray`);
  if (core === 'both' && pathKey(wsPath) === pathKey(xrayWsPath)) {
    throw new Error(`ws_path and xray_ws_path must be different in "both" mode (both are ${wsPath})`);
  }

  const settings = {
    core,
    uuid,
    extra_uuids: extraUuids,
    port,
    ws_path: wsPath,
    xray_ws_path: xrayWsPath,
    tunnel: parseBool(env.TUNNEL, parseBool(raw.tunnel, true)),
    log_level: String(env.LOG_LEVEL || raw.log_level || 'warning').toLowerCase(),
    versions: {
      'sing-box': String(env.SINGBOX_VERSION || (raw.versions && raw.versions['sing-box']) || DEFAULT_VERSIONS['sing-box']),
      xray: String(env.XRAY_VERSION || (raw.versions && raw.versions.xray) || DEFAULT_VERSIONS.xray),
      cloudflared: String(env.CLOUDFLARED_VERSION || (raw.versions && raw.versions.cloudflared) || DEFAULT_VERSIONS.cloudflared),
    },
  };

  // Persist a generated UUID (so share links stay stable across restarts)
  // and migrate the legacy sing-box-formatted config.json to the flat format.
  // Values coming from environment variables are never written to disk.
  if (generated || isLegacy || needsMigration || historyChanged) {
    const persistedCore = normalizeCore(fileCore || 'both');
    const persisted = {
      ...(isLegacy ? {} : raw), // keep user keys such as "versions"
      config_version: CONFIG_VERSION,
      core: SUPPORTED_CORES.includes(persistedCore) ? persistedCore : 'both',
      uuid: generated ? uuid : (UUID_RE.test(fileUuid) ? fileUuid : ''),
      extra_uuids: parseUuidList(raw.extra_uuids),
      previous_uuids: uniq([...extractAllUuids(raw), uuid, ...extraUuids])
        .filter((u) => u !== (generated ? uuid : fileUuid) && !BUILTIN_UUIDS.includes(u) && !parseUuidList(raw.extra_uuids).includes(u)),
      ws_path: env.WS_PATH ? normalizePath(extractWsPath(raw) || '/vless-ws') : wsPath,
      xray_ws_path: env.XRAY_WS_PATH ? normalizePath(raw.xray_ws_path || `${wsPath.replace(/\/+$/, '')}-xray`) : xrayWsPath,
      tunnel: parseBool(raw.tunnel, true),
      log_level: raw.log_level || 'warning',
    };
    if (!persisted.extra_uuids.length) delete persisted.extra_uuids;
    if (!persisted.previous_uuids.length) delete persisted.previous_uuids;
    try {
      fs.writeFileSync(CONFIG_PATH, `${JSON.stringify(persisted, null, 2)}\n`);
      if (isLegacy) log('Config', 'Migrated legacy config.json to the new format.');
      if (needsMigration && !isLegacy) log('Config', `Updated config.json to format v${CONFIG_VERSION} (core: ${persisted.core}).`);
      if (generated) log('Config', `No UUID configured, using the built-in default and saving it to config.json: ${uuid}`);
      if (historyChanged && !generated) log('Config', 'Saved UUID history to config.json (old links keep working).');
    } catch (e) {
      warn('Config', `Could not write config.json: ${e.message}`);
    }
  }

  return settings;
}

// ---------------------------------------------------------------------------
// Core config generators
// ---------------------------------------------------------------------------

function singBoxLogLevel(level) {
  const map = { warning: 'warn', none: 'panic' };
  const lvl = map[level] || level;
  return ['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'panic'].includes(lvl) ? lvl : 'warn';
}

function xrayLogLevel(level) {
  const map = { warn: 'warning', trace: 'debug', fatal: 'error', panic: 'error' };
  const lvl = map[level] || level;
  return ['debug', 'info', 'warning', 'error', 'none'].includes(lvl) ? lvl : 'warning';
}

function allUuids(s) {
  return uniq([s.uuid, ...(s.extra_uuids || [])]);
}

/**
 * Core log filter: collapses the flood of "unknown UUID" errors into one
 * clear, rate-limited hint instead of spamming the console (hosts may kill
 * servers that produce huge amounts of output).
 */
function makeCoreLogger(name, settings) {
  const seen = new Map();
  return (line) => {
    const m = line.match(/(?:unknown UUID|invalid request user id)[^0-9a-f]*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?/i);
    if (!m) { console.log(`[${name}] ${line}`); return; }
    const key = (m[1] || 'unknown').toLowerCase();
    const now = Date.now();
    const entry = seen.get(key) || { last: 0, suppressed: 0 };
    if (now - entry.last < 60000) { entry.suppressed += 1; seen.set(key, entry); return; }
    const extra = entry.suppressed ? ` (${entry.suppressed} similar errors hidden in the last minute)` : '';
    warn(name, `Rejected client UUID ${key}${extra}. Server accepts: ${allUuids(settings).join(', ')}. `
      + 'Re-import the share link printed above, or add that UUID to "extra_uuids" in config.json.');
    seen.set(key, { last: now, suppressed: 0 });
  };
}

function buildSingBoxConfig(s, { listen = '0.0.0.0', port = s.port, wsPath = s.ws_path } = {}) {
  return {
    log: { level: singBoxLogLevel(s.log_level), timestamp: true },
    inbounds: [{
      type: 'vless',
      tag: 'vless-in',
      listen,
      listen_port: port,
      users: allUuids(s).map((uuid, i) => ({ name: i ? `extra-${i}` : 'default', uuid })),
      transport: { type: 'ws', path: pathKey(wsPath) },
    }],
    outbounds: [{ type: 'direct', tag: 'direct' }],
    route: { final: 'direct' },
  };
}

function buildXrayConfig(s, { listen = '0.0.0.0', port = s.port, wsPath = s.ws_path } = {}) {
  return {
    log: { loglevel: xrayLogLevel(s.log_level) },
    inbounds: [{
      tag: 'vless-in',
      listen,
      port,
      protocol: 'vless',
      settings: {
        clients: allUuids(s).map((id, i) => ({ id, email: i ? `extra-${i}@local` : 'default@local' })),
        decryption: 'none',
      },
      streamSettings: {
        network: 'ws',
        security: 'none',
        wsSettings: { path: pathKey(wsPath) },
      },
    }],
    outbounds: [{ tag: 'direct', protocol: 'freedom' }],
  };
}

// ---------------------------------------------------------------------------
// HTTP helpers (no curl dependency)
// ---------------------------------------------------------------------------

function httpGet(url, { timeout = 15000, maxRedirects = 10, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'vless-node-runner', Accept: '*/*', ...headers },
      timeout,
    }, (res) => {
      const { statusCode, headers: resHeaders } = res;
      if (statusCode >= 300 && statusCode < 400 && resHeaders.location) {
        res.resume();
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        const next = new URL(resHeaders.location, url).toString();
        return resolve(httpGet(next, { timeout, maxRedirects: maxRedirects - 1, headers }));
      }
      if (statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('timeout', () => req.destroy(new Error(`Timeout fetching ${url}`)));
    req.on('error', reject);
  });
}

async function withRetry(fn, attempts = 3, label = 'request') {
  let lastErr;
  for (let i = 1; i <= attempts; i += 1) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      warn('Download', `${label} failed (attempt ${i}/${attempts}): ${e.message}`);
      if (i < attempts) await new Promise((r) => setTimeout(r, 2000 * i));
    }
  }
  throw lastErr;
}

async function resolveVersion(repo, wanted) {
  const v = String(wanted).replace(/^v/, '');
  if (v !== 'latest') return v;
  const body = await withRetry(() => httpGet(`https://api.github.com/repos/${repo}/releases/latest`, {
    headers: { Accept: 'application/vnd.github+json' },
  }), 2, `resolve latest ${repo}`);
  const tag = JSON.parse(body.toString('utf8')).tag_name;
  if (!tag) throw new Error(`No release tag found for ${repo}`);
  return String(tag).replace(/^v/, '');
}

// ---------------------------------------------------------------------------
// Archive extraction (tar.gz / zip) with built-in zlib
// ---------------------------------------------------------------------------

function extractFromTarGz(buffer, wantedBaseName) {
  const tar = zlib.gunzipSync(buffer);
  let offset = 0;
  let longName = null;
  while (offset + 512 <= tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((b) => b === 0)) break;
    const str = (start, len) => header.subarray(start, start + len).toString('utf8').replace(/\0.*$/s, '');
    let name = str(0, 100);
    const prefix = str(345, 155);
    if (prefix) name = `${prefix}/${name}`;
    const size = Number.parseInt(str(124, 12).trim() || '0', 8);
    const type = String.fromCharCode(header[156] || 48);
    const dataStart = offset + 512;
    const data = tar.subarray(dataStart, dataStart + size);

    if (type === 'L') {
      longName = data.toString('utf8').replace(/\0.*$/s, '');
    } else if (type === 'x') {
      const m = data.toString('utf8').match(/(?:^|\n)\d+ path=([^\n]*)\n/);
      if (m) longName = m[1];
    } else if (type === 'g') {
      // global pax header: ignore
    } else {
      if (longName) { name = longName; longName = null; }
      if ((type === '0' || type === '\0') && path.posix.basename(name) === wantedBaseName) {
        return Buffer.from(data);
      }
    }
    offset = dataStart + Math.ceil(size / 512) * 512;
  }
  throw new Error(`"${wantedBaseName}" not found in tar archive`);
}

function extractFromZip(buffer, wantedBaseName) {
  // Locate the End Of Central Directory record.
  let eocd = -1;
  for (let i = buffer.length - 22; i >= Math.max(0, buffer.length - 65557); i -= 1) {
    if (buffer.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('Invalid zip archive (no EOCD)');
  const entries = buffer.readUInt16LE(eocd + 10);
  let ptr = buffer.readUInt32LE(eocd + 16);

  for (let n = 0; n < entries; n += 1) {
    if (buffer.readUInt32LE(ptr) !== 0x02014b50) throw new Error('Invalid zip central directory');
    const method = buffer.readUInt16LE(ptr + 10);
    const compSize = buffer.readUInt32LE(ptr + 20);
    const nameLen = buffer.readUInt16LE(ptr + 28);
    const extraLen = buffer.readUInt16LE(ptr + 30);
    const commentLen = buffer.readUInt16LE(ptr + 32);
    const localOffset = buffer.readUInt32LE(ptr + 42);
    const name = buffer.subarray(ptr + 46, ptr + 46 + nameLen).toString('utf8');
    ptr += 46 + nameLen + extraLen + commentLen;

    if (path.posix.basename(name) !== wantedBaseName || name.endsWith('/')) continue;
    if (buffer.readUInt32LE(localOffset) !== 0x04034b50) throw new Error('Invalid zip local header');
    const lNameLen = buffer.readUInt16LE(localOffset + 26);
    const lExtraLen = buffer.readUInt16LE(localOffset + 28);
    const start = localOffset + 30 + lNameLen + lExtraLen;
    const data = buffer.subarray(start, start + compSize);
    if (method === 0) return Buffer.from(data);
    if (method === 8) return zlib.inflateRawSync(data);
    throw new Error(`Unsupported zip compression method ${method}`);
  }
  throw new Error(`"${wantedBaseName}" not found in zip archive`);
}

// ---------------------------------------------------------------------------
// Binary management
// ---------------------------------------------------------------------------

function detectArch() {
  const arch = os.arch();
  if (arch === 'x64') return 'amd64';
  if (arch === 'arm64') return 'arm64';
  throw new Error(`Unsupported CPU architecture: ${arch} (supported: x64, arm64)`);
}

const BINARIES = {
  'sing-box': {
    repo: 'SagerNet/sing-box',
    file: 'sing-box',
    url: (v, arch) => `https://github.com/SagerNet/sing-box/releases/download/v${v}/sing-box-${v}-linux-${arch}.tar.gz`,
    extract: (buf) => extractFromTarGz(buf, 'sing-box'),
  },
  xray: {
    repo: 'XTLS/Xray-core',
    file: 'xray',
    url: (v, arch) => `https://github.com/XTLS/Xray-core/releases/download/v${v}/Xray-linux-${arch === 'amd64' ? '64' : 'arm64-v8a'}.zip`,
    extract: (buf) => extractFromZip(buf, 'xray'),
  },
  cloudflared: {
    repo: 'cloudflare/cloudflared',
    file: 'cloudflared',
    url: (v, arch) => `https://github.com/cloudflare/cloudflared/releases/download/${v}/cloudflared-linux-${arch}`,
    extract: (buf) => buf,
  },
};

/**
 * Ensures the requested binary/version is installed in ./bin.
 * Downloads to a temp file and renames atomically, so an interrupted
 * download never leaves a corrupt executable behind.
 */
async function ensureBinary(name, wantedVersion) {
  const spec = BINARIES[name];
  const binPath = path.join(BIN_DIR, spec.file);
  const versionFile = `${binPath}.version`;
  fs.mkdirSync(BIN_DIR, { recursive: true });

  const installed = fs.existsSync(binPath) && fs.existsSync(versionFile)
    ? fs.readFileSync(versionFile, 'utf8').trim()
    : '';

  let version;
  try {
    version = await resolveVersion(spec.repo, wantedVersion);
  } catch (e) {
    if (installed) {
      warn(name, `Could not resolve latest version (${e.message}), keeping installed ${installed}.`);
      return { binPath, version: installed };
    }
    version = DEFAULT_VERSIONS[name];
    warn(name, `Could not resolve latest version (${e.message}), falling back to ${version}.`);
  }

  if (installed === version) return { binPath, version };

  const arch = detectArch();
  const url = spec.url(version, arch);
  log(name, installed ? `Updating ${installed} -> ${version} ...` : `Downloading ${version} (${arch}) ...`);
  let bin;
  try {
    const archive = await withRetry(() => httpGet(url, { timeout: 120000 }), 3, `${name} download`);
    bin = spec.extract(archive);
    if (!bin || bin.length < 1024 * 1024) throw new Error(`downloaded binary looks invalid (${bin ? bin.length : 0} bytes)`);
  } catch (e) {
    if (installed) {
      warn(name, `Update failed (${e.message}), keeping installed ${installed}.`);
      return { binPath, version: installed };
    }
    throw new Error(`${name}: ${e.message}`);
  }

  const tmp = `${binPath}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, bin, { mode: 0o755 });
  fs.renameSync(tmp, binPath);
  fs.chmodSync(binPath, 0o755);
  fs.writeFileSync(versionFile, `${version}\n`);
  log(name, `Installed ${version}.`);
  return { binPath, version };
}

// ---------------------------------------------------------------------------
// Process supervision
// ---------------------------------------------------------------------------

const children = new Map(); // name -> ChildProcess
let shuttingDown = false;

function savePids() {
  try {
    fs.mkdirSync(RUNTIME_DIR, { recursive: true });
    const pids = {};
    for (const [name, child] of children) if (child.pid) pids[name] = child.pid;
    fs.writeFileSync(PID_FILE, JSON.stringify(pids));
  } catch (e) { /* non-fatal */ }
}

/** Kill processes left over from a previous run, but only if they are ours. */
function killStaleProcesses() {
  const pids = readJson(PID_FILE) || {};
  for (const pid of Object.values(pids)) {
    if (!Number.isInteger(pid) || pid <= 1 || pid === process.pid) continue;
    try {
      const cmdline = fs.readFileSync(`/proc/${pid}/cmdline`, 'utf8');
      if (cmdline.includes(BIN_DIR)) {
        process.kill(pid, 'SIGKILL');
        log('Cleanup', `Killed stale process ${pid}`);
      }
    } catch (e) { /* process gone or not ours */ }
  }
  try { fs.unlinkSync(PID_FILE); } catch (e) { /* ignore */ }
}

/** Binaries written by the previous version of this script (renamed to ./bin/*). */
function removeLegacyFiles() {
  for (const legacy of ['web', 'npm-runner']) {
    const p = path.join(ROOT, legacy);
    try {
      if (fs.statSync(p).isFile()) {
        fs.unlinkSync(p);
        log('Cleanup', `Removed legacy binary ./${legacy}`);
      }
    } catch (e) { /* not present */ }
  }
}

function pipeLines(stream, onLine) {
  let buf = '';
  stream.setEncoding('utf8');
  stream.on('data', (chunk) => {
    buf += chunk;
    const lines = buf.split(/\r?\n/);
    buf = lines.pop();
    for (const line of lines) if (line.trim()) onLine(line);
  });
  stream.on('end', () => { if (buf.trim()) onLine(buf); buf = ''; });
}

/**
 * Runs a command forever with exponential backoff on crashes.
 * `onStart(child)` is called for every (re)spawn.
 */
function supervise(name, cmd, args, { onLine, onStart } = {}) {
  let delay = 2000;
  const start = () => {
    if (shuttingDown) return;
    const startedAt = Date.now();
    log(name, `Starting: ${path.basename(cmd)} ${args.join(' ')}`);
    const child = spawn(cmd, args, { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    children.set(name, child);
    savePids();
    const handler = (line) => (onLine ? onLine(line) : console.log(`[${name}] ${line}`));
    pipeLines(child.stdout, handler);
    pipeLines(child.stderr, handler);
    if (onStart) onStart(child);

    let restarted = false;
    const restart = (reason) => {
      if (restarted) return;
      restarted = true;
      if (children.get(name) === child) children.delete(name);
      savePids();
      if (shuttingDown) return;
      if (Date.now() - startedAt > 60000) delay = 2000; // ran fine for a while: reset backoff
      warn(name, `${reason}. Restarting in ${Math.round(delay / 1000)}s ...`);
      setTimeout(start, delay);
      delay = Math.min(delay * 2, 60000);
    };
    child.on('error', (err) => restart(`Failed to start (${err.message})`));
    child.on('exit', (code, signal) => restart(`Exited (code=${code}, signal=${signal})`));
  };
  start();
}

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log('Main', `Received ${signal}, stopping child processes ...`);
  for (const child of children.values()) {
    try { child.kill('SIGTERM'); } catch (e) { /* ignore */ }
  }
  setTimeout(() => process.exit(0), 1500).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// ---------------------------------------------------------------------------
// Loopback ports & WebSocket path router ("both" mode)
// ---------------------------------------------------------------------------

function getFreePort(exclude = []) {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => (exclude.includes(port) ? resolve(getFreePort(exclude)) : resolve(port)));
    });
  });
}

/**
 * Listens on the public port and forwards every WebSocket upgrade to the
 * loopback core whose WS path matches. Plain HTTP requests get a harmless
 * 200 page (useful as a health check and for cloudflared).
 */
function startRouter(publicPort, routes) {
  const table = new Map(routes.map((r) => [pathKey(r.path), r]));
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('OK\n');
  });

  server.on('upgrade', (req, socket, head) => {
    socket.on('error', () => {});
    const route = table.get(pathKey(req.url || '/'));
    if (!route) {
      socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    const upstream = net.connect(route.port, '127.0.0.1');
    upstream.setNoDelay(true);
    socket.setNoDelay(true);
    upstream.on('connect', () => {
      const lines = [`${req.method} ${req.url} HTTP/1.1`];
      for (let i = 0; i < req.rawHeaders.length; i += 2) lines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
      upstream.write(`${lines.join('\r\n')}\r\n\r\n`);
      if (head && head.length) upstream.write(head);
      socket.pipe(upstream).pipe(socket);
    });
    upstream.on('error', (err) => {
      if (!socket.writableEnded && upstream.bytesRead === 0) {
        warn('Router', `${route.name} unreachable on 127.0.0.1:${route.port} (${err.code || err.message})`);
        socket.end('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      } else {
        socket.destroy();
      }
    });
    socket.on('close', () => upstream.destroy());
    upstream.on('close', () => socket.destroy());
  });

  server.on('clientError', (err, socket) => { try { socket.destroy(); } catch (e) { /* ignore */ } });
  server.keepAliveTimeout = 5000;
  server.requestTimeout = 0; // long-lived tunnels
  server.headersTimeout = 30000;

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(publicPort, '0.0.0.0', () => {
      server.removeListener('error', reject);
      server.on('error', (e) => warn('Router', e.message));
      log('Router', `Listening on 0.0.0.0:${publicPort} -> ${routes.map((r) => `${pathKey(r.path)} => ${r.name}`).join(', ')}`);
      resolve(server);
    });
  });
}

// ---------------------------------------------------------------------------
// Network discovery & share links
// ---------------------------------------------------------------------------

async function fetchPublicIP() {
  const apis = ['https://api.ipify.org', 'https://ifconfig.me/ip', 'https://icanhazip.com'];
  for (const api of apis) {
    try {
      const ip = (await httpGet(api, { timeout: 4000 })).toString('utf8').trim();
      if (net.isIP(ip) && !/^(0\.|127\.|::1$)/.test(ip)) return ip;
    } catch (e) { /* try next */ }
  }
  return null;
}

async function reverseLookup(ip) {
  if (!ip) return null;
  try {
    const names = await dns.reverse(ip);
    return names && names.length ? names[0] : null;
  } catch (e) {
    return null;
  }
}

function vlessLink({ uuid, host, port, tls, sni, wsHost, wsPath, name }) {
  const address = net.isIPv6(host) ? `[${host}]` : host;
  const params = new URLSearchParams({ encryption: 'none', security: tls ? 'tls' : 'none', type: 'ws' });
  if (tls && sni) params.set('sni', sni);
  if (wsHost) params.set('host', wsHost);
  params.set('path', wsPath);
  // URLSearchParams encodes spaces as "+", most clients expect %20.
  return `vless://${uuid}@${address}:${port}?${params.toString().replace(/\+/g, '%20')}#${encodeURIComponent(name)}`;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  if (process.platform !== 'linux') throw new Error('This runner only supports Linux hosts.');

  const settings = loadSettings();
  const cores = settings.core === 'both' ? [...CORE_NAMES] : [settings.core];
  const dual = cores.length > 1;
  const wsPathOf = (name) => (dual && name === 'xray' ? settings.xray_ws_path : settings.ws_path);
  log('Main', `Core: ${settings.core} | Port: ${settings.port} | WS path: ${cores.map((c) => `${c}=${wsPathOf(c)}`).join(', ')} | Tunnel: ${settings.tunnel ? 'on' : 'off'}`);

  killStaleProcesses();
  removeLegacyFiles();

  // Start network discovery in parallel with the downloads.
  const ipPromise = fetchPublicIP();

  // Download every core in parallel; in "both" mode one failing core must not kill the other.
  const results = await Promise.allSettled(cores.map((c) => ensureBinary(c, settings.versions[c])));
  const installed = [];
  results.forEach((r, i) => {
    if (r.status === 'fulfilled') installed.push({ name: cores[i], ...r.value });
    else warn(cores[i], `Disabled: ${r.reason && r.reason.message ? r.reason.message : r.reason}`);
  });
  if (!installed.length) throw new Error('No proxy core could be installed.');

  fs.mkdirSync(RUNTIME_DIR, { recursive: true });
  const routerMode = dual && installed.length > 1;
  const usedPorts = [settings.port];
  for (const core of installed) {
    core.wsPath = routerMode ? wsPathOf(core.name) : settings.ws_path;
    if (routerMode) {
      core.listen = '127.0.0.1';
      core.port = await getFreePort(usedPorts);
      usedPorts.push(core.port);
    } else {
      core.listen = '0.0.0.0';
      core.port = settings.port;
    }
    const opts = { listen: core.listen, port: core.port, wsPath: core.wsPath };
    const cfg = core.name === 'xray' ? buildXrayConfig(settings, opts) : buildSingBoxConfig(settings, opts);
    const cfgPath = path.join(RUNTIME_DIR, `${core.name}.json`);
    fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 2));
    supervise(core.name, core.binPath, ['run', '-c', cfgPath], { onLine: makeCoreLogger(core.name, settings) });
  }

  if (routerMode) {
    await startRouter(settings.port, installed.map((c) => ({ name: c.name, path: c.wsPath, port: c.port })));
  }

  const ip = await ipPromise;
  const domain = await reverseLookup(ip);

  console.log('\n==================================================');
  for (const core of installed) console.log(`[Info] Core     : ${core.name} ${core.version} (ws path ${core.wsPath})`);
  console.log(`[Info] Public IP: ${ip || 'unknown'}`);
  console.log(`[Info] PTR name : ${domain || 'none (no reverse DNS record)'}`);
  console.log(`[Info] UUID     : ${settings.uuid}`);
  if (settings.extra_uuids.length) console.log(`[Info] Also OK  : ${settings.extra_uuids.join(', ')}`);
  for (const core of installed) {
    const common = { uuid: settings.uuid, wsPath: core.wsPath };
    if (ip) {
      console.log(`\n⚡ ${core.name} | Direct (IP):`);
      console.log(vlessLink({ ...common, host: ip, port: settings.port, tls: false, wsHost: ip, name: `${core.name}-IP-Direct` }));
    }
    if (domain) {
      console.log(`\n🌐 ${core.name} | Direct (domain):`);
      console.log(vlessLink({ ...common, host: domain, port: settings.port, tls: false, wsHost: domain, name: `${core.name}-Domain-Direct` }));
    }
  }
  console.log('==================================================\n');

  if (!settings.tunnel) return;

  let tunnel;
  try {
    tunnel = await ensureBinary('cloudflared', settings.versions.cloudflared);
  } catch (e) {
    warn('Tunnel', `cloudflared unavailable, tunnel disabled: ${e.message}`);
    return;
  }

  let printed = false;
  supervise('cloudflared', tunnel.binPath, [
    'tunnel', '--no-autoupdate', '--url', `http://127.0.0.1:${settings.port}`,
  ], {
    onStart: () => { printed = false; },
    onLine: (line) => {
      const match = line.match(/https:\/\/([a-z0-9-]+\.trycloudflare\.com)/i);
      if (match && match[1].toLowerCase() !== 'api.trycloudflare.com' && !printed) {
        printed = true;
        const sub = match[1];
        console.log('\n==================================================');
        for (const core of installed) {
          console.log(`🚀 ${core.name} | Cloudflare tunnel (TLS):`);
          console.log(vlessLink({ uuid: settings.uuid, wsPath: core.wsPath, host: sub, port: 443, tls: true, sni: sub, wsHost: sub, name: `${core.name}-CF-Tunnel` }));
        }
        console.log('==================================================\n');
      } else if (/\b(ERR|error)\b/.test(line)) {
        console.log(`[cloudflared] ${line}`);
      }
    },
  });
}

if (require.main === module) {
  main().catch((err) => {
    console.error('[Fatal]', err && err.stack ? err.stack : err);
    process.exit(1);
  });
}

module.exports = {
  buildSingBoxConfig, buildXrayConfig, extractFromTarGz, extractFromZip, extractUuid, vlessLink,
  loadSettings, pathKey, startRouter, parseUuidList, extractAllUuids, makeCoreLogger,
};
