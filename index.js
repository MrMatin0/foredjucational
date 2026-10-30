'use strict';

/**
 * VLESS + WebSocket node runner for Pterodactyl-style hosts (e.g. Bot-Hosting).
 *
 * Supported cores:  sing-box (default)  |  xray
 * Optional:         Cloudflare quick tunnel (cloudflared)
 *
 * Zero npm dependencies: downloads, archive extraction and process
 * supervision are implemented with Node.js built-ins only (Node >= 18).
 */

const { spawn } = require('child_process');
const crypto = require('crypto');
const dns = require('dns').promises;
const fs = require('fs');
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

const SUPPORTED_CORES = ['sing-box', 'xray'];
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

function extractWsPath(cfg) {
  if (!cfg || typeof cfg !== 'object') return '';
  if (typeof cfg.ws_path === 'string') return cfg.ws_path;
  const inbound = Array.isArray(cfg.inbounds) ? cfg.inbounds[0] : null;
  if (!inbound) return '';
  return (inbound.transport && inbound.transport.path)
    || (inbound.streamSettings && inbound.streamSettings.wsSettings && inbound.streamSettings.wsSettings.path)
    || '';
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

  const core = String(env.CORE || raw.core || 'sing-box').toLowerCase().replace(/^singbox$/, 'sing-box');
  if (!SUPPORTED_CORES.includes(core)) {
    throw new Error(`Unsupported core "${core}". Use one of: ${SUPPORTED_CORES.join(', ')}`);
  }

  let uuid = String(env.UUID || extractUuid(raw) || '').trim();
  let generated = false;
  if (!UUID_RE.test(uuid)) {
    if (uuid) warn('Config', `"${uuid}" is not a valid UUID, generating a new one.`);
    uuid = crypto.randomUUID();
    generated = true;
  }

  let wsPath = String(env.WS_PATH || extractWsPath(raw) || '/vless-ws').trim();
  if (!wsPath.startsWith('/')) wsPath = `/${wsPath}`;

  const settings = {
    core,
    uuid,
    port,
    ws_path: wsPath,
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
  if (generated || isLegacy || !raw.core) {
    const fileUuid = extractUuid(raw);
    const persisted = {
      ...(isLegacy ? {} : raw), // keep user keys such as "versions"
      core: env.CORE ? (SUPPORTED_CORES.includes(raw.core) ? raw.core : 'sing-box') : core,
      uuid: generated ? uuid : (UUID_RE.test(fileUuid) ? fileUuid : ''),
      ws_path: env.WS_PATH ? (extractWsPath(raw) || '/vless-ws') : wsPath,
      tunnel: parseBool(raw.tunnel, true),
      log_level: raw.log_level || 'warning',
    };
    try {
      fs.writeFileSync(CONFIG_PATH, `${JSON.stringify(persisted, null, 2)}\n`);
      if (isLegacy) log('Config', 'Migrated legacy config.json to the new format.');
      if (generated) log('Config', `Generated a new UUID and saved it to config.json: ${uuid}`);
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

function buildSingBoxConfig(s) {
  return {
    log: { level: singBoxLogLevel(s.log_level), timestamp: true },
    inbounds: [{
      type: 'vless',
      tag: 'vless-in',
      listen: '0.0.0.0',
      listen_port: s.port,
      users: [{ name: 'default', uuid: s.uuid }],
      transport: { type: 'ws', path: s.ws_path },
    }],
    outbounds: [{ type: 'direct', tag: 'direct' }],
    route: { final: 'direct' },
  };
}

function buildXrayConfig(s) {
  return {
    log: { loglevel: xrayLogLevel(s.log_level) },
    inbounds: [{
      tag: 'vless-in',
      listen: '0.0.0.0',
      port: s.port,
      protocol: 'vless',
      settings: {
        clients: [{ id: s.uuid, email: 'default@local' }],
        decryption: 'none',
      },
      streamSettings: {
        network: 'ws',
        security: 'none',
        wsSettings: { path: s.ws_path },
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
      return binPath;
    }
    version = DEFAULT_VERSIONS[name];
    warn(name, `Could not resolve latest version (${e.message}), falling back to ${version}.`);
  }

  if (installed === version) return binPath;

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
      return binPath;
    }
    throw new Error(`${name}: ${e.message}`);
  }

  const tmp = `${binPath}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, bin, { mode: 0o755 });
  fs.renameSync(tmp, binPath);
  fs.chmodSync(binPath, 0o755);
  fs.writeFileSync(versionFile, `${version}\n`);
  log(name, `Installed ${version}.`);
  return binPath;
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
      children.delete(name);
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
  const coreName = settings.core;
  log('Main', `Core: ${coreName} | Port: ${settings.port} | WS path: ${settings.ws_path} | Tunnel: ${settings.tunnel ? 'on' : 'off'}`);

  killStaleProcesses();
  removeLegacyFiles();

  // Start network discovery in parallel with the downloads.
  const ipPromise = fetchPublicIP();

  const coreBin = await ensureBinary(coreName, settings.versions[coreName]);

  fs.mkdirSync(RUNTIME_DIR, { recursive: true });
  const coreConfigPath = path.join(RUNTIME_DIR, `${coreName}.json`);
  const coreConfig = coreName === 'xray' ? buildXrayConfig(settings) : buildSingBoxConfig(settings);
  fs.writeFileSync(coreConfigPath, JSON.stringify(coreConfig, null, 2));

  supervise(coreName, coreBin, ['run', '-c', coreConfigPath], {
    onLine: (line) => console.log(`[${coreName}] ${line}`),
  });

  const ip = await ipPromise;
  const domain = await reverseLookup(ip);
  const common = { uuid: settings.uuid, wsPath: settings.ws_path };

  console.log('\n==================================================');
  console.log(`[Info] Core     : ${coreName} ${settings.versions[coreName]}`);
  console.log(`[Info] Public IP: ${ip || 'unknown'}`);
  console.log(`[Info] PTR name : ${domain || 'none (no reverse DNS record)'}`);
  console.log(`[Info] UUID     : ${settings.uuid}`);
  if (ip) {
    console.log('\n⚡ Direct (IP):');
    console.log(vlessLink({ ...common, host: ip, port: settings.port, tls: false, wsHost: ip, name: `${coreName}-IP-Direct` }));
  }
  if (domain) {
    console.log('\n🌐 Direct (domain):');
    console.log(vlessLink({ ...common, host: domain, port: settings.port, tls: false, wsHost: domain, name: `${coreName}-Domain-Direct` }));
  }
  console.log('==================================================\n');

  if (!settings.tunnel) return;

  let tunnelBin;
  try {
    tunnelBin = await ensureBinary('cloudflared', settings.versions.cloudflared);
  } catch (e) {
    warn('Tunnel', `cloudflared unavailable, tunnel disabled: ${e.message}`);
    return;
  }

  let printed = false;
  supervise('cloudflared', tunnelBin, [
    'tunnel', '--no-autoupdate', '--url', `http://127.0.0.1:${settings.port}`,
  ], {
    onStart: () => { printed = false; },
    onLine: (line) => {
      const match = line.match(/https:\/\/([a-z0-9-]+\.trycloudflare\.com)/i);
      if (match && match[1].toLowerCase() !== 'api.trycloudflare.com' && !printed) {
        printed = true;
        const sub = match[1];
        console.log('\n==================================================');
        console.log('🚀 Cloudflare tunnel (TLS):');
        console.log(vlessLink({ ...common, host: sub, port: 443, tls: true, sni: sub, wsHost: sub, name: `${coreName}-CF-Tunnel` }));
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
};
