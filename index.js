const https = require('https');
const express = require('express');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

function getEnvNumber(name, fallback, min = 0) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value >= min ? value : fallback;
}

/**************** CONFIG ****************/
if (!process.env.TELEGRAM_TOKEN) throw new Error('Missing TELEGRAM_TOKEN');
if (!process.env.SPREADSHEET_ID) throw new Error('Missing SPREADSHEET_ID');
if (!process.env.GOOGLE_CLIENT_EMAIL) throw new Error('Missing GOOGLE_CLIENT_EMAIL');
if (!process.env.GOOGLE_PRIVATE_KEY) throw new Error('Missing GOOGLE_PRIVATE_KEY');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const PORT = Number(process.env.PORT || 10000);
const APP_TIMEZONE = process.env.APP_TIMEZONE || 'Asia/Phnom_Penh';
const TELEGRAM_TIMEOUT_MS = getEnvNumber('TELEGRAM_TIMEOUT_MS', 15000, 1000);
const SHEET_CACHE_TTL_MS = getEnvNumber('SHEET_CACHE_TTL_MS', 5000, 0);
const PROCESSED_MESSAGE_TTL_MS = getEnvNumber('PROCESSED_MESSAGE_TTL_MS', 30 * 60 * 1000, 1000);
const PROCESSED_MESSAGE_CACHE_LIMIT = getEnvNumber('PROCESSED_MESSAGE_CACHE_LIMIT', 5000, 100);

const BOOTSTRAP_SUPER_ADMINS = (process.env.SUPER_ADMINS || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;
const CONFIRM_EXPIRE_MINUTES = 10;

const GROUP_BYPASS_COMMANDS = new Set(['/allowgroup', '/start', '/help']);
const WRITE_COMMANDS = new Set([
  '/additem', '/deleteitem', '/in', '/out', '/adjust',
  '/setalert', '/setunit', '/renameitem', '/confirm', '/cancel'
]);

const telegramHttp = axios.create({
  baseURL: TELEGRAM_API,
  timeout: TELEGRAM_TIMEOUT_MS,
  httpsAgent: new https.Agent({ keepAlive: true })
});

/**************** GOOGLE AUTH ****************/
const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_CLIENT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const sheets = google.sheets({ version: 'v4', auth });

/**************** CACHES ****************/
const rowCache = new Map();
const dateFormatterCache = new Map();
const processedMessageCache = new Map();
const processedMessageInflight = new Set();
const sheetIdCache = new Map();
let spreadsheetMetaCache = {
  data: null,
  expiresAt: 0
};

/**************** SIMPLE IN-MEMORY LOCKS ****************/
/*
  Note:
  - Works for a single running instance.
  - If you scale to multiple instances, move locking to external storage.
*/
const itemLocks = new Map();

async function withLock(key, fn) {
  const safeKey = String(key || '').trim().toLowerCase();
  const prev = itemLocks.get(safeKey) || Promise.resolve();

  let release;
  const next = new Promise(resolve => {
    release = resolve;
  });

  itemLocks.set(safeKey, prev.then(() => next));

  try {
    await prev;
    return await fn();
  } finally {
    release();
    if (itemLocks.get(safeKey) === next) {
      itemLocks.delete(safeKey);
    }
  }
}

/**************** HELPERS ****************/
function norm(text) {
  return String(text || '').trim().toLowerCase();
}

function clean(text) {
  return String(text || '')
    .replace(/\u00A0/g, ' ')
    .replace(/[\u200B\u200C\u200D\u2060\uFEFF]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanUsername(text) {
  return clean(text).replace(/^@+/, '').toLowerCase();
}

function logError(label, err) {
  console.error(label, err?.response?.data || err?.message || err);
}

async function runNonCriticalTask(label, fn) {
  try {
    return await fn();
  } catch (err) {
    logError(label, err);
    return null;
  }
}

function parsePipe(text) {
  return String(text || '')
    .split('|')
    .map(part => clean(part));
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isNaN(n) ? fallback : n;
}

function nowIso() {
  return new Date().toISOString();
}

function minutesFromNowIso(minutes) {
  return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}

function isExpired(isoString) {
  if (!isoString) return true;
  return new Date(isoString).getTime() < Date.now();
}

function getDateFormatter(timeZone = APP_TIMEZONE) {
  const key = String(timeZone || APP_TIMEZONE);
  let formatter = dateFormatterCache.get(key);

  if (!formatter) {
    formatter = new Intl.DateTimeFormat('en-CA', {
      timeZone: key,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    });
    dateFormatterCache.set(key, formatter);
  }

  return formatter;
}

function getLocalDateString(timeZone = APP_TIMEZONE) {
  return getDateFormatter(timeZone).format(new Date());
}

function getLocalDateStringFromDate(date, timeZone = APP_TIMEZONE) {
  return getDateFormatter(timeZone).format(date);
}

function isValidItemName(name) {
  const s = clean(name);
  return Boolean(s) && s.length <= 100 && !/[\r\n]/.test(s) && !s.includes('|');
}

function escapeCsv(value) {
  let s = String(value ?? '');

  // Prevent CSV/Excel/Sheets formula injection
  if (/^[=+\-@]/.test(s)) {
    s = "'" + s;
  }

  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function getUsername(msg) {
  return cleanUsername(msg?.from?.username || '');
}

function getActorIdentifier(msg) {
  const username = getUsername(msg);
  if (username) return `@${username}`;
  const fallbackName = clean(msg?.from?.first_name || msg?.from?.last_name || '');
  if (fallbackName) return fallbackName;
  return `user_${msg?.from?.id || 'unknown'}`;
}

function getChatType(msg) {
  return String(msg?.chat?.type || '');
}

function isGroupChat(msg) {
  const t = getChatType(msg);
  return t === 'group' || t === 'supergroup';
}

function makeConfirmCode(prefix = 'CF') {
  const rand = Math.random().toString(36).slice(2, 8).toUpperCase();
  return `${prefix}${Date.now().toString().slice(-6)}${rand}`;
}

async function sendMessage(chatId, text) {
  try {
    await telegramHttp.post('/sendMessage', {
      chat_id: chatId,
      text
    });
  } catch (err) {
    logError('Telegram sendMessage error:', err);
  }
}

async function sendDocument(chatId, filename, content) {
  try {
    const form = new FormData();
    form.append('chat_id', String(chatId));
    form.append('document', Buffer.from(content, 'utf8'), {
      filename,
      contentType: 'text/csv'
    });

    await telegramHttp.post('/sendDocument', form, {
      headers: form.getHeaders()
    });
  } catch (err) {
    logError('Telegram sendDocument error:', err);
    await sendMessage(chatId, '❌ Failed to send export file');
  }
}

function chunkMessage(text, maxLen = 3500) {
  const lines = String(text || '').split('\n');
  const chunks = [];
  let current = '';

  for (const line of lines) {
    if ((current + '\n' + line).length > maxLen) {
      if (current) chunks.push(current);
      current = line;
    } else {
      current += (current ? '\n' : '') + line;
    }
  }

  if (current) chunks.push(current);
  return chunks;
}

async function sendLongMessage(chatId, text) {
  const chunks = chunkMessage(text);
  for (const chunk of chunks) {
    await sendMessage(chatId, chunk);
  }
}

function findRowIndex(data, itemName) {
  const target = norm(itemName);
  for (let i = 0; i < data.length; i++) {
    if (norm(data[i][0]) === target) return i;
  }
  return -1;
}

function getBalanceFromRow(r) {
  const inQty = toNumber(r[1]);
  const outQty = toNumber(r[2]);
  return toNumber(r[3], inQty - outQty);
}

function getStockRowObject(r) {
  return {
    item: clean(r[0] || ''),
    inQty: toNumber(r[1]),
    outQty: toNumber(r[2]),
    balance: getBalanceFromRow(r),
    minAlert: toNumber(r[4]),
    unit: clean(r[5] || ''),
    updatedAt: clean(r[6] || '')
  };
}

function toA1Column(n) {
  let result = '';
  let x = n;
  while (x > 0) {
    const rem = (x - 1) % 26;
    result = String.fromCharCode(65 + rem) + result;
    x = Math.floor((x - 1) / 26);
  }
  return result;
}

function getActorContext(msg) {
  const username = getUsername(msg);
  return {
    username,
    actor: getActorIdentifier(msg),
    userId: String(msg?.from?.id || ''),
    chatId: String(msg?.chat?.id || ''),
    chatTitle: clean(msg?.chat?.title || msg?.chat?.first_name || ''),
    chatType: getChatType(msg)
  };
}

function canConfirmPending(role, action) {
  if (action === 'deleteitem') {
    return role === 'super_admin' || role === 'admin';
  }
  if (action === 'adjust') {
    return role === 'super_admin';
  }
  return false;
}

function buildProcessedMessageKey(messageId, chatId) {
  return `${chatId}:${messageId}`;
}

function pruneProcessedMessageCache() {
  const now = Date.now();
  for (const [key, expiresAt] of processedMessageCache) {
    if (expiresAt <= now) {
      processedMessageCache.delete(key);
    }
  }

  while (processedMessageCache.size > PROCESSED_MESSAGE_CACHE_LIMIT) {
    const oldestKey = processedMessageCache.keys().next().value;
    if (!oldestKey) break;
    processedMessageCache.delete(oldestKey);
  }
}

function hasProcessedMessageKey(key) {
  const expiresAt = processedMessageCache.get(key);
  if (!expiresAt) return false;
  if (expiresAt <= Date.now()) {
    processedMessageCache.delete(key);
    return false;
  }
  return true;
}

function beginProcessedMessage(messageId, chatId) {
  if (!messageId) return null;

  const key = buildProcessedMessageKey(messageId, chatId);
  pruneProcessedMessageCache();

  if (processedMessageInflight.has(key) || hasProcessedMessageKey(key)) {
    return null;
  }

  processedMessageInflight.add(key);
  return key;
}

function releaseProcessedMessage(key) {
  if (key) {
    processedMessageInflight.delete(key);
  }
}

function rememberProcessedMessage(key) {
  if (!key) return;
  processedMessageCache.set(key, Date.now() + PROCESSED_MESSAGE_TTL_MS);
  pruneProcessedMessageCache();
}

async function finalizeProcessedMessage(messageId, chatId, command, key) {
  if (!key) return;

  rememberProcessedMessage(key);
  releaseProcessedMessage(key);
  await runNonCriticalTask('ProcessedMessages append error:', async () => {
    await appendProcessedMessage(messageId, chatId, command);
  });
}

/**************** SHEET CONFIG ****************/
const SHEET_HEADERS = {
  Stock: ['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt'],
  Logs: ['Timestamp', 'Type', 'Item', 'Qty', 'BalanceBefore', 'BalanceAfter', 'Unit', 'ChatId', 'ChatTitle', 'Username', 'Role', 'Note'],
  Reports: ['Timestamp', 'Type', 'Details'],
  Roles: ['Username', 'Role', 'UpdatedAt'],
  AllowedChats: ['ChatId', 'ChatTitle', 'ChatType', 'AddedAt'],
  PendingActions: ['Code', 'Username', 'ChatId', 'Action', 'PayloadJson', 'Status', 'CreatedAt', 'ExpiresAt'],
  ProcessedMessages: ['MessageId', 'ChatId', 'Command', 'ProcessedAt']
};

const SHEET_RANGES = {
  Stock: "'Stock'!A2:G",
  Logs: "'Logs'!A2:L",
  Roles: "'Roles'!A2:C",
  AllowedChats: "'AllowedChats'!A2:D",
  PendingActions: "'PendingActions'!A2:H",
  ProcessedMessages: "'ProcessedMessages'!A2:D"
};

const SHEET_CACHE_KEYS = {
  Roles: 'sheet:roles',
  AllowedChats: 'sheet:allowed_chats'
};

function isCacheEntryFresh(entry) {
  return Boolean(entry) && entry.expiresAt > Date.now();
}

function primeRowCache(cacheKey, value, ttlMs = SHEET_CACHE_TTL_MS) {
  rowCache.set(cacheKey, {
    value,
    expiresAt: Date.now() + ttlMs
  });
  return value;
}

function invalidateRowCache(cacheKey) {
  if (cacheKey) {
    rowCache.delete(cacheKey);
  }
}

function invalidateSheetCache(sheetTitle) {
  invalidateRowCache(SHEET_CACHE_KEYS[sheetTitle]);
}

async function getCachedSheetRows(cacheKey, range, ttlMs = SHEET_CACHE_TTL_MS) {
  const cached = rowCache.get(cacheKey);
  if (ttlMs > 0 && isCacheEntryFresh(cached)) {
    return cached.value;
  }

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range
  });

  const values = res.data.values || [];
  if (ttlMs > 0) {
    primeRowCache(cacheKey, values, ttlMs);
  } else {
    invalidateRowCache(cacheKey);
  }

  return values;
}

function cacheSheetIds(meta) {
  sheetIdCache.clear();
  for (const sheet of meta?.sheets || []) {
    const title = sheet?.properties?.title;
    const sheetId = sheet?.properties?.sheetId;
    if (title && typeof sheetId === 'number') {
      sheetIdCache.set(title, sheetId);
    }
  }
}

async function getSpreadsheetMeta(options = {}) {
  const { force = false } = options;
  if (!force && isCacheEntryFresh(spreadsheetMetaCache)) {
    return spreadsheetMetaCache.data;
  }

  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  spreadsheetMetaCache = {
    data: meta.data,
    expiresAt: Date.now() + SHEET_CACHE_TTL_MS
  };
  cacheSheetIds(meta.data);
  return meta.data;
}

async function legacyEnsureSheetExists(title) {
  const meta = await getSpreadsheetMeta();
  const existing = (meta.sheets || []).find(s => s.properties.title === title);
  if (existing) return existing.properties.sheetId;

  const res = await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{ addSheet: { properties: { title } } }]
    }
  });

  const addedSheet = res.data.replies?.[0]?.addSheet?.properties;
  console.log(`✅ Created sheet: ${title}`);
  return addedSheet?.sheetId;
}

async function getSheetId(sheetTitle) {
  if (sheetIdCache.has(sheetTitle)) {
    return sheetIdCache.get(sheetTitle);
  }

  await getSpreadsheetMeta();
  if (sheetIdCache.has(sheetTitle)) {
    return sheetIdCache.get(sheetTitle);
  }

  throw new Error(`${sheetTitle} sheet not found`);
}

async function ensureSheetsExist(titles) {
  const meta = await getSpreadsheetMeta({ force: true });
  const existingTitles = new Set((meta.sheets || []).map(sheet => sheet.properties?.title).filter(Boolean));
  const missingTitles = titles.filter(title => !existingTitles.has(title));

  if (missingTitles.length === 0) {
    return meta;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: missingTitles.map(title => ({
        addSheet: { properties: { title } }
      }))
    }
  });

  for (const title of missingTitles) {
    console.log(`âœ… Created sheet: ${title}`);
  }

  return getSpreadsheetMeta({ force: true });
}

async function ensureHeader(title, headers) {
  const endCol = toA1Column(headers.length);
  const range = `'${title}'!A1:${endCol}1`;

  const headerRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range
  });

  const current = headerRes.data.values || [];
  if (current.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
    console.log(`✅ Header created: ${title}`);
  } else {
    console.log(`ℹ️ Header already exists: ${title}`);
  }
}

async function setupSheet() {
  await ensureSheetsExist(Object.keys(SHEET_HEADERS));
  await Promise.all(
    Object.entries(SHEET_HEADERS).map(([title, headers]) => ensureHeader(title, headers))
  );
}

/**************** DATA ACCESS ****************/
async function getData() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_RANGES.Stock
  });
  return res.data.values || [];
}

async function getLogs() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_RANGES.Logs
  });
  return res.data.values || [];
}

async function getRoles() {
  return getCachedSheetRows(SHEET_CACHE_KEYS.Roles, SHEET_RANGES.Roles);
}

async function getAllowedChats() {
  return getCachedSheetRows(SHEET_CACHE_KEYS.AllowedChats, SHEET_RANGES.AllowedChats);
}

async function getPendingActions() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_RANGES.PendingActions
  });
  return res.data.values || [];
}

async function getProcessedMessages() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_RANGES.ProcessedMessages
  });
  return res.data.values || [];
}

async function appendRow(values) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Stock'!A:G",
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  });
  invalidateSheetCache('Stock');
}

async function updateRow(rowIndex, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `'Stock'!A${rowIndex + 2}:G${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  });
  invalidateSheetCache('Stock');
}

async function appendLog(values) {
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Logs'!A:L",
      valueInputOption: 'RAW',
      requestBody: { values: [values] }
    });
  } catch (err) {
    logError('Append log error:', err);
  }
}

async function appendReport(type, details) {
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Reports'!A:C",
      valueInputOption: 'RAW',
      requestBody: { values: [[nowIso(), type, details]] }
    });
  } catch (err) {
    logError('Append report error:', err);
  }
}

async function appendProcessedMessage(messageId, chatId, command) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'ProcessedMessages'!A:D",
    valueInputOption: 'RAW',
    requestBody: {
      values: [[String(messageId), String(chatId), clean(command), nowIso()]]
    }
  });
}

async function isMessageProcessed(messageId, chatId) {
  if (!messageId) return false;

  const rows = await getProcessedMessages();
  return rows.some(r =>
    String(r[0] || '') === String(messageId) &&
    String(r[1] || '') === String(chatId)
  );
}

async function deleteRowFromSheet(sheetTitle, rowIndexZeroBasedWithoutHeader) {
  const sheetId = await getSheetId(sheetTitle);
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        deleteDimension: {
          range: {
            sheetId,
            dimension: 'ROWS',
            startIndex: rowIndexZeroBasedWithoutHeader + 1,
            endIndex: rowIndexZeroBasedWithoutHeader + 2
          }
        }
      }]
    }
  });
  invalidateSheetCache(sheetTitle);
}

/**************** ROLES ****************/
function getUserRoleFromRows(username, roleRows) {
  const u = cleanUsername(username);
  if (!u) return 'guest';

  if (BOOTSTRAP_SUPER_ADMINS.includes(u)) return 'super_admin';

  for (const r of roleRows) {
    if (cleanUsername(r[0]) === u) {
      const role = norm(r[1]);
      if (role === 'super_admin' || role === 'admin' || role === 'member') {
        return role;
      }
    }
  }
  return 'guest';
}

async function upsertRole(username, role) {
  const u = cleanUsername(username);
  if (!u) throw new Error('Username required');
  if (!['super_admin', 'admin', 'member'].includes(role)) {
    throw new Error('Invalid role');
  }

  const roleRows = await getRoles();
  let idx = -1;
  for (let i = 0; i < roleRows.length; i++) {
    if (cleanUsername(roleRows[i][0]) === u) {
      idx = i;
      break;
    }
  }

  const values = [u, role, nowIso()];

  if (idx === -1) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Roles'!A:C",
      valueInputOption: 'RAW',
      requestBody: { values: [values] }
    });
  } else {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `'Roles'!A${idx + 2}:C${idx + 2}`,
      valueInputOption: 'RAW',
      requestBody: { values: [values] }
    });
  }
  invalidateSheetCache('Roles');
}

async function removeRole(username, expectedRole = null) {
  const u = cleanUsername(username);
  const roleRows = await getRoles();

  for (let i = 0; i < roleRows.length; i++) {
    const rowUser = cleanUsername(roleRows[i][0]);
    const rowRole = norm(roleRows[i][1]);
    if (rowUser === u && (!expectedRole || rowRole === expectedRole)) {
      await deleteRowFromSheet('Roles', i);
      return true;
    }
  }
  return false;
}

/**************** ALLOWED CHATS ****************/
function isAllowedChatId(chatId, allowedChatRows) {
  return allowedChatRows.some(r => String(r[0] || '') === String(chatId));
}

async function addAllowedChat(chatId, chatTitle, chatType) {
  const rows = await getAllowedChats();
  if (isAllowedChatId(chatId, rows)) return false;

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'AllowedChats'!A:D",
    valueInputOption: 'RAW',
    requestBody: {
      values: [[String(chatId), clean(chatTitle), clean(chatType), nowIso()]]
    }
  });
  invalidateSheetCache('AllowedChats');
  return true;
}

async function removeAllowedChat(chatId) {
  const rows = await getAllowedChats();
  for (let i = 0; i < rows.length; i++) {
    if (String(rows[i][0] || '') === String(chatId)) {
      await deleteRowFromSheet('AllowedChats', i);
      return true;
    }
  }
  return false;
}

/**************** PENDING ACTIONS ****************/
async function createPendingAction(username, chatId, action, payload) {
  const code = makeConfirmCode();
  const createdAt = nowIso();
  const expiresAt = minutesFromNowIso(CONFIRM_EXPIRE_MINUTES);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'PendingActions'!A:H",
    valueInputOption: 'RAW',
    requestBody: {
      values: [[
        code,
        cleanUsername(username),
        String(chatId),
        action,
        JSON.stringify(payload),
        'pending',
        createdAt,
        expiresAt
      ]]
    }
  });

  return { code, createdAt, expiresAt };
}

async function findPendingActionByCode(code) {
  const rows = await getPendingActions();

  for (let i = 0; i < rows.length; i++) {
    if (clean(rows[i][0]) === clean(code)) {
      return {
        rowIndex: i,
        code: clean(rows[i][0]),
        username: cleanUsername(rows[i][1]),
        chatId: String(rows[i][2] || ''),
        action: clean(rows[i][3]),
        payloadJson: clean(rows[i][4]),
        status: clean(rows[i][5]),
        createdAt: clean(rows[i][6]),
        expiresAt: clean(rows[i][7])
      };
    }
  }

  return null;
}

async function updatePendingActionStatus(rowIndex, status) {
  const rows = await getPendingActions();
  const row = rows[rowIndex];
  if (!row) throw new Error('Pending action row not found');

  const newValues = [
    clean(row[0]),
    cleanUsername(row[1]),
    String(row[2] || ''),
    clean(row[3]),
    clean(row[4]),
    status,
    clean(row[6]),
    clean(row[7])
  ];

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `'PendingActions'!A${rowIndex + 2}:H${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [newValues] }
  });
}

/**************** PERMISSIONS ****************/
const ROLE_COMMANDS = {
  super_admin: new Set([
    '/start', '/help', '/myrole', '/roles', '/groups',
    '/addsuperadmin', '/removesuperadmin',
    '/addadmin', '/removeadmin',
    '/addmember', '/removemember',
    '/allowgroup', '/disallowgroup',
    '/additem', '/in', '/out', '/stock', '/allstock',
    '/alertstock', '/lowstock',
    '/setalert', '/setunit', '/renameitem', '/deleteitem',
    '/history', '/today', '/report', '/exportsummary', '/adjust',
    '/search', '/exportlogs', '/confirm', '/cancel'
  ]),
  admin: new Set([
    '/start', '/help', '/myrole',
    '/additem', '/deleteitem', '/exportsummary',
    '/in', '/out', '/stock', '/allstock', '/alertstock', '/lowstock',
    '/search', '/exportlogs', '/confirm', '/cancel'
  ]),
  member: new Set([
    '/start', '/help', '/myrole',
    '/in', '/out', '/stock', '/allstock', '/alertstock', '/lowstock',
    '/search', '/confirm', '/cancel'
  ])
};

function canUseCommand(role, command) {
  return ROLE_COMMANDS[role]?.has(command) || false;
}

/**************** ALERT ****************/
async function checkAndSendLowStockAlert(chatId, row) {
  const balance = getBalanceFromRow(row);
  const minAlert = toNumber(row[4]);
  const unit = clean(row[5] || '');
  const item = clean(row[0] || '');

  if (item && balance <= minAlert) {
    await sendMessage(
      chatId,
      `🚨 LOW STOCK ALERT\n\n` +
      `💊 Item: ${item}\n` +
      `📦 Balance: ${balance} ${unit}\n` +
      `⚠️ MinAlert: ${minAlert}\n\n` +
      `🛒 Please restock soon.`
    );
  }
}

/**************** REPORT HELPERS ****************/
function summarizeStock(data) {
  let totalItems = 0;
  let lowStockCount = 0;
  let totalBalance = 0;

  for (const r of data) {
    const row = getStockRowObject(r);
    if (!row.item) continue;
    totalItems += 1;
    totalBalance += row.balance;
    if (row.balance <= row.minAlert) lowStockCount += 1;
  }

  return { totalItems, lowStockCount, totalBalance };
}

/**************** EXECUTE CONFIRMED ACTION ****************/
async function executeConfirmedAction(pending, msg, role, actorCtx) {
  let payload;
  try {
    payload = JSON.parse(pending.payloadJson || '{}');
  } catch {
    throw new Error('Invalid pending payload');
  }

  if (pending.action === 'deleteitem') {
    const itemName = clean(payload.itemName);

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) {
        throw new Error(`Item not found: ${itemName}`);
      }

      await deleteRowFromSheet('Stock', row);
      await runNonCriticalTask('Append report error:', () =>
        appendReport('DELETE_ITEM', `By=${actorCtx.actor}, Deleted item=${itemName}`)
      );
      return `🗑️ Deleted Item: ${itemName}`;
    });
  }

  if (pending.action === 'adjust') {
    const itemName = clean(payload.itemName);
    const newBalance = Number(payload.newBalance);

    if (Number.isNaN(newBalance) || newBalance < 0) {
      throw new Error('Invalid NewBalance');
    }

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) {
        throw new Error(`Item not found: ${itemName}`);
      }

      const r = data[row];
      const currentIn = toNumber(r[1]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const oldBalance = getBalanceFromRow(r);

      if (newBalance > currentIn) {
        throw new Error(`NewBalance cannot be greater than total In (${currentIn})`);
      }

      const newOut = currentIn - newBalance;
      const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
      await updateRow(row, updated);

      await Promise.all([
        runNonCriticalTask('Append log error:', () =>
          appendLog([
            nowIso(), 'ADJUST', r[0], 0, oldBalance, newBalance, unit,
            actorCtx.chatId, actorCtx.chatTitle, actorCtx.actor, role, `Adjust balance to ${newBalance}`
          ])
        ),
        runNonCriticalTask('Append report error:', () =>
          appendReport('ADJUST', `By=${actorCtx.actor}, Item=${r[0]}, Before=${oldBalance}, After=${newBalance}`)
        )
      ]);
      await checkAndSendLowStockAlert(actorCtx.chatId, updated);

      return (
        `🛠 Balance Adjusted\n\n` +
        `💊 Item: ${r[0]}\n` +
        `📦 Old Balance: ${oldBalance} ${unit}\n` +
        `✅ New Balance: ${newBalance} ${unit}`
      );
    });
  }

  throw new Error(`Unknown pending action: ${pending.action}`);
}

/**************** COMMAND HANDLER ****************/
async function handleCommand(msg) {
  const chatId = msg.chat.id;
  const text = clean(msg.text || '');
  const parts = parsePipe(text);
  const command = norm(parts[0]);
  const actorCtx = getActorContext(msg);
  const username = actorCtx.username;
  const actor = actorCtx.actor;

  if (!command) {
    return sendMessage(chatId, '⚠️ Invalid command');
  }

  const [roleRows, allowedChatRows] = await Promise.all([getRoles(), getAllowedChats()]);
  const role = getUserRoleFromRows(username, roleRows);
  const isWriteCommand = WRITE_COMMANDS.has(command);
  const processedMessageKey = isWriteCommand ? beginProcessedMessage(msg.message_id, chatId) : null;
  let processedMessageMarked = false;

  async function markProcessedIfNeeded() {
    if (!processedMessageKey || processedMessageMarked) {
      return;
    }

    processedMessageMarked = true;
    await finalizeProcessedMessage(msg.message_id, chatId, command, processedMessageKey);
  }

  try {
    /************ GROUP WHITELIST CHECK ************/
    if (isGroupChat(msg) && !GROUP_BYPASS_COMMANDS.has(command)) {
      if (allowedChatRows.length === 0) {
      return sendMessage(
        chatId,
        '⛔ This group is not allowed yet.\nSuper Admin must run /allowgroup in this group first.'
      );
    }

      if (!isAllowedChatId(chatId, allowedChatRows)) {
      return sendMessage(
        chatId,
        '⛔ This group is not in whitelist.\nPlease ask Super Admin to run /allowgroup here.'
      );
    }
  }

  /************ PERMISSION CHECK ************/
  if (command !== '/start' && command !== '/help' && !canUseCommand(role, command)) {
    const displayUser = msg?.from?.username ? `@${msg.from.username}` : actor;
    return sendMessage(
      chatId,
      `⛔ Sorry ${displayUser}\nYou are not allowed to use ${command}\n👤 Your role: ${role}`
    );
  }

  /*
  Old duplicate idempotency block
  if (isWriteCommand && !processedMessageKey) {
      console.log(`ℹ️ Duplicate webhook ignored: message_id=${msg.message_id}, command=${command}`);
      return sendMessage(chatId, 'ℹ️ This command was already processed.');
    }
  }

  */

  /************ IDEMPOTENCY CHECK FOR WRITE COMMANDS ************/
  if (isWriteCommand && !processedMessageKey) {
    console.log(`Duplicate webhook ignored: message_id=${msg.message_id}, command=${command}`);
    return sendMessage(chatId, 'This command was already processed.');
    }

  /************ HELP / START ************/
  if (command === '/start' || command === '/help') {
    if (role === 'super_admin') {
      return sendMessage(
        chatId,
        '🤖 Stock Bot\n\n' +
        '👑 Super Admin:\n' +
        '/addsuperadmin | username\n' +
        '/removesuperadmin | username\n' +
        '/addadmin | username\n' +
        '/removeadmin | username\n' +
        '/addmember | username\n' +
        '/removemember | username\n' +
        '/roles\n' +
        '/myrole\n' +
        '/allowgroup\n' +
        '/disallowgroup\n' +
        '/groups\n' +
        '/additem | Item | MinAlert | Unit\n' +
        '/deleteitem | Item\n' +
        '/in | Item | Qty\n' +
        '/out | Item | Qty\n' +
        '/adjust | Item | NewBalance\n' +
        '/search | keyword\n' +
        '/stock | Item\n' +
        '/allstock\n' +
        '/alertstock\n' +
        '/history | Item\n' +
        '/today\n' +
        '/report\n' +
        '/exportsummary\n' +
        '/exportlogs\n' +
        '/confirm | CODE\n' +
        '/cancel | CODE'
      );
    }

    if (role === 'admin') {
      return sendMessage(
        chatId,
        '🤖 Stock Bot\n\n' +
        '🛠 Admin:\n' +
        '/additem | Item | MinAlert | Unit\n' +
        '/deleteitem | Item\n' +
        '/exportsummary\n' +
        '/exportlogs\n' +
        '/in | Item | Qty\n' +
        '/out | Item | Qty\n' +
        '/search | keyword\n' +
        '/stock | Item\n' +
        '/allstock\n' +
        '/alertstock\n' +
        '/confirm | CODE\n' +
        '/cancel | CODE\n' +
        '/myrole'
      );
    }

    if (role === 'member') {
      return sendMessage(
        chatId,
        '🤖 Stock Bot\n\n' +
        '👥 Member:\n' +
        '/in | Item | Qty\n' +
        '/out | Item | Qty\n' +
        '/search | keyword\n' +
        '/stock | Item\n' +
        '/allstock\n' +
        '/alertstock\n' +
        '/confirm | CODE\n' +
        '/cancel | CODE\n' +
        '/myrole'
      );
    }

    return sendMessage(
      chatId,
      '⛔ You are not registered to use this bot.\nPlease ask Super Admin to add your username.\n\n⚠️ Note: Telegram username is required.'
    );
  }

  if (command === '/myrole') {
    return sendMessage(
      chatId,
      `👤 Username: ${username ? '@' + username : 'no username'}\n🔐 Role: ${role}`
    );
  }

  if (command === '/roles') {
    const rows = roleRows;
    let msgOut = '👥 Roles\n\n';

    if (BOOTSTRAP_SUPER_ADMINS.length > 0) {
      msgOut += '👑 Bootstrap Super Admins:\n';
      for (const u of BOOTSTRAP_SUPER_ADMINS) {
        msgOut += `- @${u}\n`;
      }
      msgOut += '\n';
    }

    if (rows.length === 0) {
      msgOut += 'No sheet roles yet.';
      return sendMessage(chatId, msgOut);
    }

    const groups = { super_admin: [], admin: [], member: [] };

    for (const r of rows) {
      const u = cleanUsername(r[0]);
      const rr = norm(r[1]);
      if (groups[rr]) groups[rr].push(u);
    }

    msgOut += '👑 Super Admin:\n' + (groups.super_admin.map(u => `- @${u}`).join('\n') || '-') + '\n\n';
    msgOut += '🛠 Admin:\n' + (groups.admin.map(u => `- @${u}`).join('\n') || '-') + '\n\n';
    msgOut += '👥 Member:\n' + (groups.member.map(u => `- @${u}`).join('\n') || '-');

    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/groups') {
    const rows = allowedChatRows;
    if (rows.length === 0) return sendMessage(chatId, '📭 No allowed groups yet');

    let msgOut = '📋 Allowed Groups\n\n';
    for (const r of rows) {
      msgOut += `🆔 ${r[0]}\n🏷 ${clean(r[1] || '-')}\n🧩 ${clean(r[2] || '-')}\n\n`;
    }
    return sendLongMessage(chatId, msgOut.trim());
  }

  if (command === '/allowgroup') {
    if (!isGroupChat(msg)) {
      return sendMessage(chatId, '⚠️ /allowgroup can only be used inside a group');
    }

    const added = await addAllowedChat(chatId, actorCtx.chatTitle, actorCtx.chatType);
    await appendReport('ALLOW_GROUP', `ChatId=${chatId}, Title=${actorCtx.chatTitle}, Added=${added}, By=${actor}`);
    return sendMessage(
      chatId,
      added
        ? `✅ Group allowed\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}`
        : `ℹ️ Group already allowed\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}`
    );
  }

  if (command === '/disallowgroup') {
    if (!isGroupChat(msg)) {
      return sendMessage(chatId, '⚠️ /disallowgroup can only be used inside a group');
    }

    const removed = await removeAllowedChat(chatId);
    await appendReport('DISALLOW_GROUP', `ChatId=${chatId}, Title=${actorCtx.chatTitle}, Removed=${removed}, By=${actor}`);
    return sendMessage(
      chatId,
      removed
        ? `🗑️ Group removed from whitelist\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}`
        : 'ℹ️ This group was not in whitelist'
    );
  }

  if (command === '/addsuperadmin' || command === '/addadmin' || command === '/addmember') {
    if (parts.length < 2) return sendMessage(chatId, `⚠️ Usage:\n${command} | username`);

    const target = cleanUsername(parts[1]);
    const targetRole =
      command === '/addsuperadmin' ? 'super_admin' :
      command === '/addadmin' ? 'admin' : 'member';

    if (!target) return sendMessage(chatId, '⚠️ Username required');

    await upsertRole(target, targetRole);
    await appendReport('ROLE_UPSERT', `By=${actor}, User=@${target}, Role=${targetRole}`);

    return sendMessage(chatId, `✅ Role updated\n👤 @${target}\n🔐 ${targetRole}`);
  }

  if (command === '/removesuperadmin' || command === '/removeadmin' || command === '/removemember') {
    if (parts.length < 2) return sendMessage(chatId, `⚠️ Usage:\n${command} | username`);

    const target = cleanUsername(parts[1]);
    const targetRole =
      command === '/removesuperadmin' ? 'super_admin' :
      command === '/removeadmin' ? 'admin' : 'member';

    if (!target) return sendMessage(chatId, '⚠️ Username required');

    if (BOOTSTRAP_SUPER_ADMINS.includes(target) && targetRole === 'super_admin') {
      return sendMessage(chatId, '⚠️ Cannot remove bootstrap super admin from env');
    }

    const removed = await removeRole(target, targetRole);
    await appendReport('ROLE_REMOVE', `By=${actor}, User=@${target}, Role=${targetRole}, Removed=${removed}`);

    return sendMessage(
      chatId,
      removed
        ? `🗑️ Role removed\n👤 @${target}\n🔐 ${targetRole}`
        : `ℹ️ User not found in role list\n👤 @${target}\n🔐 ${targetRole}`
    );
  }

  /*
  if (command === '/confirm') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/confirm | CODE');

    const code = clean(parts[1]);
    const pending = await findPendingActionByCode(code);

    if (!pending) return sendMessage(chatId, '❌ Confirmation code not found');
    if (pending.status !== 'pending') return sendMessage(chatId, `ℹ️ This request is already ${pending.status}`);
    if (pending.username !== username) return sendMessage(chatId, '⛔ This confirmation code is not yours');
    if (pending.chatId !== String(chatId)) return sendMessage(chatId, '⛔ This confirmation code belongs to another chat');
    if (isExpired(pending.expiresAt)) {
      await updatePendingActionStatus(pending.rowIndex, 'expired');
      return sendMessage(chatId, '⌛ Confirmation code expired');
    }

    if (!canConfirmPending(role, pending.action)) {
      return sendMessage(chatId, `⛔ You no longer have permission to confirm "${pending.action}"`);
    }

    try {
      const resultMessage = await executeConfirmedAction(pending, msg, role, actorCtx);
      await updatePendingActionStatus(pending.rowIndex, 'confirmed');
      return sendMessage(chatId, `✅ Confirmed\nCode: ${code}\n\n${resultMessage}`);
    } catch (err) {
      await updatePendingActionStatus(pending.rowIndex, 'failed');
      return sendMessage(chatId, `❌ Failed to execute confirmation\nCode: ${code}\nReason: ${err.message}`);
    }
  }

  */

  if (command === '/confirm') {
    if (parts.length < 2) return sendMessage(chatId, 'Usage:\n/confirm | CODE');

    if (parts.length < 2) return sendMessage(chatId, 'Usage:\n/confirm | CODE');
    const code = clean(parts[1]);

    return await withLock(`pending:${code}`, async () => {
      const pending = await findPendingActionByCode(code);
      if (!pending) return sendMessage(chatId, 'Confirmation code not found');
      if (pending.status !== 'pending') return sendMessage(chatId, `This request is already ${pending.status}`);
      if (pending.username !== username) return sendMessage(chatId, 'This confirmation code is not yours');
      if (pending.chatId !== String(chatId)) return sendMessage(chatId, 'This confirmation code belongs to another chat');
      if (isExpired(pending.expiresAt)) {
        await updatePendingActionStatus(pending.rowIndex, 'expired');
        await markProcessedIfNeeded();
        return sendMessage(chatId, 'Confirmation code expired');
      }

      if (!canConfirmPending(role, pending.action)) {
        return sendMessage(chatId, `You no longer have permission to confirm "${pending.action}"`);
      }

      try {
        const resultMessage = await executeConfirmedAction(pending, msg, role, actorCtx);
        await updatePendingActionStatus(pending.rowIndex, 'confirmed');
        await markProcessedIfNeeded();
        return sendMessage(chatId, `Confirmed\nCode: ${code}\n\n${resultMessage}`);
      } catch (err) {
        await runNonCriticalTask('PendingActions update error:', () =>
          updatePendingActionStatus(pending.rowIndex, 'failed')
        );
        return sendMessage(chatId, `Failed to execute confirmation\nCode: ${code}\nReason: ${err.message}`);
      }
    });
  }

  /*
  if (command === '/cancel') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/cancel | CODE');

    const code = clean(parts[1]);
    const pending = await findPendingActionByCode(code);

    if (!pending) return sendMessage(chatId, '❌ Confirmation code not found');
    if (pending.status !== 'pending') return sendMessage(chatId, `ℹ️ This request is already ${pending.status}`);
    if (pending.username !== username) return sendMessage(chatId, '⛔ This confirmation code is not yours');
    if (pending.chatId !== String(chatId)) return sendMessage(chatId, '⛔ This confirmation code belongs to another chat');

    await updatePendingActionStatus(pending.rowIndex, 'cancelled');
    return sendMessage(chatId, `🛑 Cancelled\nCode: ${code}`);
  }

  */

  if (command === '/cancel') {
    if (parts.length < 2) return sendMessage(chatId, 'Usage:\n/cancel | CODE');

    const code = clean(parts[1]);

    return await withLock(`pending:${code}`, async () => {
      const pending = await findPendingActionByCode(code);

      if (!pending) return sendMessage(chatId, 'Confirmation code not found');
      if (pending.status !== 'pending') return sendMessage(chatId, `This request is already ${pending.status}`);
      if (pending.username !== username) return sendMessage(chatId, 'This confirmation code is not yours');
      if (pending.chatId !== String(chatId)) return sendMessage(chatId, 'This confirmation code belongs to another chat');

      await updatePendingActionStatus(pending.rowIndex, 'cancelled');
      await markProcessedIfNeeded();
      return sendMessage(chatId, `Cancelled\nCode: ${code}`);
    });
  }

  if (command === '/additem') {
    if (parts.length < 4) {
      return sendMessage(chatId, '⚠️ Usage:\n/additem | Item Name | MinAlert | Unit');
    }

    const itemName = clean(parts[1]);
    const minAlert = Number(parts[2]);
    const unit = clean(parts[3]);

    if (!isValidItemName(itemName)) {
      return sendMessage(chatId, '⚠️ Invalid item name\n- Required\n- Max 100 chars\n- No newline\n- "|" is not allowed');
    }
    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }
    if (!unit) return sendMessage(chatId, '⚠️ Unit required');

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const existing = findRowIndex(data, itemName);
      if (existing !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${itemName}`);

      await appendRow([itemName, 0, 0, 0, minAlert, unit, nowIso()]);
      await markProcessedIfNeeded();
      await runNonCriticalTask('Append report error:', () =>
        appendReport('ADD_ITEM', `By=${actor}, Item=${itemName}, MinAlert=${minAlert}, Unit=${unit}`)
      );

      return sendMessage(
        chatId,
        `✅ Item Added\n\n💊 Item: ${itemName}\n⚠️ MinAlert: ${minAlert}\n📦 Unit: ${unit}`
      );
    });
  }

  if (command === '/in') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/in | Item Name | Qty');

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
    if (Number.isNaN(qty) || qty <= 0) return sendMessage(chatId, '⚠️ Qty must be greater than 0');

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

      const r = data[row];
      const currentIn = toNumber(r[1]);
      const currentOut = toNumber(r[2]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const oldBalance = getBalanceFromRow(r);

      const newIn = currentIn + qty;
      const newBalance = newIn - currentOut;

      const updated = [r[0], newIn, currentOut, newBalance, minAlert, unit, nowIso()];
      await updateRow(row, updated);
      await markProcessedIfNeeded();

      await Promise.all([
        runNonCriticalTask('Append log error:', () =>
          appendLog([
            nowIso(), 'IN', r[0], qty, oldBalance, newBalance, unit,
            actorCtx.chatId, actorCtx.chatTitle, actor, role, ''
          ])
        ),
        runNonCriticalTask('Append report error:', () =>
          appendReport('IN', `By=${actor}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}`)
        )
      ]);

      return sendMessage(
        chatId,
        `📥 Stock Updated\n\n💊 Item: ${r[0]}\n➕ Qty In: ${qty}\n📦 Balance: ${newBalance} ${unit}`
      );
    });
  }

  if (command === '/out') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/out | Item Name | Qty');

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
    if (Number.isNaN(qty) || qty <= 0) return sendMessage(chatId, '⚠️ Qty must be greater than 0');

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

      const r = data[row];
      const currentIn = toNumber(r[1]);
      const currentOut = toNumber(r[2]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const oldBalance = getBalanceFromRow(r);

      if (qty > oldBalance) {
        return sendMessage(chatId, `❌ Not enough stock\n📦 Balance: ${oldBalance} ${unit}`);
      }

      const newOut = currentOut + qty;
      const newBalance = currentIn - newOut;

      const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
      await updateRow(row, updated);
      await markProcessedIfNeeded();

      await Promise.all([
        runNonCriticalTask('Append log error:', () =>
          appendLog([
            nowIso(), 'OUT', r[0], qty, oldBalance, newBalance, unit,
            actorCtx.chatId, actorCtx.chatTitle, actor, role, ''
          ])
        ),
        runNonCriticalTask('Append report error:', () =>
          appendReport('OUT', `By=${actor}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}`)
        )
      ]);

      await sendMessage(
        chatId,
        `📤 Stock Updated\n\n💊 Item: ${r[0]}\n➖ Qty Out: ${qty}\n📦 Balance: ${newBalance} ${unit}`
      );

      await checkAndSendLowStockAlert(chatId, updated);
      return;
    });
  }

  if (command === '/adjust') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/adjust | Item Name | NewBalance');

    const itemName = clean(parts[1]);
    const newBalance = Number(parts[2]);

    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
    if (Number.isNaN(newBalance) || newBalance < 0) {
      return sendMessage(chatId, '⚠️ NewBalance must be a valid number >= 0');
    }

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

      const r = data[row];
      const currentIn = toNumber(r[1]);
      const currentBalance = getBalanceFromRow(r);
      const unit = clean(r[5] || '');

      if (newBalance > currentIn) {
        return sendMessage(
          chatId,
          `⚠️ NewBalance cannot be greater than total In (${currentIn}).\nCurrent design keeps In unchanged and recalculates Out.`
        );
      }

      const pending = await createPendingAction(username, chatId, 'adjust', {
        itemName,
        newBalance
      });
      await markProcessedIfNeeded();

      return sendMessage(
        chatId,
        `⚠️ Confirm Adjust Required\n\n` +
        `💊 Item: ${r[0]}\n` +
        `📦 Current Balance: ${currentBalance} ${unit}\n` +
        `✅ New Balance: ${newBalance} ${unit}\n\n` +
        `🧾 Code: ${pending.code}\n` +
        `⏳ Expires in ${CONFIRM_EXPIRE_MINUTES} minutes\n\n` +
        `Confirm:\n/confirm | ${pending.code}\n\n` +
        `Cancel:\n/cancel | ${pending.code}`
      );
    });
  }

  if (command === '/search') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/search | keyword');

    const keyword = norm(parts[1]);
    if (!keyword) return sendMessage(chatId, '⚠️ Keyword required');

    const data = await getData();
    const matched = data.filter(r => norm(r[0]).includes(keyword));

    if (matched.length === 0) {
      return sendMessage(chatId, `📭 No items found for: ${parts[1]}`);
    }

    let msgOut = `🔎 Search Result: ${parts[1]}\n\n`;
    for (const r of matched.slice(0, 50)) {
      const item = clean(r[0] || '');
      const balance = getBalanceFromRow(r);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const status = balance <= minAlert ? '🚨LOW' : '✅OK';
      msgOut += `💊 ${item}\n📦 ${balance} ${unit} | ⚠️ Min: ${minAlert} | ${status}\n\n`;
    }

    if (matched.length > 50) {
      msgOut += `...and ${matched.length - 50} more items`;
    }

    return sendLongMessage(chatId, msgOut.trim());
  }

  if (command === '/stock') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/stock | Item Name');

    const itemName = clean(parts[1]);
    const data = await getData();
    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const balance = getBalanceFromRow(r);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const lowMark = balance <= minAlert ? '\n🚨 Status: LOW STOCK' : '\n✅ Status: OK';

    return sendMessage(
      chatId,
      `📊 Stock Info\n\n` +
      `💊 Item: ${r[0]}\n` +
      `📥 In: ${currentIn}\n` +
      `📤 Out: ${currentOut}\n` +
      `📦 Balance: ${balance} ${unit}\n` +
      `⚠️ MinAlert: ${minAlert}` +
      lowMark
    );
  }

  if (command === '/allstock') {
    const data = await getData();
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data');

    let msgOut = '📋 All Stock\n\n';
    for (const r of data) {
      const item = clean(r[0] || '');
      const balance = getBalanceFromRow(r);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const status = balance <= minAlert ? ' 🚨LOW' : ' ✅OK';
      msgOut += `💊 ${item}: ${balance} ${unit}${status}\n`;
    }
    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/lowstock' || command === '/alertstock') {
    const data = await getData();
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data');

    const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4]));
    if (lowItems.length === 0) return sendMessage(chatId, '✅ No low stock items');

    let msgOut = '🚨 Low Stock Items\n\n';
    for (const r of lowItems) {
      const item = clean(r[0] || '');
      const balance = getBalanceFromRow(r);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      msgOut += `💊 ${item}: ${balance} ${unit} (Min: ${minAlert})\n`;
    }
    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/setalert') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/setalert | Item Name | MinAlert');

    const itemName = clean(parts[1]);
    const minAlert = Number(parts[2]);

    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

      const r = data[row];
      const currentIn = toNumber(r[1]);
      const currentOut = toNumber(r[2]);
      const balance = getBalanceFromRow(r);
      const unit = clean(r[5] || '');

      await updateRow(row, [r[0], currentIn, currentOut, balance, minAlert, unit, nowIso()]);
      await markProcessedIfNeeded();
      await runNonCriticalTask('Append report error:', () =>
        appendReport('SET_ALERT', `By=${actor}, Item=${r[0]}, MinAlert=${minAlert}`)
      );

      return sendMessage(
        chatId,
        `✅ MinAlert Updated\n\n💊 Item: ${r[0]}\n⚠️ New MinAlert: ${minAlert}`
      );
    });
  }

  if (command === '/setunit') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/setunit | Item Name | Unit');

    const itemName = clean(parts[1]);
    const unit = clean(parts[2]);

    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
    if (!unit) return sendMessage(chatId, '⚠️ Unit required');

    return await withLock(`stock:${itemName}`, async () => {
      const data = await getData();
      const row = findRowIndex(data, itemName);
      if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

      const r = data[row];
      await updateRow(row, [r[0], toNumber(r[1]), toNumber(r[2]), getBalanceFromRow(r), toNumber(r[4]), unit, nowIso()]);
      await markProcessedIfNeeded();
      await runNonCriticalTask('Append report error:', () =>
        appendReport('SET_UNIT', `By=${actor}, Item=${r[0]}, Unit=${unit}`)
      );

      return sendMessage(
        chatId,
        `✅ Unit Updated\n\n💊 Item: ${r[0]}\n📦 New Unit: ${unit}`
      );
    });
  }

  if (command === '/renameitem') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/renameitem | Old Name | New Name');

    const oldName = clean(parts[1]);
    const newName = clean(parts[2]);

    if (!isValidItemName(oldName) || !isValidItemName(newName)) {
      return sendMessage(chatId, '⚠️ Invalid old name or new name');
    }

    return await withLock(`stock:${oldName}`, async () => {
      return await withLock(`stock:${newName}`, async () => {
        const data = await getData();

        const oldRow = findRowIndex(data, oldName);
        if (oldRow === -1) return sendMessage(chatId, `❌ Item not found: ${oldName}`);

        const existingNew = findRowIndex(data, newName);
        if (existingNew !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${newName}`);

        const r = data[oldRow];
        await updateRow(oldRow, [
          newName,
          toNumber(r[1]),
          toNumber(r[2]),
          getBalanceFromRow(r),
          toNumber(r[4]),
          clean(r[5] || ''),
          nowIso()
        ]);

        await markProcessedIfNeeded();
        await runNonCriticalTask('Append report error:', () =>
          appendReport('RENAME_ITEM', `By=${actor}, ${oldName} -> ${newName}`)
        );
        return sendMessage(chatId, `✅ Item Renamed\n\n📝 Old: ${oldName}\n✨ New: ${newName}`);
      });
    });
  }

  if (command === '/deleteitem') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/deleteitem | Item Name');

    const itemName = clean(parts[1]);
    if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');

    const data = await getData();
    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const pending = await createPendingAction(username, chatId, 'deleteitem', {
      itemName
    });
    await markProcessedIfNeeded();

    return sendMessage(
      chatId,
      `⚠️ Confirm Delete Required\n\n` +
      `💊 Item: ${itemName}\n\n` +
      `🧾 Code: ${pending.code}\n` +
      `⏳ Expires in ${CONFIRM_EXPIRE_MINUTES} minutes\n\n` +
      `Confirm:\n/confirm | ${pending.code}\n\n` +
      `Cancel:\n/cancel | ${pending.code}`
    );
  }

  if (command === '/history') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/history | Item Name');

    const itemName = clean(parts[1]);
    const logs = await getLogs();
    const filtered = logs.filter(r => norm(r[2]) === norm(itemName));

    if (filtered.length === 0) {
      return sendMessage(chatId, `📭 No history found for: ${itemName}`);
    }

    const recent = filtered.slice(-20);
    let msgOut = `📜 History: ${itemName}\n\n`;

    for (const r of recent) {
      const ts = clean(r[0] || '');
      const type = clean(r[1] || '');
      const qty = toNumber(r[3]);
      const beforeBal = toNumber(r[4]);
      const afterBal = toNumber(r[5]);
      const unit = clean(r[6] || '');
      const user = clean(r[9] || '');
      const roleUsed = clean(r[10] || '');
      const note = clean(r[11] || '');

      const emoji =
        type === 'IN' ? '📥' :
        type === 'OUT' ? '📤' :
        type === 'ADJUST' ? '🛠' : '📝';

      msgOut += `${emoji} ${type} | Before: ${beforeBal} | After: ${afterBal} ${unit}\n`;
      if (qty) msgOut += `🔢 Qty: ${qty}\n`;
      msgOut += `🕒 ${ts}`;
      if (user) msgOut += ` | 👤 ${user}`;
      if (roleUsed) msgOut += ` | 🔐 ${roleUsed}`;
      if (note) msgOut += `\n📝 ${note}`;
      msgOut += '\n\n';
    }

    return sendLongMessage(chatId, msgOut.trim());
  }

  if (command === '/today') {
    const logs = await getLogs();
    const today = getLocalDateString(APP_TIMEZONE);
    const todayLogs = logs.filter(r => {
      const ts = clean(r[0] || '');
      if (!ts) return false;
      const d = new Date(ts);
      if (Number.isNaN(d.getTime())) return false;
      return getLocalDateStringFromDate(d, APP_TIMEZONE) === today;
    });

    if (todayLogs.length === 0) {
      return sendMessage(chatId, `📭 No transactions today (${today}, ${APP_TIMEZONE})`);
    }

    let inCount = 0;
    let outCount = 0;
    let adjustCount = 0;
    let inQty = 0;
    let outQty = 0;

    for (const r of todayLogs) {
      const type = clean(r[1] || '');
      const qty = toNumber(r[3]);

      if (type === 'IN') {
        inCount += 1;
        inQty += qty;
      } else if (type === 'OUT') {
        outCount += 1;
        outQty += qty;
      } else if (type === 'ADJUST') {
        adjustCount += 1;
      }
    }

    const msgOut =
      `📅 Today Summary (${today}, ${APP_TIMEZONE})\n\n` +
      `📥 IN Transactions: ${inCount}\n` +
      `➕ Total IN Qty: ${inQty}\n\n` +
      `📤 OUT Transactions: ${outCount}\n` +
      `➖ Total OUT Qty: ${outQty}\n\n` +
      `🛠 Adjust Transactions: ${adjustCount}\n\n` +
      `🧾 Total Transactions: ${todayLogs.length}`;

    return sendMessage(chatId, msgOut);
  }

  if (command === '/report') {
    const data = await getData();
    const summary = summarizeStock(data);

    let msgOut =
      `📊 Stock Report\n\n` +
      `📦 Total Items: ${summary.totalItems}\n` +
      `🚨 Low Stock Items: ${summary.lowStockCount}\n` +
      `🔢 Total Balance Qty: ${summary.totalBalance}\n\n`;

    const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4]));
    if (lowItems.length > 0) {
      msgOut += `🚨 Low Stock List:\n`;
      for (const r of lowItems) {
        msgOut += `💊 ${r[0]}: ${getBalanceFromRow(r)} ${clean(r[5] || '')} (Min: ${toNumber(r[4])})\n`;
      }
    } else {
      msgOut += `✅ No low stock items`;
    }

    await appendReport(
      'REPORT_VIEW',
      `By=${actor}, TotalItems=${summary.totalItems}, LowStock=${summary.lowStockCount}, TotalBalance=${summary.totalBalance}`
    );

    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/exportsummary') {
    const data = await getData();
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data to export');

    const rows = [['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt']];
    for (const r of data) {
      rows.push([
        clean(r[0] || ''),
        toNumber(r[1]),
        toNumber(r[2]),
        getBalanceFromRow(r),
        toNumber(r[4]),
        clean(r[5] || ''),
        clean(r[6] || '')
      ]);
    }

    const csv = rows.map(row => row.map(escapeCsv).join(',')).join('\n');
    await appendReport('EXPORT_SUMMARY', `By=${actor}, Exported ${data.length} items`);

    return sendDocument(chatId, `stock-summary-${getLocalDateString(APP_TIMEZONE)}.csv`, csv);
  }

  if (command === '/exportlogs') {
    const logs = await getLogs();
    if (logs.length === 0) return sendMessage(chatId, '📭 No logs to export');

    const rows = [[
      'Timestamp', 'Type', 'Item', 'Qty', 'BalanceBefore', 'BalanceAfter',
      'Unit', 'ChatId', 'ChatTitle', 'Username', 'Role', 'Note'
    ]];

    for (const r of logs) {
      rows.push([
        clean(r[0] || ''),
        clean(r[1] || ''),
        clean(r[2] || ''),
        toNumber(r[3]),
        toNumber(r[4]),
        toNumber(r[5]),
        clean(r[6] || ''),
        clean(r[7] || ''),
        clean(r[8] || ''),
        clean(r[9] || ''),
        clean(r[10] || ''),
        clean(r[11] || '')
      ]);
    }

    const csv = rows.map(row => row.map(escapeCsv).join(',')).join('\n');
    await appendReport('EXPORT_LOGS', `By=${actor}, Exported ${logs.length} logs`);

    return sendDocument(chatId, `stock-logs-${getLocalDateString(APP_TIMEZONE)}.csv`, csv);
  }

  return sendMessage(chatId, '❌ Unknown command. Use /help');
  } finally {
    if (processedMessageKey && !processedMessageMarked) {
      releaseProcessedMessage(processedMessageKey);
    }
  }
}

/**************** WEBHOOK ****************/
app.post('/webhook', async (req, res) => {
  try {
    const msg = req.body.message;
    if (!msg || !msg.text) return res.sendStatus(200);

    await handleCommand(msg);
    return res.sendStatus(200);
  } catch (err) {
    console.error('Webhook error:', err.response?.data || err.message || err);
    return res.sendStatus(200);
  }
});

app.get('/', (req, res) => {
  res.status(200).send('✅ Bot running');
});

app.get('/webhook', (req, res) => {
  res.status(200).send('✅ Webhook endpoint is working');
});

/**************** START SERVER ****************/
async function start() {
  await setupSheet();
  console.log('✅ setupSheet done');

  app.listen(PORT, () => {
    console.log('🚀 Server running on port ' + PORT);
    console.log('🌍 Timezone: ' + APP_TIMEZONE);
  });
}

start().catch(err => {
  console.error('❌ Startup error:', err.response?.data || err.message || err);
  process.exit(1);
});
