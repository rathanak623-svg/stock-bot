
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
if (!process.env.TELEGRAM_WEBHOOK_SECRET) throw new Error('Missing TELEGRAM_WEBHOOK_SECRET');
if (!process.env.SPREADSHEET_ID) throw new Error('Missing SPREADSHEET_ID');
if (!process.env.GOOGLE_CLIENT_EMAIL) throw new Error('Missing GOOGLE_CLIENT_EMAIL');
if (!process.env.GOOGLE_PRIVATE_KEY) throw new Error('Missing GOOGLE_PRIVATE_KEY');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const PORT = Number(process.env.PORT || 10000);
const APP_TIMEZONE = process.env.APP_TIMEZONE || 'Asia/Phnom_Penh';

const TELEGRAM_TIMEOUT_MS = getEnvNumber('TELEGRAM_TIMEOUT_MS', 15000, 1000);
const SHEET_CACHE_TTL_MS = getEnvNumber('SHEET_CACHE_TTL_MS', 5000, 0);
const PROCESSED_MESSAGE_TTL_MS = getEnvNumber('PROCESSED_MESSAGE_TTL_MS', 30 * 60 * 1000, 1000);
const PROCESSED_MESSAGE_CACHE_LIMIT = getEnvNumber('PROCESSED_MESSAGE_CACHE_LIMIT', 5000, 100);

const CONFIRM_EXPIRE_MINUTES = getEnvNumber('CONFIRM_EXPIRE_MINUTES', 10, 1);
const UNDO_WINDOW_MINUTES = getEnvNumber('UNDO_WINDOW_MINUTES', 30, 1);

const RATE_LIMIT_WINDOW_MS = getEnvNumber('RATE_LIMIT_WINDOW_MS', 2000, 100);
const RATE_LIMIT_MAX_WRITES = getEnvNumber('RATE_LIMIT_MAX_WRITES', 3, 1);

const PROCESSED_MESSAGE_RETENTION_DAYS = getEnvNumber('PROCESSED_MESSAGE_RETENTION_DAYS', 14, 1);
const PENDING_ACTION_RETENTION_DAYS = getEnvNumber('PENDING_ACTION_RETENTION_DAYS', 30, 1);
const CLEANUP_INTERVAL_MS = getEnvNumber('CLEANUP_INTERVAL_MS', 10 * 60 * 1000, 60 * 1000);

const DAILY_REPORT_HOUR = getEnvNumber('DAILY_REPORT_HOUR', 8, 0);
const DAILY_REPORT_MINUTE = getEnvNumber('DAILY_REPORT_MINUTE', 0, 0);
const DAILY_REPORT_CHECK_INTERVAL_MS = getEnvNumber('DAILY_REPORT_CHECK_INTERVAL_MS', 60 * 1000, 30 * 1000);

const INPUT_SESSION_TIMEOUT_MS = getEnvNumber('INPUT_SESSION_TIMEOUT_MS', 10 * 60 * 1000, 60 * 1000);

const BOOTSTRAP_SUPER_ADMINS = (process.env.SUPER_ADMINS || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;

const GROUP_BYPASS_COMMANDS = new Set([
  '/allowgroup', '/start', '/help', '/myrole', '/menu', '/cancelinput'
]);

const WRITE_COMMANDS = new Set([
  '/addsuperadmin', '/removesuperadmin',
  '/addadmin', '/removeadmin',
  '/addmember', '/removemember',
  '/allowgroup', '/disallowgroup',
  '/additem', '/deleteitem', '/in', '/out', '/adjust',
  '/setalert', '/setunit', '/renameitem', '/confirm', '/cancel',
  '/undo', '/setdailyreport', '/setalerts',
  '/additemsbulk', '/inbulk', '/outbulk',
  '/report', '/exportsummary', '/exportlogs'
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
const rateLimitCache = new Map();
const inputSessionCache = new Map();

let spreadsheetMetaCache = {
  data: null,
  expiresAt: 0
};

/**************** SIMPLE IN-MEMORY LOCKS ****************/
const itemLocks = new Map();

async function withLock(key, fn) {
  const safeKey = String(key || '').trim().toLowerCase();
  const prev = itemLocks.get(safeKey) || Promise.resolve();

  let release;
  const gate = new Promise(resolve => {
    release = resolve;
  });

  const current = prev.then(() => gate);
  itemLocks.set(safeKey, current);

  try {
    await prev;
    return await fn();
  } finally {
    release();
    if (itemLocks.get(safeKey) === current) {
      itemLocks.delete(safeKey);
    }
  }
}

async function withStockSheetWriteLock(fn) {
  return withLock('sheet:stock:write', fn);
}
async function withPendingActionsLock(fn) {
  return withLock('sheet:pending-actions:write', fn);
}
async function withProcessedMessagesLock(fn) {
  return withLock('sheet:processed-messages:write', fn);
}
async function withRolesLock(fn) {
  return withLock('sheet:roles:write', fn);
}
async function withAllowedChatsLock(fn) {
  return withLock('sheet:allowed-chats:write', fn);
}
async function withGroupSettingsLock(fn) {
  return withLock('sheet:group-settings:write', fn);
}

/**************** HELPERS ****************/
function clean(text) {
  return String(text || '')
    .replace(/\u00A0/g, ' ')
    .replace(/[\u200B\u200C\u200D\u2060\uFEFF]/g, '')
    .trim();
}
function compact(text) {
  return clean(text).replace(/\s+/g, ' ');
}
function norm(text) {
  return compact(text).toLowerCase();
}
function normalizeItemName(text) {
  return norm(text);
}
function cleanUsername(text) {
  return clean(text).replace(/^@+/, '').toLowerCase();
}
function normalizeCommand(raw) {
  return norm(raw).split('@')[0];
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
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
function minutesFromNowIso(minutes) {
  return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}
function daysAgoIso(days) {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}
function isExpired(isoString) {
  if (!isoString) return true;
  const t = new Date(isoString).getTime();
  return Number.isNaN(t) || t < Date.now();
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
function getTimeParts(timeZone = APP_TIMEZONE, date = new Date()) {
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = fmt.formatToParts(date);
  const hour = Number(parts.find(p => p.type === 'hour')?.value || 0);
  const minute = Number(parts.find(p => p.type === 'minute')?.value || 0);
  return { hour, minute };
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
function isValidUnit(unit) {
  const s = clean(unit);
  return Boolean(s) && s.length <= 30 && !/[\r\n]/.test(s) && !s.includes('|');
}
function escapeCsv(value) {
  let s = String(value ?? '');
  if (/^[=+\-@]/.test(s)) s = "'" + s;
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
  const fallbackName = compact(
    [msg?.from?.first_name || '', msg?.from?.last_name || '']
      .filter(Boolean)
      .join(' ')
  );
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
function assertValidQty(qty) {
  return Number.isFinite(qty) && qty > 0;
}
function parseQtyAndOptionalNote(parts) {
  const qty = Number(parts[2]);
  const note = parts.length >= 4 ? clean(parts.slice(3).join(' | ')) : '';
  return { qty, note };
}
function parseOnOff(value) {
  const v = norm(value);
  if (['on', 'true', '1', 'yes', 'enable', 'enabled'].includes(v)) return true;
  if (['off', 'false', '0', 'no', 'disable', 'disabled'].includes(v)) return false;
  return null;
}
function getDailyReplyKeyboard(role) {
  return {
    keyboard: [
      [{ text: '/allstock' }],
      [{ text: '/lowstock' }],
      [{ text: '/restocklist' }],
      [{ text: '/Menu' }]
    ],
    resize_keyboard: true,
    persistent: true,
    input_field_placeholder: 'ជ្រើស command ឬវាយ command...'
  };
}
function buildPendingActionInlineKeyboard(code) {
  return {
    inline_keyboard: [[
      { text: '✅ Confirm', callback_data: `CONFIRM|${code}` },
      { text: '❌ Cancel', callback_data: `CANCEL|${code}` }
    ]]
  };
}
function buildQuickActionsInlineKeyboard(role = 'guest') {
  const rows = [];

  if (role === 'super_admin' || role === 'admin') {
    rows.push(
      [{ text: '➕ ADD ITEM', callback_data: 'QA|ADDITEM' }, { text: '📥 IN', callback_data: 'QA|IN' }],
      [{ text: '📤 OUT', callback_data: 'QA|OUT' }, { text: '📊 STOCK', callback_data: 'QA|STOCK' }],
      [{ text: '🚨 LOW STOCK', callback_data: 'QA|LOWSTOCK' }, { text: '🗂 ALL STOCK', callback_data: 'QA|ALLSTOCK' }],
      [{ text: '🛒 RESTOCK', callback_data: 'QA|RESTOCKLIST' }]
    );
  } else {
    rows.push(
      [{ text: '📥 IN', callback_data: 'QA|IN' }, { text: '📤 OUT', callback_data: 'QA|OUT' }],
      [{ text: '📊 STOCK', callback_data: 'QA|STOCK' }, { text: '🚨 LOW STOCK', callback_data: 'QA|LOWSTOCK' }],
      [{ text: '🗂 ALL STOCK', callback_data: 'QA|ALLSTOCK' }, { text: '🛒 RESTOCK', callback_data: 'QA|RESTOCKLIST' }]
    );
  }

  return { inline_keyboard: rows };
}
function buildCallbackMsgLike(callbackQuery) {
  return {
    message_id: callbackQuery?.message?.message_id,
    from: callbackQuery?.from,
    chat: callbackQuery?.message?.chat,
    text: callbackQuery?.message?.text || ''
  };
}
function getCallbackFeedback(resultMessage, fallbackOk = 'Done') {
  const text = compact(resultMessage || '');
  if (
    text.startsWith('Confirmed') ||
    text.startsWith('Cancelled') ||
    text.startsWith('✅') ||
    text.startsWith('🗑️') ||
    text.startsWith('🛠')
  ) {
    return { text: fallbackOk, showAlert: false };
  }
  if (
    text.includes('not allowed') ||
    text.includes('not yours') ||
    text.includes('expired') ||
    text.includes('failed') ||
    text.includes('invalid') ||
    text.includes('another chat') ||
    text.includes('already')
  ) {
    return { text: text.slice(0, 180) || 'Action failed', showAlert: true };
  }
  return { text: text.slice(0, 120) || fallbackOk, showAlert: false };
}
async function sendMessage(chatId, text, options = {}) {
  try {
    const payload = { chat_id: chatId, text };
    if (options.replyMarkup) payload.reply_markup = options.replyMarkup;
    if (options.replyToMessageId) payload.reply_to_message_id = options.replyToMessageId;
    await telegramHttp.post('/sendMessage', payload);
  } catch (err) {
    logError('Telegram sendMessage error:', err);
  }
}
async function answerCallbackQuery(callbackQueryId, text, showAlert = false) {
  try {
    await telegramHttp.post('/answerCallbackQuery', {
      callback_query_id: callbackQueryId,
      text,
      show_alert: showAlert
    });
  } catch (err) {
    logError('Telegram answerCallbackQuery error:', err);
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
    await telegramHttp.post('/sendDocument', form, { headers: form.getHeaders() });
  } catch (err) {
    logError('Telegram sendDocument error:', err);
    await sendMessage(chatId, '❌ Failed to send export file');
  }
}
async function editMessageReplyMarkup(chatId, messageId, replyMarkup = null) {
  try {
    const payload = { chat_id: chatId, message_id: messageId };
    if (replyMarkup !== null) payload.reply_markup = replyMarkup;
    await telegramHttp.post('/editMessageReplyMarkup', payload);
  } catch (err) {
    logError('Telegram editMessageReplyMarkup error:', err);
  }
}
async function clearInlineKeyboardFromCallback(callbackQuery) {
  const chatId = callbackQuery?.message?.chat?.id;
  const messageId = callbackQuery?.message?.message_id;
  if (!chatId || !messageId) return;
  await editMessageReplyMarkup(chatId, messageId, { inline_keyboard: [] });
}
function chunkMessage(text, maxLen = 3500) {
  const input = String(text || '');
  if (!input) return [''];
  const lines = input.split('\n');
  const chunks = [];
  let current = '';
  const pushCurrent = () => {
    if (current) {
      chunks.push(current);
      current = '';
    }
  };
  for (let line of lines) {
    if (line.length > maxLen) {
      pushCurrent();
      while (line.length > maxLen) {
        chunks.push(line.slice(0, maxLen));
        line = line.slice(maxLen);
      }
      if (line) current = line;
      continue;
    }
    const candidate = current ? `${current}\n${line}` : line;
    if (candidate.length > maxLen) {
      pushCurrent();
      current = line;
    } else {
      current = candidate;
    }
  }
  pushCurrent();
  return chunks.length ? chunks : [''];
}
async function sendLongMessage(chatId, text, options = {}) {
  const chunks = chunkMessage(text);
  for (let i = 0; i < chunks.length; i++) {
    const isLast = i === chunks.length - 1;
    await sendMessage(chatId, chunks[i], {
      replyMarkup: isLast ? options.replyMarkup : undefined,
      replyToMessageId: isLast ? options.replyToMessageId : undefined
    });
  }
}
async function sendQuickActionsMenu(chatId, role = 'guest') {
  await sendMessage(chatId, '⚡ Quick Actions', { replyMarkup: replyKeyboard });
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
function buildStockMessage(row) {
  const stock = getStockRowObject(row);
  const status = stock.balance <= stock.minAlert ? '🚨LOW STOCK' : '✅OK';
  return (
    `💊 Item: ${stock.item}\n` +
    `📥 In: ${stock.inQty}\n` +
    `📤 Out: ${stock.outQty}\n` +
    `📦 Balance: ${stock.balance} ${stock.unit}\n` +
    `⚠️ MinAlert: ${stock.minAlert}\n` +
    `🕒 UpdatedAt: ${stock.updatedAt || '-'}\n` +
    `📌 Status: ${status}`
  );
}

function getDisplayUnit(unit) {
  return clean(unit || '') || 'unit';
}
function buildDepartmentStockRows(data = []) {
  return data.map(r => {
    const stock = getStockRowObject(r);
    const shortage = stock.minAlert - stock.balance;
    return {
      ...stock,
      unit: getDisplayUnit(stock.unit),
      isLow: stock.balance <= stock.minAlert,
      shortage: shortage > 0 ? shortage : 0,
      gap: stock.balance - stock.minAlert
    };
  });
}
function sortDepartmentStockRows(rows = []) {
  return [...rows].sort((a, b) => {
    if (a.isLow !== b.isLow) return a.isLow ? -1 : 1;
    if (a.isLow && b.isLow) {
      if (a.gap !== b.gap) return a.gap - b.gap;
      if (a.shortage !== b.shortage) return b.shortage - a.shortage;
    }
    return a.item.localeCompare(b.item, undefined, { sensitivity: 'base' });
  });
}
function buildAllStockMessage(rows, department, mode = '') {
  const normalizedMode = norm(mode);
  const sortedRows = sortDepartmentStockRows(rows);
  const lowItems = sortedRows.filter(x => x.isLow);
  const okItems = sortedRows.filter(x => !x.isLow);

  let visibleRows = sortedRows;
  if (normalizedMode === 'low') visibleRows = lowItems;
  if (normalizedMode === 'ok') visibleRows = okItems;

  let msgOut =
    `📋 All Stock - ${department}\n\n` +
    `📦 Total Items: ${sortedRows.length}\n` +
    `🚨 Low Stock: ${lowItems.length}\n` +
    `✅ Normal: ${okItems.length}`;

  if (visibleRows.length === 0) {
    if (normalizedMode === 'low') return msgOut + '\n\n✅ No low stock items';
    if (normalizedMode === 'ok') return msgOut + '\n\n📭 No normal stock items';
    return msgOut + '\n\n📭 No stock data';
  }

  msgOut += '\n\n';

  if (normalizedMode === 'detail') {
    visibleRows.forEach((x, i) => {
      msgOut += `${i + 1}. ${x.item}\n`;
      msgOut += `   📦 Balance: ${x.balance} ${x.unit}\n`;
      msgOut += `   ⚠️ MinAlert: ${x.minAlert}\n`;
      msgOut += `   📌 Status: ${x.isLow ? '🚨 LOW STOCK' : '✅ OK'}\n`;
      msgOut += `   🕒 UpdatedAt: ${x.updatedAt || '-'}\n\n`;
    });
    return msgOut.trim();
  }

  if (normalizedMode === 'low' || normalizedMode === '') {
    if (lowItems.length > 0) {
      msgOut += '🚨 LOW STOCK\n';
      lowItems.forEach((x, i) => {
        msgOut += `${i + 1}. ${x.item} — ${x.balance} ${x.unit} (Min ${x.minAlert})\n`;
      });
      if (normalizedMode === '') msgOut += '\n';
    }
  }

  if (normalizedMode === 'ok' || normalizedMode === '') {
    const baseIndex = normalizedMode === 'ok' ? 0 : lowItems.length;
    const targetRows = okItems;
    if (targetRows.length > 0) {
      msgOut += '✅ NORMAL STOCK\n';
      targetRows.forEach((x, i) => {
        msgOut += `${baseIndex + i + 1}. ${x.item} — ${x.balance} ${x.unit}\n`;
      });
    }
  }

  return msgOut.trim();
}
function buildRestockListMessage(rows, department) {
  const lowItems = sortDepartmentStockRows(rows).filter(x => x.isLow);
  if (lowItems.length === 0) return `🛒 Restock List - ${department}\n\n✅ No items need restock right now`;
  let msgOut =
    `🛒 Restock List - ${department}\n\n` +
    `🚨 Items to Restock: ${lowItems.length}\n\n`;
  lowItems.forEach((x, i) => {
    msgOut += `${i + 1}. ${x.item} — need ${x.shortage} ${x.unit}\n`;
    msgOut += `   Current: ${x.balance} ${x.unit} | Min: ${x.minAlert}\n`;
  });
  return msgOut.trim();
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
  if (action === 'deleteitem') return role === 'super_admin' || role === 'admin';
  if (action === 'adjust') return role === 'super_admin';
  return false;
}
function buildProcessedMessageKey(messageId, chatId) {
  return `${chatId}:${messageId}`;
}
function pruneProcessedMessageCache() {
  const now = Date.now();
  for (const [key, expiresAt] of processedMessageCache) {
    if (expiresAt <= now) processedMessageCache.delete(key);
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
  if (processedMessageInflight.has(key) || hasProcessedMessageKey(key)) return null;
  processedMessageInflight.add(key);
  return key;
}
function releaseProcessedMessage(key) {
  if (key) processedMessageInflight.delete(key);
}
function rememberProcessedMessage(key) {
  if (!key) return;
  processedMessageCache.set(key, Date.now() + PROCESSED_MESSAGE_TTL_MS);
  pruneProcessedMessageCache();
}
async function acquireWriteCommandGuard(messageId, chatId) {
  const key = beginProcessedMessage(messageId, chatId);
  if (!key) return { ok: false, reason: 'duplicate_memory' };
  try {
    const alreadyProcessed = await isMessageProcessed(messageId, chatId);
    if (alreadyProcessed) {
      releaseProcessedMessage(key);
      return { ok: false, reason: 'duplicate_sheet' };
    }
    return { ok: true, key };
  } catch (err) {
    releaseProcessedMessage(key);
    throw err;
  }
}
function isRateLimited(actorCtx, role, command) {
  if (!WRITE_COMMANDS.has(command)) return false;
  if (role === 'super_admin') return false;
  const key = `${actorCtx.chatId}:${actorCtx.userId}`;
  const now = Date.now();
  const entries = rateLimitCache.get(key) || [];
  const fresh = entries.filter(ts => now - ts <= RATE_LIMIT_WINDOW_MS);
  fresh.push(now);
  rateLimitCache.set(key, fresh);
  return fresh.length > RATE_LIMIT_MAX_WRITES;
}
function pruneRateLimitCache() {
  const now = Date.now();
  for (const [key, entries] of rateLimitCache) {
    const fresh = entries.filter(ts => now - ts <= RATE_LIMIT_WINDOW_MS);
    if (fresh.length === 0) rateLimitCache.delete(key);
    else rateLimitCache.set(key, fresh);
  }
}

/**************** INPUT SESSIONS ****************/
function getUserSessionKey(msg) {
  return `${msg?.chat?.id || ''}:${msg?.from?.id || ''}`;
}
function getInputSession(msg) {
  const key = getUserSessionKey(msg);
  const session = inputSessionCache.get(key);
  if (!session) return null;
  if (Date.now() - Number(session.updatedAt || 0) > INPUT_SESSION_TIMEOUT_MS) {
    inputSessionCache.delete(key);
    return null;
  }
  return session;
}
function setInputSession(msg, session) {
  inputSessionCache.set(getUserSessionKey(msg), { ...session, updatedAt: Date.now() });
}
function clearInputSession(msg) {
  inputSessionCache.delete(getUserSessionKey(msg));
}
function buildSessionPrompt(action, step) {
  const cancelHint = '\n\nវាយ /cancelinput ដើម្បីបោះបង់';
  if (action === 'ADDITEM') {
    if (step === 'itemName') return `➕ បន្ថែម Item\n\nសូមបញ្ចូលឈ្មោះ Item${cancelHint}`;
    if (step === 'minAlert') return `➕ បន្ថែម Item\n\nសូមបញ្ចូល MinAlert${cancelHint}`;
    if (step === 'unit') return `➕ បន្ថែម Item\n\nសូមបញ្ចូល Unit${cancelHint}`;
  }
  if (action === 'IN') {
    if (step === 'itemName') return `📥 បញ្ចូលស្តុក\n\nសូមបញ្ចូលឈ្មោះ Item${cancelHint}`;
    if (step === 'qty') return `📥 បញ្ចូលស្តុក\n\nសូមបញ្ចូល Qty${cancelHint}`;
    if (step === 'note') return `📥 បញ្ចូលស្តុក\n\nសូមបញ្ចូល Note ឬវាយ -${cancelHint}`;
  }
  if (action === 'OUT') {
    if (step === 'itemName') return `📤 ដកស្តុក\n\nសូមបញ្ចូលឈ្មោះ Item${cancelHint}`;
    if (step === 'qty') return `📤 ដកស្តុក\n\nសូមបញ្ចូល Qty${cancelHint}`;
    if (step === 'note') return `📤 ដកស្តុក\n\nសូមបញ្ចូល Note ឬវាយ -${cancelHint}`;
  }
  if (action === 'STOCK') {
    if (step === 'itemName') return `📊 មើលស្តុក\n\nសូមបញ្ចូលឈ្មោះ Item${cancelHint}`;
  }
  return `សូមបញ្ចូលទិន្នន័យ${cancelHint}`;
}
function getSessionValidationError(action, step, value) {
  const v = clean(value);
  if (step === 'itemName') {
    if (!isValidItemName(v)) return '⚠️ ឈ្មោះ Item មិនត្រឹមត្រូវ\n- មិនអាចទទេ\n- មិនអាចមាន |\n- មិនអាចមាន newline';
    return null;
  }
  if (action === 'ADDITEM' && step === 'minAlert') {
    const n = Number(v);
    if (Number.isNaN(n) || n < 0) return '⚠️ MinAlert ត្រូវជាលេខ ហើយត្រូវ >= 0';
    return null;
  }
  if ((action === 'IN' || action === 'OUT') && step === 'qty') {
    const n = Number(v);
    if (!assertValidQty(n)) return '⚠️ Qty ត្រូវជាលេខ ហើយត្រូវ > 0';
    return null;
  }
  if (action === 'ADDITEM' && step === 'unit') {
    if (!isValidUnit(v)) return '⚠️ Unit មិនត្រឹមត្រូវ';
    return null;
  }
  return null;
}
function getSessionNextStep(action, currentStep) {
  if (action === 'ADDITEM') {
    if (currentStep === 'itemName') return 'minAlert';
    if (currentStep === 'minAlert') return 'unit';
    return null;
  }
  if (action === 'IN' || action === 'OUT') {
    if (currentStep === 'itemName') return 'qty';
    if (currentStep === 'qty') return 'note';
    return null;
  }
  if (action === 'STOCK') {
    if (currentStep === 'itemName') return null;
  }
  return null;
}
function buildCommandFromSession(session) {
  const data = session.data || {};
  if (session.action === 'ADDITEM') return `/additem | ${data.itemName} | ${data.minAlert} | ${data.unit}`;
  if (session.action === 'IN') return `/in | ${data.itemName} | ${data.qty}${data.note ? ` | ${data.note}` : ''}`;
  if (session.action === 'OUT') return `/out | ${data.itemName} | ${data.qty}${data.note ? ` | ${data.note}` : ''}`;
  if (session.action === 'STOCK') return `/stock | ${data.itemName}`;
  return null;
}
async function processInputSessionMessage(msg) {
  const session = getInputSession(msg);
  if (!session) return false;
  const text = clean(msg.text || '');
  if (!text) {
    await sendMessage(msg.chat.id, buildSessionPrompt(session.action, session.step));
    return true;
  }
  const validationError = getSessionValidationError(session.action, session.step, text);
  if (validationError) {
    await sendMessage(msg.chat.id, `${validationError}\n\n${buildSessionPrompt(session.action, session.step)}`);
    return true;
  }
  const nextSession = { ...session, data: { ...(session.data || {}) } };
  if (session.action === 'ADDITEM') {
    if (session.step === 'itemName') nextSession.data.itemName = text;
    if (session.step === 'minAlert') nextSession.data.minAlert = Number(text);
    if (session.step === 'unit') nextSession.data.unit = text;
  }
  if (session.action === 'IN' || session.action === 'OUT') {
    if (session.step === 'itemName') nextSession.data.itemName = text;
    if (session.step === 'qty') nextSession.data.qty = Number(text);
    if (session.step === 'note') nextSession.data.note = text === '-' ? '' : text;
  }
  if (session.action === 'STOCK') {
    if (session.step === 'itemName') nextSession.data.itemName = text;
  }
  const nextStep = getSessionNextStep(session.action, session.step);
  if (nextStep) {
    nextSession.step = nextStep;
    setInputSession(msg, nextSession);
    await sendMessage(msg.chat.id, buildSessionPrompt(nextSession.action, nextStep));
    return true;
  }
  clearInputSession(msg);
  const builtCommand = buildCommandFromSession(nextSession);
  if (!builtCommand) {
    await sendMessage(msg.chat.id, '❌ Failed to build action');
    return true;
  }
  await handleCommand({ ...msg, text: builtCommand });
  return true;
}

/**************** SHEET CONFIG ****************/
const STOCK_HEADERS = ['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt'];
const LOG_HEADERS = ['Timestamp', 'Type', 'Item', 'Qty', 'BalanceBefore', 'BalanceAfter', 'Unit', 'ChatId', 'ChatTitle', 'Username', 'Role', 'Note'];

const SHEET_HEADERS = {
  Stock_Medicine: STOCK_HEADERS,
  Stock_Supplies: STOCK_HEADERS,
  Stock_Inventory: STOCK_HEADERS,
  Logs_Medicine: LOG_HEADERS,
  Logs_Supplies: LOG_HEADERS,
  Logs_Inventory: LOG_HEADERS,
  Reports: ['Timestamp', 'Type', 'Details'],
  Roles: ['Username', 'Role', 'UpdatedAt'],
  AllowedChats: ['ChatId', 'ChatTitle', 'ChatType', 'Department', 'AddedAt'],
  PendingActions: ['Code', 'Username', 'UserId', 'ChatId', 'Action', 'PayloadJson', 'Status', 'CreatedAt', 'ExpiresAt'],
  ProcessedMessages: ['MessageId', 'ChatId', 'Command', 'ProcessedAt'],
  GroupSettings: ['ChatId', 'ChatTitle', 'DailyReportEnabled', 'LowStockAlertsEnabled', 'LastDailyReportDate', 'UpdatedAt']
};
const SHEET_RANGES = {
  Stock_Medicine: "'Stock_Medicine'!A2:G",
  Stock_Supplies: "'Stock_Supplies'!A2:G",
  Stock_Inventory: "'Stock_Inventory'!A2:G",
  Logs_Medicine: "'Logs_Medicine'!A2:L",
  Logs_Supplies: "'Logs_Supplies'!A2:L",
  Logs_Inventory: "'Logs_Inventory'!A2:L",
  Roles: "'Roles'!A2:C",
  AllowedChats: "'AllowedChats'!A2:E",
  PendingActions: "'PendingActions'!A2:I",
  ProcessedMessages: "'ProcessedMessages'!A2:D",
  GroupSettings: "'GroupSettings'!A2:F"
};
const SHEET_CACHE_KEYS = {
  Roles: 'sheet:roles',
  AllowedChats: 'sheet:allowed_chats',
  GroupSettings: 'sheet:group_settings'
};

const DEPARTMENTS = {
  medicine: { stockSheet: 'Stock_Medicine', logsSheet: 'Logs_Medicine', label: 'Medicine' },
  supplies: { stockSheet: 'Stock_Supplies', logsSheet: 'Logs_Supplies', label: 'Supplies' },
  inventory: { stockSheet: 'Stock_Inventory', logsSheet: 'Logs_Inventory', label: 'Inventory' }
};
function getDepartmentSheets(department) {
  const key = norm(department);
  const dept = DEPARTMENTS[key];
  if (!dept) throw new Error(`Unknown department: ${department}`);
  return {
    department: dept.label,
    departmentKey: key,
    stockSheet: dept.stockSheet,
    logsSheet: dept.logsSheet
  };
}
async function getDepartmentContextFromMessage(msg) {
  const chatId = String(msg?.chat?.id || '');
  const allowedChats = await getAllowedChats();
  const found = allowedChats.find(r => String(r[0] || '') === chatId);
  if (!found) throw new Error('This chat is not mapped to any department');
  const department = clean(found[3] || '');
  return getDepartmentSheets(department);
}
function isCacheEntryFresh(entry) {
  return Boolean(entry) && entry.expiresAt > Date.now();
}
function primeRowCache(cacheKey, value, ttlMs = SHEET_CACHE_TTL_MS) {
  rowCache.set(cacheKey, { value, expiresAt: Date.now() + ttlMs });
  return value;
}
function invalidateRowCache(cacheKey) {
  if (cacheKey) rowCache.delete(cacheKey);
}
function invalidateSheetCache(sheetTitle) {
  const cacheKey = SHEET_CACHE_KEYS[sheetTitle];
  if (cacheKey) invalidateRowCache(cacheKey);
}
async function getCachedSheetRows(cacheKey, range, ttlMs = SHEET_CACHE_TTL_MS) {
  const cached = rowCache.get(cacheKey);
  if (ttlMs > 0 && isCacheEntryFresh(cached)) return cached.value;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  const values = res.data.values || [];
  if (ttlMs > 0) primeRowCache(cacheKey, values, ttlMs);
  else invalidateRowCache(cacheKey);
  return values;
}
function cacheSheetIds(meta) {
  sheetIdCache.clear();
  for (const sheet of meta?.sheets || []) {
    const title = sheet?.properties?.title;
    const sheetId = sheet?.properties?.sheetId;
    if (title && typeof sheetId === 'number') sheetIdCache.set(title, sheetId);
  }
}
async function getSpreadsheetMeta(options = {}) {
  const { force = false } = options;
  if (!force && isCacheEntryFresh(spreadsheetMetaCache)) return spreadsheetMetaCache.data;
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  spreadsheetMetaCache = { data: meta.data, expiresAt: Date.now() + SHEET_CACHE_TTL_MS };
  cacheSheetIds(meta.data);
  return meta.data;
}
async function getSheetId(sheetTitle) {
  if (sheetIdCache.has(sheetTitle)) return sheetIdCache.get(sheetTitle);
  await getSpreadsheetMeta();
  if (sheetIdCache.has(sheetTitle)) return sheetIdCache.get(sheetTitle);
  throw new Error(`${sheetTitle} sheet not found`);
}
async function ensureSheetsExist(titles) {
  const meta = await getSpreadsheetMeta({ force: true });
  const existingTitles = new Set((meta.sheets || []).map(sheet => sheet.properties?.title).filter(Boolean));
  const missingTitles = titles.filter(title => !existingTitles.has(title));
  if (missingTitles.length === 0) return meta;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: missingTitles.map(title => ({ addSheet: { properties: { title } } }))
    }
  });
  for (const title of missingTitles) console.log(`✅ Created sheet: ${title}`);
  return getSpreadsheetMeta({ force: true });
}
async function ensureHeader(title, headers) {
  const endCol = toA1Column(headers.length);
  const range = `'${title}'!A1:${endCol}1`;
  const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  const current = headerRes.data.values?.[0] || [];
  const expected = headers;
  const isSame = current.length === expected.length && current.every((v, i) => String(v || '') === String(expected[i] || ''));
  if (!isSame) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
    console.log(`✅ Header synced: ${title}`);
  } else {
    console.log(`ℹ️ Header already correct: ${title}`);
  }
}
async function setupSheet() {
  await ensureSheetsExist(Object.keys(SHEET_HEADERS));
  await Promise.all(Object.entries(SHEET_HEADERS).map(([title, headers]) => ensureHeader(title, headers)));
}

/**************** DATA ACCESS ****************/
async function getData(stockSheet) {
  if (!stockSheet) throw new Error('stockSheet is required');
  const range = SHEET_RANGES[stockSheet] || `'${stockSheet}'!A2:G`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  return res.data.values || [];
}
async function getLogs(logsSheet) {
  if (!logsSheet) throw new Error('logsSheet is required');
  const range = SHEET_RANGES[logsSheet] || `'${logsSheet}'!A2:L`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  return res.data.values || [];
}
async function getRoles() {
  return getCachedSheetRows(SHEET_CACHE_KEYS.Roles, SHEET_RANGES.Roles);
}
async function getAllowedChats() {
  return getCachedSheetRows(SHEET_CACHE_KEYS.AllowedChats, SHEET_RANGES.AllowedChats);
}
async function getGroupSettingsRows() {
  return getCachedSheetRows(SHEET_CACHE_KEYS.GroupSettings, SHEET_RANGES.GroupSettings);
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
async function getAllDepartmentData() {
  const all = await Promise.all(Object.values(DEPARTMENTS).map(d => getData(d.stockSheet)));
  return all.flat();
}
async function getAllDepartmentLogs() {
  const all = await Promise.all(Object.values(DEPARTMENTS).map(d => getLogs(d.logsSheet)));
  return all.flat();
}
async function writeWithRetry(operation, retries = 4) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (err) {
      lastErr = err;
      const code = err?.response?.data?.error?.code || err?.code;
      if (code !== 429 || attempt === retries) throw err;
      const waitMs = Math.min(8000, 1000 * Math.pow(2, attempt));
      console.warn(`Google Sheets quota hit (429). Retrying in ${waitMs}ms...`);
      await sleep(waitMs);
    }
  }
  throw lastErr;
}
async function appendRowsToRange(range, values) {
  if (!Array.isArray(values) || values.length === 0) return;
  await writeWithRetry(() => sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range,
    valueInputOption: 'RAW',
    requestBody: { values }
  }));
}
async function batchUpdateValues(data) {
  if (!Array.isArray(data) || data.length === 0) return;
  await writeWithRetry(() => sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      valueInputOption: 'RAW',
      data
    }
  }));
}
async function appendRow(sheetTitle, values) {
  await appendRowsToRange(`'${sheetTitle}'!A:G`, [values]);
}
async function updateRow(sheetTitle, rowIndex, values) {
  await writeWithRetry(() => sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `'${sheetTitle}'!A${rowIndex + 2}:G${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  }));
}
async function appendLog(sheetTitle, values) {
  try {
    await appendRowsToRange(`'${sheetTitle}'!A:L`, [values]);
  } catch (err) {
    logError('Append log error:', err);
  }
}
async function appendLogs(sheetTitle, rows) {
  try {
    await appendRowsToRange(`'${sheetTitle}'!A:L`, rows);
  } catch (err) {
    logError('Append logs error:', err);
  }
}
async function appendReport(type, details) {
  try {
    await appendRowsToRange("'Reports'!A:C", [[nowIso(), type, details]]);
  } catch (err) {
    logError('Append report error:', err);
  }
}
async function appendReports(rows) {
  try {
    await appendRowsToRange("'Reports'!A:C", rows.map(r => [nowIso(), r.type, r.details]));
  } catch (err) {
    logError('Append reports error:', err);
  }
}
async function appendProcessedMessage(messageId, chatId, command) {
  await appendRowsToRange("'ProcessedMessages'!A:D", [[String(messageId), String(chatId), clean(command), nowIso()]]);
}
async function isMessageProcessed(messageId, chatId) {
  if (!messageId) return false;
  const rows = await getProcessedMessages();
  return rows.some(r => String(r[0] || '') === String(messageId) && String(r[1] || '') === String(chatId));
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
async function deleteRowsFromSheet(sheetTitle, rowIndexesZeroBasedWithoutHeader) {
  if (!rowIndexesZeroBasedWithoutHeader || rowIndexesZeroBasedWithoutHeader.length === 0) return;
  const sheetId = await getSheetId(sheetTitle);
  const sorted = [...new Set(rowIndexesZeroBasedWithoutHeader)].sort((a, b) => b - a);
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: sorted.map(idx => ({
        deleteDimension: {
          range: {
            sheetId,
            dimension: 'ROWS',
            startIndex: idx + 1,
            endIndex: idx + 2
          }
        }
      }))
    }
  });
  invalidateSheetCache(sheetTitle);
}
async function getStockRowByItemName(itemName, stockSheet) {
  const safeName = clean(itemName);
  if (!safeName) return null;
  const data = await getData(stockSheet);
  const rowIndex = findRowIndex(data, safeName);
  if (rowIndex === -1) return null;
  return { rowIndex, row: data[rowIndex] };
}

/**************** GROUP SETTINGS ****************/
async function getGroupSetting(chatId) {
  const rows = await getGroupSettingsRows();
  const found = rows.find(r => String(r[0] || '') === String(chatId));
  if (!found) {
    return {
      chatId: String(chatId),
      chatTitle: '',
      dailyReportEnabled: true,
      lowStockAlertsEnabled: true,
      lastDailyReportDate: '',
      updatedAt: ''
    };
  }
  return {
    chatId: String(found[0] || ''),
    chatTitle: clean(found[1] || ''),
    dailyReportEnabled: norm(found[2]) !== 'false',
    lowStockAlertsEnabled: norm(found[3]) !== 'false',
    lastDailyReportDate: clean(found[4] || ''),
    updatedAt: clean(found[5] || '')
  };
}
async function upsertGroupSetting(chatId, chatTitle, partial) {
  return withGroupSettingsLock(async () => {
    const rows = await getGroupSettingsRows();
    let idx = -1;
    for (let i = 0; i < rows.length; i++) {
      if (String(rows[i][0] || '') === String(chatId)) {
        idx = i;
        break;
      }
    }
    const current = idx === -1
      ? { dailyReportEnabled: true, lowStockAlertsEnabled: true, lastDailyReportDate: '', chatTitle: clean(chatTitle) }
      : {
          dailyReportEnabled: norm(rows[idx][2]) !== 'false',
          lowStockAlertsEnabled: norm(rows[idx][3]) !== 'false',
          lastDailyReportDate: clean(rows[idx][4] || ''),
          chatTitle: clean(rows[idx][1] || chatTitle)
        };
    const values = [
      String(chatId),
      clean(chatTitle || current.chatTitle),
      String(partial.dailyReportEnabled ?? current.dailyReportEnabled),
      String(partial.lowStockAlertsEnabled ?? current.lowStockAlertsEnabled),
      clean(partial.lastDailyReportDate ?? current.lastDailyReportDate),
      nowIso()
    ];
    if (idx === -1) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: "'GroupSettings'!A:F",
        valueInputOption: 'RAW',
        requestBody: { values: [values] }
      });
    } else {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `'GroupSettings'!A${idx + 2}:F${idx + 2}`,
        valueInputOption: 'RAW',
        requestBody: { values: [values] }
      });
    }
    invalidateSheetCache('GroupSettings');
  });
}
async function markDailyReportSent(chatId, chatTitle, dateString) {
  await upsertGroupSetting(chatId, chatTitle, { lastDailyReportDate: clean(dateString) });
}

/**************** ROLES ****************/
function getUserRoleFromRows(username, roleRows) {
  const u = cleanUsername(username);
  if (!u) return 'guest';
  if (BOOTSTRAP_SUPER_ADMINS.includes(u)) return 'super_admin';
  for (const r of roleRows) {
    if (cleanUsername(r[0]) === u) {
      const role = norm(r[1]);
      if (['super_admin', 'admin', 'member'].includes(role)) return role;
    }
  }
  return 'guest';
}
async function upsertRole(username, role) {
  return withRolesLock(async () => {
    const u = cleanUsername(username);
    if (!u) throw new Error('Username required');
    if (!['super_admin', 'admin', 'member'].includes(role)) throw new Error('Invalid role');
    const roleRows = await getRoles();
    let idx = -1;
    for (let i = 0; i < roleRows.length; i++) {
      if (cleanUsername(roleRows[i][0]) === u) { idx = i; break; }
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
  });
}
async function removeRole(username, expectedRole = null) {
  return withRolesLock(async () => {
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
  });
}

/**************** ALLOWED CHATS ****************/
function isAllowedChatId(chatId, allowedChatRows) {
  return allowedChatRows.some(r => String(r[0] || '') === String(chatId));
}
async function addAllowedChat(chatId, chatTitle, chatType, department) {
  return withAllowedChatsLock(async () => {
    const rows = await getAllowedChats();
    if (isAllowedChatId(chatId, rows)) return false;
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: "'AllowedChats'!A:E",
      valueInputOption: 'RAW',
      requestBody: { values: [[String(chatId), clean(chatTitle), clean(chatType), clean(department), nowIso()]] }
    });
    invalidateSheetCache('AllowedChats');
    return true;
  });
}
async function removeAllowedChat(chatId) {
  return withAllowedChatsLock(async () => {
    const rows = await getAllowedChats();
    for (let i = 0; i < rows.length; i++) {
      if (String(rows[i][0] || '') === String(chatId)) {
        await deleteRowFromSheet('AllowedChats', i);
        return true;
      }
    }
    return false;
  });
}

/**************** PENDING ACTIONS ****************/
async function createPendingAction(username, userId, chatId, action, payload) {
  return withPendingActionsLock(async () => {
    const code = makeConfirmCode();
    const createdAt = nowIso();
    const expiresAt = minutesFromNowIso(CONFIRM_EXPIRE_MINUTES);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: "'PendingActions'!A:I",
      valueInputOption: 'RAW',
      requestBody: {
        values: [[
          code,
          cleanUsername(username),
          String(userId || ''),
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
  });
}
async function findPendingActionByCode(code) {
  const rows = await getPendingActions();
  for (let i = 0; i < rows.length; i++) {
    if (clean(rows[i][0]) === clean(code)) {
      return {
        rowIndex: i,
        code: clean(rows[i][0]),
        username: cleanUsername(rows[i][1]),
        userId: String(rows[i][2] || ''),
        chatId: String(rows[i][3] || ''),
        action: clean(rows[i][4]),
        payloadJson: String(rows[i][5] || ''),
        status: clean(rows[i][6]),
        createdAt: clean(rows[i][7]),
        expiresAt: clean(rows[i][8])
      };
    }
  }
  return null;
}
async function updatePendingActionStatusByCode(code, status) {
  return withPendingActionsLock(async () => {
    const pending = await findPendingActionByCode(code);
    if (!pending) throw new Error('Pending action row not found');
    const values = [
      pending.code,
      pending.username,
      pending.userId,
      pending.chatId,
      pending.action,
      pending.payloadJson,
      status,
      pending.createdAt,
      pending.expiresAt
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `'PendingActions'!A${pending.rowIndex + 2}:I${pending.rowIndex + 2}`,
      valueInputOption: 'RAW',
      requestBody: { values: [values] }
    });
  });
}
function validatePendingActionOwnership(pending, username, userId, chatId) {
  if (!pending) return 'Confirmation code not found';
  if (pending.status !== 'pending') return `This request is already ${pending.status}`;
  if (pending.userId) {
    if (String(pending.userId) !== String(userId || '')) return 'This confirmation code is not yours';
  } else if (pending.username !== cleanUsername(username)) {
    return 'This confirmation code is not yours';
  }
  if (pending.chatId !== String(chatId)) return 'This confirmation code belongs to another chat';
  if (isExpired(pending.expiresAt)) return 'expired';
  return null;
}
async function cleanupExpiredPendingActions() {
  const rows = await getPendingActions();
  for (let i = 0; i < rows.length; i++) {
    const status = clean(rows[i][6]);
    const expiresAt = clean(rows[i][8]);
    const code = clean(rows[i][0]);
    if (status === 'pending' && isExpired(expiresAt) && code) {
      await runNonCriticalTask('cleanupExpiredPendingActions error:', async () => {
        await updatePendingActionStatusByCode(code, 'expired');
      });
    }
  }
}
async function cleanupOldPendingActions(days = PENDING_ACTION_RETENTION_DAYS) {
  return withPendingActionsLock(async () => {
    const rows = await getPendingActions();
    const cutoff = new Date(daysAgoIso(days)).getTime();
    const deleteIndexes = [];
    for (let i = 0; i < rows.length; i++) {
      const status = clean(rows[i][6]);
      const createdAt = clean(rows[i][7]);
      const t = new Date(createdAt).getTime();
      if (Number.isNaN(t)) continue;
      if (['confirmed', 'cancelled', 'failed', 'expired'].includes(status) && t < cutoff) deleteIndexes.push(i);
    }
    if (deleteIndexes.length > 0) await deleteRowsFromSheet('PendingActions', deleteIndexes);
    return deleteIndexes.length;
  });
}
async function cleanupOldProcessedMessages(days = PROCESSED_MESSAGE_RETENTION_DAYS) {
  return withProcessedMessagesLock(async () => {
    const rows = await getProcessedMessages();
    const cutoff = new Date(daysAgoIso(days)).getTime();
    const deleteIndexes = [];
    for (let i = 0; i < rows.length; i++) {
      const processedAt = clean(rows[i][3]);
      const t = new Date(processedAt).getTime();
      if (!Number.isNaN(t) && t < cutoff) deleteIndexes.push(i);
    }
    if (deleteIndexes.length > 0) await deleteRowsFromSheet('ProcessedMessages', deleteIndexes);
    return deleteIndexes.length;
  });
}
async function autoCleanupSheets() {
  await cleanupExpiredPendingActions();
  const [processedDeleted, pendingDeleted] = await Promise.all([
    cleanupOldProcessedMessages(),
    cleanupOldPendingActions()
  ]);
  if (processedDeleted > 0 || pendingDeleted > 0) {
    await appendReport('AUTO_CLEANUP', `ProcessedMessagesDeleted=${processedDeleted}, PendingActionsDeleted=${pendingDeleted}`);
  }
}

/**************** PERMISSIONS ****************/
const ROLE_COMMANDS = {
  super_admin: new Set([
    '/start', '/help', '/menu', '/myrole', '/roles', '/groups',
    '/addsuperadmin', '/removesuperadmin',
    '/addadmin', '/removeadmin',
    '/addmember', '/removemember',
    '/allowgroup', '/disallowgroup',
    '/additem', '/in', '/out', '/stock', '/allstock', '/restocklist',
    '/alertstock', '/lowstock',
    '/setalert', '/setunit', '/renameitem', '/deleteitem',
    '/history', '/today', '/report', '/exportsummary', '/adjust',
    '/search', '/exportlogs', '/confirm', '/cancel',
    '/undo', '/dashboard', '/itemlogs', '/audit',
    '/health', '/stats', '/cancelinput',
    '/setdailyreport', '/setalerts',
    '/additemsbulk', '/inbulk', '/outbulk'
  ]),
  admin: new Set([
    '/start', '/help', '/menu', '/myrole',
    '/additem', '/deleteitem', '/exportsummary',
    '/in', '/out', '/stock', '/allstock', '/restocklist', '/alertstock', '/lowstock',
    '/search', '/exportlogs', '/confirm', '/cancel',
    '/undo', '/dashboard', '/itemlogs',
    '/health', '/stats', '/cancelinput',
    '/setdailyreport', '/setalerts',
    '/additemsbulk', '/inbulk', '/outbulk',
    '/today'
  ]),
  member: new Set([
    '/start', '/help', '/menu', '/myrole',
    '/in', '/out', '/inbulk', '/outbulk',
    '/stock', '/allstock', '/restocklist', '/alertstock', '/lowstock',
    '/search', '/confirm', '/cancel',
    '/dashboard', '/itemlogs', '/cancelinput',
    '/today'
  ])
};
function canUseCommand(role, command) {
  return ROLE_COMMANDS[role]?.has(command) || false;
}

/**************** ALERT ****************/
async function isLowStockAlertEnabled(chatId) {
  const setting = await getGroupSetting(chatId);
  return setting.lowStockAlertsEnabled;
}
async function checkAndSendLowStockAlert(chatId, row) {
  const enabled = await isLowStockAlertEnabled(chatId);
  if (!enabled) return;
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
function summarizeTodayLogs(logs, timeZone = APP_TIMEZONE) {
  const today = getLocalDateString(timeZone);
  let inCount = 0;
  let outCount = 0;
  let adjustCount = 0;
  let undoCount = 0;
  let inQty = 0;
  let outQty = 0;
  for (const r of logs) {
    const ts = clean(r[0] || '');
    if (!ts) continue;
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) continue;
    if (getLocalDateStringFromDate(d, timeZone) !== today) continue;
    const type = clean(r[1] || '');
    const qty = toNumber(r[3]);
    if (type === 'IN') { inCount += 1; inQty += qty; }
    else if (type === 'OUT') { outCount += 1; outQty += qty; }
    else if (type === 'ADJUST') adjustCount += 1;
    else if (type === 'UNDO') undoCount += 1;
  }
  return { today, inCount, outCount, adjustCount, undoCount, inQty, outQty };
}
function buildLowStockSummaryMessage(data) {
  const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4]));
  if (lowItems.length === 0) return '✅ Daily Low Stock Summary\n\nNo low stock items today.';
  let msg = '📣 Daily Low Stock Summary\n\n';
  for (const r of lowItems.slice(0, 50)) {
    msg += `💊 ${clean(r[0] || '')}: ${getBalanceFromRow(r)} ${clean(r[5] || '')} (Min: ${toNumber(r[4])})\n`;
  }
  if (lowItems.length > 50) msg += `\n...and ${lowItems.length - 50} more items`;
  return msg;
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
    return withStockSheetWriteLock(async () => withLock(`stock:${clean(payload.stockSheet || '') || itemName}:${itemName}`, async () => {
      const stockSheet = clean(payload.stockSheet);
      const data = await getData(stockSheet);
      const row = findRowIndex(data, itemName);
      if (row === -1) throw new Error(`Item not found: ${itemName}`);
      await deleteRowFromSheet(stockSheet, row);
      await runNonCriticalTask('Append report error:', () =>
        appendReport('DELETE_ITEM', `By=${actorCtx.actor}, DepartmentSheet=${clean(payload.stockSheet || '')}, Deleted item=${itemName}`)
      );
      return `🗑️ Deleted Item: ${itemName}`;
    }));
  }
  if (pending.action === 'adjust') {
    const itemName = clean(payload.itemName);
    const newBalance = Number(payload.newBalance);
    if (Number.isNaN(newBalance) || newBalance < 0) throw new Error('Invalid NewBalance');
    return withStockSheetWriteLock(async () => withLock(`stock:${clean(payload.stockSheet || '') || itemName}:${itemName}`, async () => {
      const stockSheet = clean(payload.stockSheet);
      const logsSheet = clean(payload.logsSheet);
      const data = await getData(stockSheet);
      const row = findRowIndex(data, itemName);
      if (row === -1) throw new Error(`Item not found: ${itemName}`);
      const r = data[row];
      const currentIn = toNumber(r[1]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const oldBalance = getBalanceFromRow(r);
      if (newBalance > currentIn) throw new Error(`NewBalance cannot be greater than total In (${currentIn})`);
      const newOut = currentIn - newBalance;
      const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
      await updateRow(stockSheet, row, updated);
      await Promise.all([
        runNonCriticalTask('Append log error:', () =>
          appendLog(logsSheet, [
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
    }));
  }
  throw new Error(`Unknown pending action: ${pending.action}`);
}

/**************** UNDO ****************/
async function getLastUndoableLogForItem(itemName, logsSheet) {
  const logs = await getLogs(logsSheet);
  return logs
    .map((r, i) => ({ row: r, idx: i }))
    .filter(x => norm(x.row[2]) === norm(itemName))
    .filter(x => ['IN', 'OUT'].includes(clean(x.row[1] || '')))
    .reverse()[0] || null;
}
async function undoLastAction(itemName, actorCtx, role, deptCtx) {
  return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
    const stockRef = await getStockRowByItemName(itemName, deptCtx.stockSheet);
    if (!stockRef) throw new Error(`Item not found: ${itemName}`);
    const lastLog = await getLastUndoableLogForItem(itemName, deptCtx.logsSheet);
    if (!lastLog) throw new Error(`No undoable IN/OUT history found for: ${itemName}`);
    const actionTs = new Date(clean(lastLog.row[0] || '')).getTime();
    if (Number.isNaN(actionTs)) throw new Error('Last log timestamp is invalid');
    if (Date.now() - actionTs > UNDO_WINDOW_MINUTES * 60 * 1000) {
      throw new Error(`Undo window expired. Allowed within ${UNDO_WINDOW_MINUTES} minutes`);
    }
    const r = stockRef.row;
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const currentBalance = getBalanceFromRow(r);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const logType = clean(lastLog.row[1] || '');
    const qty = toNumber(lastLog.row[3]);
    let newIn = currentIn;
    let newOut = currentOut;
    let note = '';
    if (logType === 'IN') {
      if (qty > currentIn) throw new Error('Cannot undo last IN because current total In is less than logged Qty');
      newIn = currentIn - qty;
      if (newIn < currentOut) throw new Error('Cannot undo last IN because resulting balance would be negative');
      note = `Undo last IN of ${qty}`;
    } else if (logType === 'OUT') {
      if (qty > currentOut) throw new Error('Cannot undo last OUT because current total Out is less than logged Qty');
      newOut = currentOut - qty;
      note = `Undo last OUT of ${qty}`;
    } else {
      throw new Error('Last action is not undoable');
    }
    const newBalance = newIn - newOut;
    const updated = [r[0], newIn, newOut, newBalance, minAlert, unit, nowIso()];
    await updateRow(deptCtx.stockSheet, stockRef.rowIndex, updated);
    await Promise.all([
      runNonCriticalTask('Append log error:', () =>
        appendLog(deptCtx.logsSheet, [
          nowIso(), 'UNDO', r[0], qty, currentBalance, newBalance, unit,
          actorCtx.chatId, actorCtx.chatTitle, actorCtx.actor, role, note
        ])
      ),
      runNonCriticalTask('Append report error:', () =>
        appendReport('UNDO', `By=${actorCtx.actor}, Department=${deptCtx.department}, Item=${r[0]}, ${note}, Before=${currentBalance}, After=${newBalance}`)
      )
    ]);
    await checkAndSendLowStockAlert(actorCtx.chatId, updated);
    return (
      `↩️ Undo Success\n\n` +
      `💊 Item: ${r[0]}\n` +
      `📝 Action: ${note}\n` +
      `📦 Old Balance: ${currentBalance} ${unit}\n` +
      `✅ New Balance: ${newBalance} ${unit}`
    );
  }));
}

/**************** BULK HELPERS ****************/
async function processBulkAddItems(entries, actorCtx, deptCtx) {
  return withStockSheetWriteLock(async () => {
    const results = [];
    const data = await getData(deptCtx.stockSheet);
    const existingNames = new Set(data.map(r => normalizeItemName(r[0])));
    const pendingNames = new Set();
    const rowsToAppend = [];
    const reportRows = [];

    for (const line of entries) {
      const parts = parsePipe(line);
      if (parts.length < 3) {
        results.push(`❌ Invalid line: ${line}`);
        continue;
      }
      const itemName = clean(parts[0]);
      const minAlert = Number(parts[1]);
      const unit = clean(parts[2]);
      const normalized = normalizeItemName(itemName);
      if (!isValidItemName(itemName)) { results.push(`❌ Invalid item name: ${itemName || line}`); continue; }
      if (Number.isNaN(minAlert) || minAlert < 0) { results.push(`❌ Invalid MinAlert for ${itemName}`); continue; }
      if (!isValidUnit(unit)) { results.push(`❌ Invalid Unit for ${itemName}`); continue; }
      if (existingNames.has(normalized) || pendingNames.has(normalized)) {
        results.push(`⚠️ Already exists: ${itemName}`);
        continue;
      }
      pendingNames.add(normalized);
      rowsToAppend.push([itemName, 0, 0, 0, minAlert, unit, nowIso()]);
      reportRows.push({
        type: 'ADD_ITEM_BULK',
        details: `By=${actorCtx.actor}, Department=${deptCtx.department}, Item=${itemName}, MinAlert=${minAlert}, Unit=${unit}`
      });
      results.push(`✅ Added: ${itemName}`);
    }

    if (rowsToAppend.length > 0) {
      await appendRowsToRange(`'${deptCtx.stockSheet}'!A:G`, rowsToAppend);
      await runNonCriticalTask('Append reports error:', () => appendReports(reportRows));
    }
    return results;
  });
}
async function processBulkMovement(entries, mode, actorCtx, role, deptCtx) {
  return withStockSheetWriteLock(async () => {
    const results = [];
    const data = await getData(deptCtx.stockSheet);
    const normalizedIndex = new Map();
    data.forEach((row, rowIndex) => normalizedIndex.set(normalizeItemName(row[0]), { row, rowIndex }));

    const workingRows = new Map();
    const updates = [];
    const logRows = [];
    const reportRows = [];
    const lowStockCandidates = [];

    for (const line of entries) {
      const parts = parsePipe(line);
      if (parts.length < 2) { results.push(`❌ Invalid line: ${line}`); continue; }
      const itemName = clean(parts[0]);
      const qty = Number(parts[1]);
      const note = parts.length >= 3 ? clean(parts.slice(2).join(' | ')) : '';
      if (!isValidItemName(itemName)) { results.push(`❌ Invalid item name: ${itemName || line}`); continue; }
      if (!assertValidQty(qty)) { results.push(`❌ Invalid Qty for ${itemName}`); continue; }

      const key = normalizeItemName(itemName);
      const ref = normalizedIndex.get(key);
      if (!ref) { results.push(`❌ Item not found: ${itemName}`); continue; }

      const currentRow = workingRows.get(key) || [...ref.row];
      const currentIn = toNumber(currentRow[1]);
      const currentOut = toNumber(currentRow[2]);
      const minAlert = toNumber(currentRow[4]);
      const unit = clean(currentRow[5] || '');
      const oldBalance = getBalanceFromRow(currentRow);
      let updated;
      let newBalance;

      if (mode === 'IN') {
        const newIn = currentIn + qty;
        newBalance = newIn - currentOut;
        updated = [currentRow[0], newIn, currentOut, newBalance, minAlert, unit, nowIso()];
      } else {
        if (qty > oldBalance) {
          results.push(`❌ Not enough stock: ${itemName} (Balance ${oldBalance} ${unit})`);
          continue;
        }
        const newOut = currentOut + qty;
        newBalance = currentIn - newOut;
        updated = [currentRow[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
      }

      workingRows.set(key, updated);
      updates.push({
        range: `'${deptCtx.stockSheet}'!A${ref.rowIndex + 2}:G${ref.rowIndex + 2}`,
        values: [updated]
      });
      logRows.push([
        nowIso(), mode, currentRow[0], qty, oldBalance, newBalance, unit,
        actorCtx.chatId, actorCtx.chatTitle, actorCtx.actor, role, note
      ]);
      reportRows.push({
        type: `${mode}_BULK`,
        details: `By=${actorCtx.actor}, Department=${deptCtx.department}, Item=${currentRow[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}, Note=${note}`
      });
      if (mode === 'OUT') lowStockCandidates.push(updated);
      results.push(`✅ ${mode}: ${currentRow[0]} -> ${newBalance} ${unit}`);
    }

    if (updates.length > 0) {
      await batchUpdateValues(updates);
      await Promise.all([
        runNonCriticalTask('Append logs error:', () => appendLogs(deptCtx.logsSheet, logRows)),
        runNonCriticalTask('Append reports error:', () => appendReports(reportRows))
      ]);
    }

    if (mode === 'OUT') {
      for (const row of lowStockCandidates) {
        await checkAndSendLowStockAlert(actorCtx.chatId, row);
      }
    }
    return results;
  });
}

/**************** DAILY SUMMARY ****************/
async function sendDailyLowStockSummariesIfDue() {
  const now = new Date();
  const parts = getTimeParts(APP_TIMEZONE, now);
  const currentMinuteOfDay = (parts.hour * 60) + parts.minute;
  const scheduledMinuteOfDay = (DAILY_REPORT_HOUR * 60) + DAILY_REPORT_MINUTE;
  if (currentMinuteOfDay < scheduledMinuteOfDay) return;
  const today = getLocalDateString(APP_TIMEZONE);
  const allowedChats = await getAllowedChats();
  for (const chat of allowedChats) {
    const chatId = String(chat[0] || '');
    const chatTitle = clean(chat[1] || '');
    const department = clean(chat[3] || '');
    if (!chatId || !department) continue;
    const deptCfg = getDepartmentSheets(department);
    const data = await getData(deptCfg.stockSheet);
    const lowStockMsg = buildLowStockSummaryMessage(data);
    await withLock(`daily-report:${chatId}:${today}`, async () => {
      const setting = await getGroupSetting(chatId);
      if (!setting.dailyReportEnabled) return;
      if (clean(setting.lastDailyReportDate) === today) return;
      await sendLongMessage(chatId, lowStockMsg);
      await markDailyReportSent(chatId, chatTitle, today);
    });
  }
}

/**************** HEALTH ****************/
function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return `${d}d ${h}h ${m}m ${sec}s`;
}
async function buildHealthMessage() {
  const [data, logs, pending, processed, roles, allowedChats, groupSettings] = await Promise.all([
    getAllDepartmentData(), getAllDepartmentLogs(), getPendingActions(), getProcessedMessages(),
    getRoles(), getAllowedChats(), getGroupSettingsRows()
  ]);
  const mem = process.memoryUsage();
  return (
    `🩺 Bot Health\n\n` +
    `✅ Uptime: ${formatUptime(process.uptime() * 1000)}\n` +
    `💾 RSS: ${Math.round(mem.rss / 1024 / 1024)} MB\n` +
    `🧠 Heap Used: ${Math.round(mem.heapUsed / 1024 / 1024)} MB\n\n` +
    `📦 Stock Rows: ${data.length}\n` +
    `📝 Log Rows: ${logs.length}\n` +
    `⏳ Pending Actions: ${pending.length}\n` +
    `🧾 Processed Messages: ${processed.length}\n` +
    `👥 Roles Rows: ${roles.length}\n` +
    `💬 Allowed Chats: ${allowedChats.length}\n` +
    `⚙️ Group Settings Rows: ${groupSettings.length}\n\n` +
    `🗂 In-memory processed cache: ${processedMessageCache.size}\n` +
    `🧠 Input sessions: ${inputSessionCache.size}\n` +
    `🚦 Rate limit buckets: ${rateLimitCache.size}\n` +
    `🔒 Active locks: ${itemLocks.size}`
  );
}

/**************** CONFIRM/CANCEL HELPERS ****************/
async function confirmPendingActionByCode(code, msg, role, actorCtx) {
  return withLock(`pending:${code}`, async () => {
    const pending = await findPendingActionByCode(code);
    const validation = validatePendingActionOwnership(pending, actorCtx.username, actorCtx.userId, actorCtx.chatId);
    if (validation === 'expired') {
      await updatePendingActionStatusByCode(code, 'expired');
      return 'Confirmation code expired';
    }
    if (validation) return validation;
    if (!canConfirmPending(role, pending.action)) return `You no longer have permission to confirm "${pending.action}"`;
    try {
      const resultMessage = await executeConfirmedAction(pending, msg, role, actorCtx);
      await updatePendingActionStatusByCode(code, 'confirmed');
      return `Confirmed\nCode: ${code}\n\n${resultMessage}`;
    } catch (err) {
      await runNonCriticalTask('PendingActions update error:', () => updatePendingActionStatusByCode(code, 'failed'));
      return `Failed to execute confirmation\nCode: ${code}\nReason: ${err.message}`;
    }
  });
}
async function cancelPendingActionByCode(code, actorCtx) {
  return withLock(`pending:${code}`, async () => {
    const pending = await findPendingActionByCode(code);
    const validation = validatePendingActionOwnership(pending, actorCtx.username, actorCtx.userId, actorCtx.chatId);
    if (validation === 'expired') {
      await updatePendingActionStatusByCode(code, 'expired');
      return 'Confirmation code expired';
    }
    if (validation) return validation;
    await updatePendingActionStatusByCode(code, 'cancelled');
    return `Cancelled\nCode: ${code}`;
  });
}

function stripHeader(rawText) {
  return String(rawText || '').split(/\r?\n/).slice(1).map(line => clean(line)).filter(Boolean);
}

/**************** COMMAND HANDLER ****************/
async function handleCommand(msg) {
  const rawText = String(msg.text || '');
  const lines = rawText.split(/\r?\n/).map(s => clean(s)).filter(Boolean);
  const headerLine = lines[0] || '';
  const parts = parsePipe(headerLine);
  const command = normalizeCommand(parts[0]);

  const chatId = msg.chat.id;
  const actorCtx = getActorContext(msg);
  const username = actorCtx.username;
  const actor = actorCtx.actor;

  if (!command) return sendMessage(chatId, '⚠️ Invalid command');

  const [roleRows, allowedChatRows] = await Promise.all([getRoles(), getAllowedChats()]);
  const role = getUserRoleFromRows(username, roleRows);
  const isWriteCommand = WRITE_COMMANDS.has(command);
  const replyKeyboard = getDailyReplyKeyboard(role);

  if (isRateLimited(actorCtx, role, command)) {
    return sendMessage(chatId, '⏳ Too many write commands. Please slow down a bit.');
  }

  let processedMessageKey = null;
  let processedMessageMarked = false;

  if (isWriteCommand) {
    const guard = await acquireWriteCommandGuard(msg.message_id, chatId);
    if (!guard.ok) return sendMessage(chatId, 'ℹ️ This command was already processed.');
    processedMessageKey = guard.key;
  }

  async function markProcessedIfNeeded() {
    if (!processedMessageKey || processedMessageMarked) return;
    await withProcessedMessagesLock(async () => {
      const alreadyProcessed = await isMessageProcessed(msg.message_id, chatId);
      if (!alreadyProcessed) {
        await appendProcessedMessage(msg.message_id, chatId, command);
      }
      processedMessageMarked = true;
      rememberProcessedMessage(processedMessageKey);
      releaseProcessedMessage(processedMessageKey);
    });
  }

  try {
    if (isGroupChat(msg) && !GROUP_BYPASS_COMMANDS.has(command)) {
      if (allowedChatRows.length === 0) {
        return sendMessage(chatId, '⛔ This group is not allowed yet.\nSuper Admin must run /allowgroup in this group first.');
      }
      if (!isAllowedChatId(chatId, allowedChatRows)) {
        return sendMessage(chatId, '⛔ This group is not in whitelist.\nPlease ask Super Admin to run /allowgroup here.');
      }
    }

    if (command !== '/start' && command !== '/help' && command !== '/menu' && !canUseCommand(role, command)) {
      const displayUser = msg?.from?.username ? `@${msg.from.username}` : actor;
      return sendMessage(chatId, `⛔ Sorry ${displayUser}\nYou are not allowed to use ${command}\n👤 Your role: ${role}`);
    }

    if (isWriteCommand) await markProcessedIfNeeded();

    if (command === '/cancelinput') {
      const existing = getInputSession(msg);
      clearInputSession(msg);
      return sendMessage(chatId, existing ? '✅ បានបោះបង់ input flow ហើយ' : 'ℹ️ មិនមាន input flow កំពុងដំណើរការ');
    }

    if (command === '/menu') {
      clearInputSession(msg);
      await sendQuickActionsMenu(chatId, role);
      return;
    }

    if (command === '/start' || command === '/help') {
      clearInputSession(msg);
      if (role === 'super_admin') {
        await sendLongMessage(chatId,
          '🤖 Stock Bot\n\n' +
          '👑 Super Admin:\n' +
          '/addsuperadmin | username\n/removesuperadmin | username\n/addadmin | username\n/removeadmin | username\n' +
          '/addmember | username\n/removemember | username\n/roles\n/myrole\n/allowgroup\n/disallowgroup\n/groups\n' +
          '/setdailyreport | on/off\n/setalerts | on/off\n/additem | Item | MinAlert | Unit\n' +
          '/additemsbulk\nItem | MinAlert | Unit\n...\n/deleteitem | Item\n/in | Item | Qty | Optional Note\n' +
          '/inbulk\nItem | Qty | Optional Note\n...\n/out | Item | Qty | Optional Note\n' +
          '/outbulk\nItem | Qty | Optional Note\n...\n/adjust | Item | NewBalance\n/undo | Item\n/search | keyword\n' +
          '/stock | Item\n/allstock\n/allstock | low\n/allstock | ok\n/allstock | detail\n/restocklist\n/alertstock\n/lowstock\n/history | Item\n/itemlogs | Item | limit\n' +
          '/today\n/report\n/dashboard\n/audit | username\n/health\n/stats\n/exportsummary\n/exportlogs\n' +
          '/confirm | CODE\n/cancel | CODE\n/cancelinput'
        );
      } else if (role === 'admin') {
        await sendLongMessage(chatId,
          '🤖 Stock Bot\n\n🛠 Admin:\n' +
          '/myrole\n/setdailyreport | on/off\n/setalerts | on/off\n/additem | Item | MinAlert | Unit\n' +
          '/additemsbulk\nItem | MinAlert | Unit\n...\n/deleteitem | Item\n/in | Item | Qty | Optional Note\n' +
          '/inbulk\nItem | Qty | Optional Note\n...\n/out | Item | Qty | Optional Note\n/outbulk\nItem | Qty | Optional Note\n...\n' +
          '/undo | Item\n/search | keyword\n/stock | Item\n/allstock\n/allstock | low\n/allstock | ok\n/allstock | detail\n/restocklist\n/alertstock\n/lowstock\n/dashboard\n' +
          '/itemlogs | Item | limit\n/health\n/stats\n/today\n/exportsummary\n/exportlogs\n/confirm | CODE\n/cancel | CODE\n/cancelinput'
        );
      } else if (role === 'member') {
        await sendLongMessage(chatId,
          '🤖 Stock Bot\n\n👥 Member:\n' +
          '/myrole\n/in | Item | Qty | Optional Note\n/out | Item | Qty | Optional Note\n' +
          '/inbulk\nItem | Qty | Optional Note\n...\n/outbulk\nItem | Qty | Optional Note\n...\n' +
          '/search | keyword\n/stock | Item\n/allstock\n/allstock | low\n/allstock | ok\n/restocklist\n/alertstock\n/lowstock\n/dashboard\n/today\n' +
          '/itemlogs | Item | limit\n/confirm | CODE\n/cancel | CODE\n/cancelinput'
        );
      } else {
        return sendMessage(chatId,
          '⛔ You are not registered to use this bot.\nPlease ask Super Admin to add your username.\n\n⚠️ Telegram username is required.',
          { replyMarkup: replyKeyboard }
        );
      }
      await sendMessage(chatId, '📌 Daily menu is ready', { replyMarkup: replyKeyboard });
      await sendQuickActionsMenu(chatId, role);
      return;
    }

    if (command === '/myrole') {
      return sendMessage(chatId, `👤 Username: ${username ? '@' + username : 'no username'}\n🔐 Role: ${role}`, { replyMarkup: replyKeyboard });
    }

    if (command === '/roles') {
      let msgOut = '👥 Roles\n\n';
      if (BOOTSTRAP_SUPER_ADMINS.length > 0) {
        msgOut += '👑 Bootstrap Super Admins:\n';
        for (const u of BOOTSTRAP_SUPER_ADMINS) msgOut += `- @${u}\n`;
        msgOut += '\n';
      }
      if (roleRows.length === 0) {
        msgOut += 'No sheet roles yet.';
        return sendMessage(chatId, msgOut);
      }
      const groups = { super_admin: [], admin: [], member: [] };
      for (const r of roleRows) {
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
      if (allowedChatRows.length === 0) return sendMessage(chatId, '📭 No allowed groups yet');
      let msgOut = '📋 Allowed Groups\n\n';
      for (const r of allowedChatRows) {
        const setting = await getGroupSetting(r[0]);
        msgOut +=
          `🆔 ${r[0]}\n` +
          `🏷 ${clean(r[1] || '-')}\n` +
          `🧩 ${clean(r[2] || '-')}\n` +
          `🏬 ${clean(r[3] || '-')}\n` +
          `📣 DailyReport: ${setting.dailyReportEnabled}\n` +
          `🚨 Alerts: ${setting.lowStockAlertsEnabled}\n` +
          `🗓 LastDailyReportDate: ${setting.lastDailyReportDate || '-'}\n\n`;
      }
      return sendLongMessage(chatId, msgOut.trim());
    }

    if (command === '/allowgroup') {
      if (!isGroupChat(msg)) return sendMessage(chatId, '⚠️ /allowgroup can only be used inside a group');
      const department = norm(parts[1] || '');
      if (!DEPARTMENTS[department]) return sendMessage(chatId, '⚠️ Usage:\n/allowgroup | medicine|supplies|inventory');
      const added = await addAllowedChat(chatId, actorCtx.chatTitle, actorCtx.chatType, department);
      await upsertGroupSetting(chatId, actorCtx.chatTitle, {
        dailyReportEnabled: true,
        lowStockAlertsEnabled: true,
        lastDailyReportDate: ''
      });
      await appendReport('ALLOW_GROUP', `ChatId=${chatId}, Title=${actorCtx.chatTitle}, Department=${department}, Added=${added}, By=${actor}`);
      return sendMessage(chatId, added ? `✅ Group allowed\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}` : `ℹ️ Group already allowed\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}`);
    }

    if (command === '/disallowgroup') {
      if (!isGroupChat(msg)) return sendMessage(chatId, '⚠️ /disallowgroup can only be used inside a group');
      const removed = await removeAllowedChat(chatId);
      await appendReport('DISALLOW_GROUP', `ChatId=${chatId}, Title=${actorCtx.chatTitle}, Removed=${removed}, By=${actor}`);
      return sendMessage(chatId, removed ? `🗑️ Group removed from whitelist\n🆔 ${chatId}\n🏷 ${actorCtx.chatTitle}` : 'ℹ️ This group was not in whitelist');
    }

    if (command === '/setdailyreport') {
      if (!isGroupChat(msg)) return sendMessage(chatId, '⚠️ This command can only be used in a group');
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/setdailyreport | on/off');
      const value = parseOnOff(parts[1]);
      if (value === null) return sendMessage(chatId, '⚠️ Value must be on or off');
      await upsertGroupSetting(chatId, actorCtx.chatTitle, { dailyReportEnabled: value });
      await appendReport('SET_DAILY_REPORT', `By=${actor}, ChatId=${chatId}, Enabled=${value}`);
      return sendMessage(chatId, `✅ Daily report at ${String(DAILY_REPORT_HOUR).padStart(2, '0')}:${String(DAILY_REPORT_MINUTE).padStart(2, '0')} (${APP_TIMEZONE}) is now ${value ? 'ON' : 'OFF'}`);
    }

    if (command === '/setalerts') {
      if (!isGroupChat(msg)) return sendMessage(chatId, '⚠️ This command can only be used in a group');
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/setalerts | on/off');
      const value = parseOnOff(parts[1]);
      if (value === null) return sendMessage(chatId, '⚠️ Value must be on or off');
      await upsertGroupSetting(chatId, actorCtx.chatTitle, { lowStockAlertsEnabled: value });
      await appendReport('SET_ALERTS', `By=${actor}, ChatId=${chatId}, Enabled=${value}`);
      return sendMessage(chatId, `✅ Instant low-stock alerts are now ${value ? 'ON' : 'OFF'}`);
    }

    if (command === '/addsuperadmin' || command === '/addadmin' || command === '/addmember') {
      if (parts.length < 2) return sendMessage(chatId, `⚠️ Usage:\n${command} | username`);
      const target = cleanUsername(parts[1]);
      const targetRole = command === '/addsuperadmin' ? 'super_admin' : command === '/addadmin' ? 'admin' : 'member';
      if (!target) return sendMessage(chatId, '⚠️ Username required');
      await upsertRole(target, targetRole);
      await appendReport('ROLE_UPSERT', `By=${actor}, User=@${target}, Role=${targetRole}`);
      return sendMessage(chatId, `✅ Role updated\n👤 @${target}\n🔐 ${targetRole}`);
    }

    if (command === '/removesuperadmin' || command === '/removeadmin' || command === '/removemember') {
      if (parts.length < 2) return sendMessage(chatId, `⚠️ Usage:\n${command} | username`);
      const target = cleanUsername(parts[1]);
      const targetRole = command === '/removesuperadmin' ? 'super_admin' : command === '/removeadmin' ? 'admin' : 'member';
      if (!target) return sendMessage(chatId, '⚠️ Username required');
      if (BOOTSTRAP_SUPER_ADMINS.includes(target) && targetRole === 'super_admin') {
        return sendMessage(chatId, '⚠️ Cannot remove bootstrap super admin from env');
      }
      const removed = await removeRole(target, targetRole);
      await appendReport('ROLE_REMOVE', `By=${actor}, User=@${target}, Role=${targetRole}, Removed=${removed}`);
      return sendMessage(chatId, removed ? `🗑️ Role removed\n👤 @${target}\n🔐 ${targetRole}` : `ℹ️ User not found in role list\n👤 @${target}\n🔐 ${targetRole}`);
    }

    if (command === '/confirm') {
      if (parts.length < 2) return sendMessage(chatId, 'Usage:\n/confirm | CODE');
      const code = clean(parts[1]);
      const resultMessage = await confirmPendingActionByCode(code, msg, role, actorCtx);
      return sendMessage(chatId, resultMessage);
    }

    if (command === '/cancel') {
      if (parts.length < 2) return sendMessage(chatId, 'Usage:\n/cancel | CODE');
      const code = clean(parts[1]);
      const resultMessage = await cancelPendingActionByCode(code, actorCtx);
      return sendMessage(chatId, resultMessage);
    }

    if (command === '/additem') {
      if (parts.length < 4) return sendMessage(chatId, '⚠️ Usage:\n/additem | Item Name | MinAlert | Unit');
      const itemName = clean(parts[1]);
      const minAlert = Number(parts[2]);
      const unit = clean(parts[3]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name\n- Required\n- Max 100 chars\n- No newline\n- "|" is not allowed');
      if (Number.isNaN(minAlert) || minAlert < 0) return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
      if (!isValidUnit(unit)) return sendMessage(chatId, '⚠️ Invalid Unit');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const data = await getData(deptCtx.stockSheet);
        const existing = findRowIndex(data, itemName);
        if (existing !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${itemName}`);
        await appendRow(deptCtx.stockSheet, [itemName, 0, 0, 0, minAlert, unit, nowIso()]);
        await runNonCriticalTask('Append report error:', () =>
          appendReport('ADD_ITEM', `By=${actor}, Department=${deptCtx.department}, Item=${itemName}, MinAlert=${minAlert}, Unit=${unit}`)
        );
        return sendMessage(chatId, `✅ Item Added\n\n💊 Item: ${itemName}\n⚠️ MinAlert: ${minAlert}\n📦 Unit: ${unit}`);
      }));
    }

    if (command === '/additemsbulk') {
      const entries = stripHeader(rawText);
      if (entries.length === 0) return sendLongMessage(chatId, '⚠️ Usage:\n/additemsbulk\nItem | MinAlert | Unit\nItem2 | MinAlert | Unit');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const results = await processBulkAddItems(entries, actorCtx, deptCtx);
      return sendLongMessage(chatId, `📦 Bulk Add Result\n\n${results.join('\n')}`);
    }

    if (command === '/inbulk') {
      const entries = stripHeader(rawText);
      if (entries.length === 0) return sendLongMessage(chatId, '⚠️ Usage:\n/inbulk\nItem | Qty | Optional Note\nItem2 | Qty | Optional Note');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const results = await processBulkMovement(entries, 'IN', actorCtx, role, deptCtx);
      return sendLongMessage(chatId, `📥 Bulk IN Result\n\n${results.join('\n')}`);
    }

    if (command === '/outbulk') {
      const entries = stripHeader(rawText);
      if (entries.length === 0) return sendLongMessage(chatId, '⚠️ Usage:\n/outbulk\nItem | Qty | Optional Note\nItem2 | Qty | Optional Note');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const results = await processBulkMovement(entries, 'OUT', actorCtx, role, deptCtx);
      return sendLongMessage(chatId, `📤 Bulk OUT Result\n\n${results.join('\n')}`);
    }

    if (command === '/in') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/in | Item Name | Qty | Optional Note');
      const itemName = clean(parts[1]);
      const { qty, note } = parseQtyAndOptionalNote(parts);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      if (!assertValidQty(qty)) return sendMessage(chatId, '⚠️ Qty must be greater than 0');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
        if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const r = ref.row;
        const currentIn = toNumber(r[1]);
        const currentOut = toNumber(r[2]);
        const minAlert = toNumber(r[4]);
        const unit = clean(r[5] || '');
        const oldBalance = getBalanceFromRow(r);
        const newIn = currentIn + qty;
        const newBalance = newIn - currentOut;
        const updated = [r[0], newIn, currentOut, newBalance, minAlert, unit, nowIso()];
        await updateRow(deptCtx.stockSheet, ref.rowIndex, updated);
        await Promise.all([
          runNonCriticalTask('Append log error:', () =>
            appendLog(deptCtx.logsSheet, [nowIso(), 'IN', r[0], qty, oldBalance, newBalance, unit, actorCtx.chatId, actorCtx.chatTitle, actor, role, note])
          ),
          runNonCriticalTask('Append report error:', () =>
            appendReport('IN', `By=${actor}, Department=${deptCtx.department}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}, Note=${note}`)
          )
        ]);
        return sendMessage(chatId, `📥 Stock Updated\n\n💊 Item: ${r[0]}\n➕ Qty In: ${qty}\n📦 Balance: ${newBalance} ${unit}` + (note ? `\n📝 Note: ${note}` : ''));
      }));
    }

    if (command === '/out') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/out | Item Name | Qty | Optional Note');
      const itemName = clean(parts[1]);
      const { qty, note } = parseQtyAndOptionalNote(parts);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      if (!assertValidQty(qty)) return sendMessage(chatId, '⚠️ Qty must be greater than 0');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
        if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const r = ref.row;
        const currentIn = toNumber(r[1]);
        const currentOut = toNumber(r[2]);
        const minAlert = toNumber(r[4]);
        const unit = clean(r[5] || '');
        const oldBalance = getBalanceFromRow(r);
        if (qty > oldBalance) return sendMessage(chatId, `❌ Not enough stock\n📦 Balance: ${oldBalance} ${unit}`);
        const newOut = currentOut + qty;
        const newBalance = currentIn - newOut;
        const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
        await updateRow(deptCtx.stockSheet, ref.rowIndex, updated);
        await Promise.all([
          runNonCriticalTask('Append log error:', () =>
            appendLog(deptCtx.logsSheet, [nowIso(), 'OUT', r[0], qty, oldBalance, newBalance, unit, actorCtx.chatId, actorCtx.chatTitle, actor, role, note])
          ),
          runNonCriticalTask('Append report error:', () =>
            appendReport('OUT', `By=${actor}, Department=${deptCtx.department}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}, Note=${note}`)
          )
        ]);
        await sendMessage(chatId, `📤 Stock Updated\n\n💊 Item: ${r[0]}\n➖ Qty Out: ${qty}\n📦 Balance: ${newBalance} ${unit}` + (note ? `\n📝 Note: ${note}` : ''));
        await checkAndSendLowStockAlert(chatId, updated);
      }));
    }

    if (command === '/adjust') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/adjust | Item Name | NewBalance');
      const itemName = clean(parts[1]);
      const newBalance = Number(parts[2]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      if (Number.isNaN(newBalance) || newBalance < 0) return sendMessage(chatId, '⚠️ NewBalance must be a valid number >= 0');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
      const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
        if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const r = ref.row;
        const currentIn = toNumber(r[1]);
        const currentBalance = getBalanceFromRow(r);
        const unit = clean(r[5] || '');
        if (newBalance > currentIn) {
          return sendMessage(chatId, `⚠️ NewBalance cannot be greater than total In (${currentIn}).\nCurrent design keeps In unchanged and recalculates Out.`);
        }
        const pending = await createPendingAction(username, actorCtx.userId, chatId, 'adjust', {
          itemName,
          newBalance,
          stockSheet: deptCtx.stockSheet,
          logsSheet: deptCtx.logsSheet,
          department: deptCtx.department
        });
        return sendMessage(
          chatId,
          `⚠️ Confirm Adjust Required\n\n` +
          `💊 Item: ${r[0]}\n📦 Current Balance: ${currentBalance} ${unit}\n✅ New Balance: ${newBalance} ${unit}\n\n` +
          `🧾 Code: ${pending.code}\n⏳ Expires in ${CONFIRM_EXPIRE_MINUTES} minutes\n\n` +
          `Confirm:\n/confirm | ${pending.code}\n\nCancel:\n/cancel | ${pending.code}`,
          { replyMarkup: buildPendingActionInlineKeyboard(pending.code) }
        );
      }));
    }

    if (command === '/undo') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/undo | Item Name');
      const itemName = clean(parts[1]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      try {
        const deptCtx = await getDepartmentContextFromMessage(msg);
        const result = await undoLastAction(itemName, actorCtx, role, deptCtx);
        return sendMessage(chatId, result);
      } catch (err) {
        return sendMessage(chatId, `❌ Undo failed\nReason: ${err.message}`);
      }
    }

    if (command === '/search') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/search | keyword');
      const keyword = norm(parts[1]);
      if (!keyword) return sendMessage(chatId, '⚠️ Keyword required');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
      const matched = data.filter(r => norm(r[0]).includes(keyword));
      if (matched.length === 0) return sendMessage(chatId, `📭 No items found for: ${parts[1]}`);
      let msgOut = `🔎 Search Result: ${parts[1]}\n\n`;
      for (const r of matched.slice(0, 50)) {
        const item = clean(r[0] || '');
        const balance = getBalanceFromRow(r);
        const minAlert = toNumber(r[4]);
        const unit = clean(r[5] || '');
        const status = balance <= minAlert ? '🚨LOW' : '✅OK';
        msgOut += `💊 ${item}\n📦 ${balance} ${unit} | ⚠️ Min: ${minAlert} | ${status}\n\n`;
      }
      if (matched.length > 50) msgOut += `...and ${matched.length - 50} more items`;
      return sendLongMessage(chatId, msgOut.trim());
    }

    if (command === '/stock') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/stock | Item Name');
      const itemName = clean(parts[1]);
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
      if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
      await sendMessage(chatId, `📊 Stock Info\n\n${buildStockMessage(ref.row)}`, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/allstock') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
      if (data.length === 0) return sendMessage(chatId, '📭 No stock data', { replyMarkup: replyKeyboard });
      const mode = clean(parts[1] || '');
      const rows = buildDepartmentStockRows(data);
      const msgOut = buildAllStockMessage(rows, deptCtx.department, mode);
      await sendLongMessage(chatId, msgOut, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/restocklist') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
      if (data.length === 0) return sendMessage(chatId, '📭 No stock data', { replyMarkup: replyKeyboard });
      const rows = buildDepartmentStockRows(data);
      const msgOut = buildRestockListMessage(rows, deptCtx.department);
      await sendLongMessage(chatId, msgOut, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/lowstock' || command === '/alertstock') {

      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
      if (data.length === 0) return sendMessage(chatId, '📭 No stock data', { replyMarkup: replyKeyboard });
      const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4]));
      if (lowItems.length === 0) return sendMessage(chatId, '✅ No low stock items', { replyMarkup: replyKeyboard });
      let msgOut = '🚨 Low Stock Items\n\n';
      for (const r of lowItems) {
        const item = clean(r[0] || '');
        const balance = getBalanceFromRow(r);
        const minAlert = toNumber(r[4]);
        const unit = clean(r[5] || '');
        msgOut += `💊 ${item}: ${balance} ${unit} (Min: ${minAlert})\n`;
      }
      await sendLongMessage(chatId, msgOut, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/setalert') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/setalert | Item Name | MinAlert');
      const itemName = clean(parts[1]);
      const minAlert = Number(parts[2]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      if (Number.isNaN(minAlert) || minAlert < 0) return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
        if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const r = ref.row;
        const currentIn = toNumber(r[1]);
        const currentOut = toNumber(r[2]);
        const balance = getBalanceFromRow(r);
        const unit = clean(r[5] || '');
        await updateRow(deptCtx.stockSheet, ref.rowIndex, [r[0], currentIn, currentOut, balance, minAlert, unit, nowIso()]);
        await runNonCriticalTask('Append report error:', () =>
          appendReport('SET_ALERT', `By=${actor}, Department=${deptCtx.department}, Item=${r[0]}, MinAlert=${minAlert}`)
        );
        return sendMessage(chatId, `✅ MinAlert Updated\n\n💊 Item: ${r[0]}\n⚠️ New MinAlert: ${minAlert}`);
      }));
    }

    if (command === '/setunit') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/setunit | Item Name | Unit');
      const itemName = clean(parts[1]);
      const unit = clean(parts[2]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      if (!isValidUnit(unit)) return sendMessage(chatId, '⚠️ Invalid Unit');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const ref = await getStockRowByItemName(itemName, deptCtx.stockSheet);
        if (!ref) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const r = ref.row;
        await updateRow(deptCtx.stockSheet, ref.rowIndex, [r[0], toNumber(r[1]), toNumber(r[2]), getBalanceFromRow(r), toNumber(r[4]), unit, nowIso()]);
        await runNonCriticalTask('Append report error:', () =>
          appendReport('SET_UNIT', `By=${actor}, Item=${r[0]}, Unit=${unit}`)
        );
        return sendMessage(chatId, `✅ Unit Updated\n\n💊 Item: ${r[0]}\n📦 New Unit: ${unit}`);
      }));
    }

    if (command === '/renameitem') {
      if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/renameitem | Old Name | New Name');
      const oldName = clean(parts[1]);
      const newName = clean(parts[2]);
      if (!isValidItemName(oldName) || !isValidItemName(newName)) return sendMessage(chatId, '⚠️ Invalid old name or new name');
      if (norm(oldName) === norm(newName)) return sendMessage(chatId, '⚠️ Old name and new name are the same');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const lockKeys = [
        `stock:${deptCtx.stockSheet}:${oldName}`,
        `stock:${deptCtx.stockSheet}:${newName}`
      ].sort();
      return withStockSheetWriteLock(async () => withLock(lockKeys[0], async () => withLock(lockKeys[1], async () => {
        const data = await getData(deptCtx.stockSheet);
        const oldRow = findRowIndex(data, oldName);
        if (oldRow === -1) return sendMessage(chatId, `❌ Item not found: ${oldName}`);
        const existingNew = findRowIndex(data, newName);
        if (existingNew !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${newName}`);
        const r = data[oldRow];
        await updateRow(deptCtx.stockSheet, oldRow, [newName, toNumber(r[1]), toNumber(r[2]), getBalanceFromRow(r), toNumber(r[4]), clean(r[5] || ''), nowIso()]);
        await runNonCriticalTask('Append report error:', () =>
          appendReport('RENAME_ITEM', `By=${actor}, ${oldName} -> ${newName}`)
        );
        return sendMessage(chatId, `✅ Item Renamed\n\n📝 Old: ${oldName}\n✨ New: ${newName}`);
      })));
    }

    if (command === '/deleteitem') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/deleteitem | Item Name');
      const itemName = clean(parts[1]);
      if (!isValidItemName(itemName)) return sendMessage(chatId, '⚠️ Invalid item name');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      return withStockSheetWriteLock(async () => withLock(`stock:${deptCtx.stockSheet}:${itemName}`, async () => {
        const data = await getData(deptCtx.stockSheet);
        const row = findRowIndex(data, itemName);
        if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);
        const pending = await createPendingAction(username, actorCtx.userId, chatId, 'deleteitem', { itemName, stockSheet: deptCtx.stockSheet, department: deptCtx.department });
        return sendMessage(
          chatId,
          `⚠️ Confirm Delete Required\n\n💊 Item: ${itemName}\n\n🧾 Code: ${pending.code}\n⏳ Expires in ${CONFIRM_EXPIRE_MINUTES} minutes\n\nConfirm:\n/confirm | ${pending.code}\n\nCancel:\n/cancel | ${pending.code}`,
          { replyMarkup: buildPendingActionInlineKeyboard(pending.code) }
        );
      }));
    }

    if (command === '/history') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/history | Item Name');
      const itemName = clean(parts[1]);
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const logs = await getLogs(deptCtx.logsSheet);
      const filtered = logs.filter(r => norm(r[2]) === norm(itemName));
      if (filtered.length === 0) return sendMessage(chatId, `📭 No history found for: ${itemName}`);
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
        const emoji = type === 'IN' ? '📥' : type === 'OUT' ? '📤' : type === 'ADJUST' ? '🛠' : type === 'UNDO' ? '↩️' : '📝';
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

    if (command === '/itemlogs') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/itemlogs | Item Name | limit');
      const itemName = clean(parts[1]);
      const limit = parts.length >= 3 ? Math.max(1, Math.min(100, Number(parts[2]) || 20)) : 20;
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const logs = await getLogs(deptCtx.logsSheet);
      const filtered = logs.filter(r => norm(r[2]) === norm(itemName));
      if (filtered.length === 0) return sendMessage(chatId, `📭 No logs found for: ${itemName}`);
      const recent = filtered.slice(-limit);
      let msgOut = `📜 Item Logs: ${itemName} (last ${recent.length})\n\n`;
      for (const r of recent) {
        const ts = clean(r[0] || '');
        const type = clean(r[1] || '');
        const qty = toNumber(r[3]);
        const beforeBal = toNumber(r[4]);
        const afterBal = toNumber(r[5]);
        const unit = clean(r[6] || '');
        const user = clean(r[9] || '');
        const note = clean(r[11] || '');
        msgOut += `🕒 ${ts}\n`;
        msgOut += `🔁 ${type} | Qty=${qty} | ${beforeBal} -> ${afterBal} ${unit}\n`;
        if (user) msgOut += `👤 ${user}\n`;
        if (note) msgOut += `📝 ${note}\n`;
        msgOut += '\n';
      }
      return sendLongMessage(chatId, msgOut.trim());
    }

    if (command === '/today') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const logs = await getLogs(deptCtx.logsSheet);
      const summary = summarizeTodayLogs(logs, APP_TIMEZONE);
      const totalTx = summary.inCount + summary.outCount + summary.adjustCount + summary.undoCount;
      if (totalTx === 0) {
        return sendMessage(chatId, `📭 No transactions today (${summary.today}, ${APP_TIMEZONE})`, { replyMarkup: replyKeyboard });
      }
      const msgOut =
        `📅 Today Summary (${summary.today}, ${APP_TIMEZONE})\n\n` +
        `📥 IN Transactions: ${summary.inCount}\n➕ Total IN Qty: ${summary.inQty}\n\n` +
        `📤 OUT Transactions: ${summary.outCount}\n➖ Total OUT Qty: ${summary.outQty}\n\n` +
        `🛠 Adjust Transactions: ${summary.adjustCount}\n↩️ Undo Transactions: ${summary.undoCount}\n\n` +
        `🧾 Total Transactions: ${totalTx}`;
      await sendMessage(chatId, msgOut, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/report') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
      const summary = summarizeStock(data);
      let msgOut =
        `📊 Stock Report\n\n` +
        `📦 Total Items: ${summary.totalItems}\n` +
        `🚨 Low Stock Items: ${summary.lowStockCount}\n` +
        `🔢 Total Balance Qty: ${summary.totalBalance}\n\n`;
      const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4]));
      if (lowItems.length > 0) {
        msgOut += '🚨 Low Stock List:\n';
        for (const r of lowItems) {
          msgOut += `💊 ${r[0]}: ${getBalanceFromRow(r)} ${clean(r[5] || '')} (Min: ${toNumber(r[4])})\n`;
        }
      } else {
        msgOut += '✅ No low stock items';
      }
      await appendReport('REPORT_VIEW', `By=${actor}, TotalItems=${summary.totalItems}, LowStock=${summary.lowStockCount}, TotalBalance=${summary.totalBalance}`);
      return sendLongMessage(chatId, msgOut);
    }

    if (command === '/dashboard') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const [data, logs] = await Promise.all([getData(deptCtx.stockSheet), getLogs(deptCtx.logsSheet)]);
      const stockSummary = summarizeStock(data);
      const todaySummary = summarizeTodayLogs(logs, APP_TIMEZONE);
      const lowItems = data.filter(r => getBalanceFromRow(r) <= toNumber(r[4])).slice(0, 10);
      let msgOut =
        `📊 Dashboard\n\n` +
        `📦 Total Items: ${stockSummary.totalItems}\n` +
        `🚨 Low Stock Items: ${stockSummary.lowStockCount}\n` +
        `🔢 Total Balance Qty: ${stockSummary.totalBalance}\n\n` +
        `📅 Today (${todaySummary.today}, ${APP_TIMEZONE})\n` +
        `📥 IN: ${todaySummary.inCount} tx | Qty ${todaySummary.inQty}\n` +
        `📤 OUT: ${todaySummary.outCount} tx | Qty ${todaySummary.outQty}\n` +
        `🛠 ADJUST: ${todaySummary.adjustCount}\n` +
        `↩️ UNDO: ${todaySummary.undoCount}\n\n`;
      if (lowItems.length > 0) {
        msgOut += '🚨 Top Low Stock:\n';
        for (const r of lowItems) {
          msgOut += `💊 ${clean(r[0] || '')}: ${getBalanceFromRow(r)} ${clean(r[5] || '')} (Min ${toNumber(r[4])})\n`;
        }
      } else {
        msgOut += '✅ No low stock items';
      }
      await sendLongMessage(chatId, msgOut, { replyMarkup: replyKeyboard });
      return;
    }

    if (command === '/audit') {
      if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/audit | username');
      const target = cleanUsername(parts[1]);
      if (!target) return sendMessage(chatId, '⚠️ Username required');
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const logs = await getLogs(deptCtx.logsSheet);
      const matched = logs.filter(r => cleanUsername(r[9]) === target);
      if (matched.length === 0) return sendMessage(chatId, `📭 No activity found for @${target}`);
      const recent = matched.slice(-30);
      let msgOut = `🕵️ Audit: @${target}\n\n`;
      for (const r of recent) {
        const ts = clean(r[0] || '');
        const type = clean(r[1] || '');
        const item = clean(r[2] || '');
        const qty = toNumber(r[3]);
        const beforeBal = toNumber(r[4]);
        const afterBal = toNumber(r[5]);
        const unit = clean(r[6] || '');
        const roleUsed = clean(r[10] || '');
        const note = clean(r[11] || '');
        msgOut += `🕒 ${ts}\n`;
        msgOut += `🔁 ${type} | 💊 ${item} | Qty=${qty} | ${beforeBal} -> ${afterBal} ${unit}`;
        if (roleUsed) msgOut += ` | 🔐 ${roleUsed}`;
        if (note) msgOut += `\n📝 ${note}`;
        msgOut += '\n\n';
      }
      return sendLongMessage(chatId, msgOut.trim());
    }

    if (command === '/health' || command === '/stats') {
      const healthMsg = await buildHealthMessage();
      return sendMessage(chatId, healthMsg);
    }

    if (command === '/exportsummary') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const data = await getData(deptCtx.stockSheet);
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
      const csv = '\uFEFF' + rows.map(row => row.map(escapeCsv).join(',')).join('\n');
      await appendReport('EXPORT_SUMMARY', `By=${actor}, Exported ${data.length} items`);
      return sendDocument(chatId, `stock-summary-${getLocalDateString(APP_TIMEZONE)}.csv`, csv);
    }

    if (command === '/exportlogs') {
      const deptCtx = await getDepartmentContextFromMessage(msg);
      const logs = await getLogs(deptCtx.logsSheet);
      if (logs.length === 0) return sendMessage(chatId, '📭 No logs to export');
      const rows = [[
        'Timestamp', 'Type', 'Item', 'Qty', 'BalanceBefore', 'BalanceAfter',
        'Unit', 'ChatId', 'ChatTitle', 'Username', 'Role', 'Note'
      ]];
      for (const r of logs) {
        rows.push([
          clean(r[0] || ''), clean(r[1] || ''), clean(r[2] || ''), toNumber(r[3]),
          toNumber(r[4]), toNumber(r[5]), clean(r[6] || ''), clean(r[7] || ''),
          clean(r[8] || ''), clean(r[9] || ''), clean(r[10] || ''), clean(r[11] || '')
        ]);
      }
      const csv = '\uFEFF' + rows.map(row => row.map(escapeCsv).join(',')).join('\n');
      await appendReport('EXPORT_LOGS', `By=${actor}, Exported ${logs.length} logs`);
      return sendDocument(chatId, `stock-logs-${getLocalDateString(APP_TIMEZONE)}.csv`, csv);
    }

    return sendMessage(chatId, '❌ Unknown command. Use /help');
  } finally {
    if (processedMessageKey && !processedMessageMarked) releaseProcessedMessage(processedMessageKey);
  }
}

/**************** CALLBACK QUERY HANDLER ****************/
async function handleCallbackQuery(callbackQuery) {
  const data = clean(callbackQuery?.data || '');
  const callbackId = callbackQuery?.id;
  const msg = buildCallbackMsgLike(callbackQuery);

  if (!data || !msg?.chat?.id) {
    await answerCallbackQuery(callbackId, 'Invalid action', true);
    return;
  }

  const actorCtx = getActorContext(msg);
  const username = actorCtx.username;
  const chatId = msg.chat.id;

  const [roleRows, allowedChatRows] = await Promise.all([getRoles(), getAllowedChats()]);
  const role = getUserRoleFromRows(username, roleRows);

  if (data.startsWith('QA|')) {
    const quickAction = clean(data.split('|')[1] || '');

    if (isGroupChat(msg)) {
      if (allowedChatRows.length === 0 || !isAllowedChatId(chatId, allowedChatRows)) {
        await answerCallbackQuery(callbackId, 'This group is not allowed', true);
        return;
      }
    }

    await clearInlineKeyboardFromCallback(callbackQuery);

    if (quickAction === 'ADDITEM') {
      if (!(role === 'super_admin' || role === 'admin')) {
        await answerCallbackQuery(callbackId, 'You are not allowed', true);
        return;
      }
      clearInputSession(msg);
      setInputSession(msg, { action: 'ADDITEM', step: 'itemName', data: {} });
      await answerCallbackQuery(callbackId, 'Add item');
      await sendMessage(chatId, buildSessionPrompt('ADDITEM', 'itemName'));
      return;
    }

    if (quickAction === 'IN') {
      if (!canUseCommand(role, '/in')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      setInputSession(msg, { action: 'IN', step: 'itemName', data: {} });
      await answerCallbackQuery(callbackId, 'IN');
      await sendMessage(chatId, buildSessionPrompt('IN', 'itemName'));
      return;
    }

    if (quickAction === 'OUT') {
      if (!canUseCommand(role, '/out')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      setInputSession(msg, { action: 'OUT', step: 'itemName', data: {} });
      await answerCallbackQuery(callbackId, 'OUT');
      await sendMessage(chatId, buildSessionPrompt('OUT', 'itemName'));
      return;
    }

    if (quickAction === 'STOCK') {
      if (!canUseCommand(role, '/stock')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      setInputSession(msg, { action: 'STOCK', step: 'itemName', data: {} });
      await answerCallbackQuery(callbackId, 'STOCK');
      await sendMessage(chatId, buildSessionPrompt('STOCK', 'itemName'));
      return;
    }

    if (quickAction === 'INBULK') {
      if (!canUseCommand(role, '/inbulk')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'IN BULK');
      await sendMessage(chatId, '📥➕ IN BULK\n\nសូមផ្ញើជាបន្ទាត់ច្រើន:\n/inbulk\nItem | Qty | Optional Note\nItem2 | Qty | Optional Note\n\nឬវាយផ្ទាល់ command ចាស់ក៏បាន។');
      return;
    }

    if (quickAction === 'OUTBULK') {
      if (!canUseCommand(role, '/outbulk')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'OUT BULK');
      await sendMessage(chatId, '📤➖ OUT BULK\n\nសូមផ្ញើជាបន្ទាត់ច្រើន:\n/outbulk\nItem | Qty | Optional Note\nItem2 | Qty | Optional Note\n\nឬវាយផ្ទាល់ command ចាស់ក៏បាន។');
      return;
    }

    if (quickAction === 'LOWSTOCK') {
      if (!canUseCommand(role, '/lowstock')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'Low stock');
      await handleCommand({ ...msg, text: '/lowstock' });
      return;
    }

    if (quickAction === 'ALLSTOCK') {
      if (!canUseCommand(role, '/allstock')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'All stock');
      await handleCommand({ ...msg, text: '/allstock' });
      return;
    }

    if (quickAction === 'RESTOCKLIST') {
      if (!canUseCommand(role, '/restocklist')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'Restock list');
      await handleCommand({ ...msg, text: '/restocklist' });
      return;
    }

    if (quickAction === 'DASHBOARD') {
      if (!canUseCommand(role, '/dashboard')) { await answerCallbackQuery(callbackId, 'You are not allowed', true); return; }
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'Dashboard');
      await handleCommand({ ...msg, text: '/dashboard' });
      return;
    }

    if (quickAction === 'MENU') {
      clearInputSession(msg);
      await answerCallbackQuery(callbackId, 'Menu');
      await handleCommand({ ...msg, text: '/menu' });
      return;
    }

    await answerCallbackQuery(callbackId, 'Unknown quick action', true);
    return;
  }

  const [actionRaw, codeRaw] = data.split('|');
  const action = norm(actionRaw);
  const code = clean(codeRaw);

  if (!['confirm', 'cancel'].includes(action) || !code) {
    await answerCallbackQuery(callbackId, 'Invalid action', true);
    return;
  }

  if (isGroupChat(msg)) {
    if (allowedChatRows.length === 0 || !isAllowedChatId(chatId, allowedChatRows)) {
      await answerCallbackQuery(callbackId, 'This group is not allowed', true);
      return;
    }
  }

  if (!canUseCommand(role, action === 'confirm' ? '/confirm' : '/cancel')) {
    await answerCallbackQuery(callbackId, 'You are not allowed', true);
    return;
  }

  await clearInlineKeyboardFromCallback(callbackQuery);

  let resultMessage;
  if (action === 'confirm') resultMessage = await confirmPendingActionByCode(code, msg, role, actorCtx);
  else resultMessage = await cancelPendingActionByCode(code, actorCtx);

  const feedback = getCallbackFeedback(resultMessage, action === 'confirm' ? 'Confirmed' : 'Cancelled');
  await answerCallbackQuery(callbackId, feedback.text, feedback.showAlert);
  await sendMessage(chatId, resultMessage);
}

/**************** WEBHOOK ****************/
app.post('/webhook', async (req, res) => {
  const secret = req.get('x-telegram-bot-api-secret-token');
  if (secret !== TELEGRAM_WEBHOOK_SECRET) {
    return res.sendStatus(403);
  }

  try {
    const msg = req.body.message;
    const callbackQuery = req.body.callback_query;

    if (callbackQuery) {
      await handleCallbackQuery(callbackQuery);
      return res.sendStatus(200);
    }

    if (!msg || !msg.text) return res.sendStatus(200);

    const text = String(msg.text || '').trim();

    if (!text.startsWith('/')) {
      const consumedBySession = await processInputSessionMessage(msg);
      if (consumedBySession) return res.sendStatus(200);
    }

    await handleCommand(msg);
    return res.sendStatus(200);
  } catch (err) {
    console.error('Webhook error:', err.response?.data || err.message || err);
    return res.sendStatus(200);
  }
});

let startupState = { ready: false, initializedAt: '', error: '' };

app.get('/', (req, res) => {
  res.status(200).send(`✅ Bot running\nready=${startupState.ready}\ninitializedAt=${startupState.initializedAt || '-'}\nerror=${startupState.error || '-'}`);
});

app.get('/healthz', (req, res) => {
  res.status(200).json({ ok: true, ...startupState });
});

app.get('/webhook', (req, res) => {
  res.status(200).send('✅ Webhook endpoint is working');
});

/**************** BACKGROUND JOBS ****************/
function pruneInputSessions() {
  const now = Date.now();
  for (const [key, session] of inputSessionCache) {
    if (now - Number(session.updatedAt || 0) > INPUT_SESSION_TIMEOUT_MS) {
      inputSessionCache.delete(key);
    }
  }
}
function startBackgroundJobs() {
  setInterval(() => {
    runNonCriticalTask('pruneProcessedMessageCache error:', async () => {
      pruneProcessedMessageCache();
      pruneRateLimitCache();
      pruneInputSessions();
    });
  }, Math.max(60 * 1000, RATE_LIMIT_WINDOW_MS));

  setInterval(() => {
    runNonCriticalTask('autoCleanupSheets error:', autoCleanupSheets);
  }, CLEANUP_INTERVAL_MS);

  setInterval(() => {
    runNonCriticalTask('sendDailyLowStockSummariesIfDue error:', sendDailyLowStockSummariesIfDue);
  }, DAILY_REPORT_CHECK_INTERVAL_MS);
}

/**************** START SERVER ****************/
async function start() {
  const server = app.listen(PORT, () => {
    console.log('🚀 Server running on port ' + PORT);
    console.log('🌍 Timezone: ' + APP_TIMEZONE);
    console.log('📣 Daily summary time: ' + DAILY_REPORT_HOUR + ':' + String(DAILY_REPORT_MINUTE).padStart(2, '0'));
  });

  try {
    await setupSheet();
    await autoCleanupSheets();
    console.log('✅ setupSheet done');
    startupState = { ready: true, initializedAt: nowIso(), error: '' };
    startBackgroundJobs();
  } catch (err) {
    startupState = { ready: false, initializedAt: '', error: String(err.response?.data || err.message || err) };
    console.error('❌ Startup error:', err.response?.data || err.message || err);
    server.close(() => process.exit(1));
  }
}

process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down...');
  process.exit(0);
});

start();
