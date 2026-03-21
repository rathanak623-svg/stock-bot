const express = require('express');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

/**************** CONFIG ****************/
if (!process.env.TELEGRAM_TOKEN) throw new Error('Missing TELEGRAM_TOKEN');
if (!process.env.SPREADSHEET_ID) throw new Error('Missing SPREADSHEET_ID');
if (!process.env.GOOGLE_CLIENT_EMAIL) throw new Error('Missing GOOGLE_CLIENT_EMAIL');
if (!process.env.GOOGLE_PRIVATE_KEY) throw new Error('Missing GOOGLE_PRIVATE_KEY');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const PORT = process.env.PORT || 10000;

const BOOTSTRAP_SUPER_ADMINS = (process.env.SUPER_ADMINS || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;
const CONFIRM_EXPIRE_MINUTES = 10;

/**************** GOOGLE AUTH ****************/
const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_CLIENT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const sheets = google.sheets({ version: 'v4', auth });

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

function todayDateStringUTC() {
  return new Date().toISOString().slice(0, 10);
}

function minutesFromNowIso(minutes) {
  return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}

function isExpired(isoString) {
  if (!isoString) return true;
  return new Date(isoString).getTime() < Date.now();
}

function escapeCsv(value) {
  const s = String(value ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function getUsername(msg) {
  return cleanUsername(msg?.from?.username || '');
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
    await axios.post(`${TELEGRAM_API}/sendMessage`, {
      chat_id: chatId,
      text
    });
  } catch (err) {
    console.error('Telegram sendMessage error:', err.response?.data || err.message);
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

    await axios.post(`${TELEGRAM_API}/sendDocument`, form, {
      headers: form.getHeaders()
    });
  } catch (err) {
    console.error('Telegram sendDocument error:', err.response?.data || err.message);
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
  return toNumber(r[3], toNumber(r[1]) - toNumber(r[2]));
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

/**************** SHEET CONFIG ****************/
const SHEET_HEADERS = {
  Stock: ['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt'],
  Logs: ['Timestamp', 'Type', 'Item', 'Qty', 'BalanceBefore', 'BalanceAfter', 'Unit', 'ChatId', 'ChatTitle', 'Username', 'Role', 'Note'],
  Reports: ['Timestamp', 'Type', 'Details'],
  Roles: ['Username', 'Role', 'UpdatedAt'],
  AllowedChats: ['ChatId', 'ChatTitle', 'ChatType', 'AddedAt'],
  PendingActions: ['Code', 'Username', 'ChatId', 'Action', 'PayloadJson', 'Status', 'CreatedAt', 'ExpiresAt']
};

async function getSpreadsheetMeta() {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  return meta.data;
}

async function ensureSheetExists(title) {
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

async function ensureHeader(title, headers) {
  const endCol = String.fromCharCode(64 + headers.length);
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
  for (const title of Object.keys(SHEET_HEADERS)) {
    await ensureSheetExists(title);
    await ensureHeader(title, SHEET_HEADERS[title]);
  }
}

/**************** DATA ACCESS ****************/
async function getData() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Stock'!A2:G"
  });
  return res.data.values || [];
}

async function getLogs() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Logs'!A2:L"
  });
  return res.data.values || [];
}

async function getRoles() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Roles'!A2:C"
  });
  return res.data.values || [];
}

async function getAllowedChats() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'AllowedChats'!A2:D"
  });
  return res.data.values || [];
}

async function getPendingActions() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'PendingActions'!A2:H"
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
}

async function updateRow(rowIndex, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `'Stock'!A${rowIndex + 2}:G${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  });
}

async function appendLog(values) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Logs'!A:L",
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  });
}

async function appendReport(type, details) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Reports'!A:C",
    valueInputOption: 'RAW',
    requestBody: { values: [[nowIso(), type, details]] }
  });
}

async function deleteRowFromSheet(sheetTitle, rowIndexZeroBasedWithoutHeader) {
  const meta = await getSpreadsheetMeta();
  const sheet = (meta.sheets || []).find(s => s.properties.title === sheetTitle);
  if (!sheet) throw new Error(`${sheetTitle} sheet not found`);

  const sheetId = sheet.properties.sheetId;
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
  const balance = toNumber(row[3]);
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

/**************** CONTEXT HELPERS ****************/
function getChatMeta(msg) {
  return {
    chatId: msg?.chat?.id || '',
    chatTitle: clean(msg?.chat?.title || msg?.chat?.first_name || ''),
    username: clean(msg?.from?.username || msg?.from?.first_name || '')
  };
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
async function executeConfirmedAction(pending, msg, role, meta) {
  let payload;
  try {
    payload = JSON.parse(pending.payloadJson || '{}');
  } catch {
    throw new Error('Invalid pending payload');
  }

  const data = await getData();

  if (pending.action === 'deleteitem') {
    const itemName = clean(payload.itemName);
    const row = findRowIndex(data, itemName);
    if (row === -1) {
      throw new Error(`Item not found: ${itemName}`);
    }

    await deleteRowFromSheet('Stock', row);
    await appendReport('DELETE_ITEM', `By=@${meta.username}, Deleted item=${itemName}`);
    return `🗑️ Deleted Item: ${itemName}`;
  }

  if (pending.action === 'adjust') {
    const itemName = clean(payload.itemName);
    const newBalance = Number(payload.newBalance);

    if (Number.isNaN(newBalance) || newBalance < 0) {
      throw new Error('Invalid NewBalance');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) {
      throw new Error(`Item not found: ${itemName}`);
    }

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const oldBalance = toNumber(r[3]);

    if (newBalance > currentIn) {
      throw new Error(`NewBalance cannot be greater than total In (${currentIn})`);
    }

    const newOut = currentIn - newBalance;
    const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
    await updateRow(row, updated);

    await appendLog([
      nowIso(), 'ADJUST', r[0], 0, oldBalance, newBalance, unit,
      meta.chatId, meta.chatTitle, meta.username, role, `Adjust balance to ${newBalance}`
    ]);

    await appendReport('ADJUST', `By=@${meta.username}, Item=${r[0]}, Before=${oldBalance}, After=${newBalance}`);
    await checkAndSendLowStockAlert(meta.chatId, updated);

    return (
      `🛠 Balance Adjusted\n\n` +
      `💊 Item: ${r[0]}\n` +
      `📦 Old Balance: ${oldBalance} ${unit}\n` +
      `✅ New Balance: ${newBalance} ${unit}`
    );
  }

  throw new Error(`Unknown pending action: ${pending.action}`);
}

/**************** COMMAND HANDLER ****************/
async function handleCommand(msg) {
  const chatId = msg.chat.id;
  const text = msg.text;
  const meta = getChatMeta(msg);
  const chatType = getChatType(msg);

  const parts = parsePipe(text);
  const command = norm(parts[0]);

  if (!command) {
    return sendMessage(chatId, '⚠️ Invalid command');
  }

  const roleRows = await getRoles();
  const allowedChatRows = await getAllowedChats();
  const username = getUsername(msg);
  const role = getUserRoleFromRows(username, roleRows);

  if (isGroupChat(msg)) {
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

  if (!canUseCommand(role, command)) {
    const displayUser = msg?.from?.username ? `@${msg.from.username}` : 'this user';
    return sendMessage(
      chatId,
      `⛔ Sorry ${displayUser}\nYou are not allowed to use ${command}\n👤 Your role: ${role}`
    );
  }

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
      '⛔ You are not registered to use this bot.\nPlease ask Super Admin to add your username.'
    );
  }

  if (command === '/myrole') {
    return sendMessage(
      chatId,
      `👤 Username: ${username ? '@' + username : 'no username'}\n🔐 Role: ${role}`
    );
  }

  if (command === '/roles') {
    const rows = await getRoles();
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
    const rows = await getAllowedChats();
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

    const added = await addAllowedChat(chatId, meta.chatTitle, chatType);
    await appendReport('ALLOW_GROUP', `ChatId=${chatId}, Title=${meta.chatTitle}, Added=${added}`);
    return sendMessage(
      chatId,
      added
        ? `✅ Group allowed\n🆔 ${chatId}\n🏷 ${meta.chatTitle}`
        : `ℹ️ Group already allowed\n🆔 ${chatId}\n🏷 ${meta.chatTitle}`
    );
  }

  if (command === '/disallowgroup') {
    if (!isGroupChat(msg)) {
      return sendMessage(chatId, '⚠️ /disallowgroup can only be used inside a group');
    }

    const removed = await removeAllowedChat(chatId);
    await appendReport('DISALLOW_GROUP', `ChatId=${chatId}, Title=${meta.chatTitle}, Removed=${removed}`);
    return sendMessage(
      chatId,
      removed
        ? `🗑️ Group removed from whitelist\n🆔 ${chatId}\n🏷 ${meta.chatTitle}`
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
    await appendReport('ROLE_UPSERT', `By=@${username}, User=@${target}, Role=${targetRole}`);

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
    await appendReport('ROLE_REMOVE', `By=@${username}, User=@${target}, Role=${targetRole}, Removed=${removed}`);

    return sendMessage(
      chatId,
      removed
        ? `🗑️ Role removed\n👤 @${target}\n🔐 ${targetRole}`
        : `ℹ️ User not found in role list\n👤 @${target}\n🔐 ${targetRole}`
    );
  }

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

    try {
      const resultMessage = await executeConfirmedAction(pending, msg, role, meta);
      await updatePendingActionStatus(pending.rowIndex, 'confirmed');
      return sendMessage(chatId, `✅ Confirmed\nCode: ${code}\n\n${resultMessage}`);
    } catch (err) {
      await updatePendingActionStatus(pending.rowIndex, 'failed');
      return sendMessage(chatId, `❌ Failed to execute confirmation\nCode: ${code}\nReason: ${err.message}`);
    }
  }

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

  const data = await getData();

  if (command === '/additem') {
    if (parts.length < 4) {
      return sendMessage(chatId, '⚠️ Usage:\n/additem | Item Name | MinAlert | Unit');
    }

    const itemName = clean(parts[1]);
    const minAlert = Number(parts[2]);
    const unit = clean(parts[3]);

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }
    if (!unit) return sendMessage(chatId, '⚠️ Unit required');

    const existing = findRowIndex(data, itemName);
    if (existing !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${itemName}`);

    await appendRow([itemName, 0, 0, 0, minAlert, unit, nowIso()]);
    await appendReport('ADD_ITEM', `By=@${username}, Item=${itemName}, MinAlert=${minAlert}, Unit=${unit}`);

    return sendMessage(
      chatId,
      `✅ Item Added\n\n💊 Item: ${itemName}\n⚠️ MinAlert: ${minAlert}\n📦 Unit: ${unit}`
    );
  }

  if (command === '/in') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/in | Item Name | Qty');

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (Number.isNaN(qty) || qty <= 0) return sendMessage(chatId, '⚠️ Qty must be greater than 0');

    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const oldBalance = currentIn - currentOut;

    const newIn = currentIn + qty;
    const newBalance = newIn - currentOut;

    const updated = [r[0], newIn, currentOut, newBalance, minAlert, unit, nowIso()];
    await updateRow(row, updated);

    await appendLog([
      nowIso(), 'IN', r[0], qty, oldBalance, newBalance, unit,
      meta.chatId, meta.chatTitle, username, role, ''
    ]);

    await appendReport('IN', `By=@${username}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}`);

    return sendMessage(
      chatId,
      `📥 Stock Updated\n\n💊 Item: ${r[0]}\n➕ Qty In: ${qty}\n📦 Balance: ${newBalance} ${unit}`
    );
  }

  if (command === '/out') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/out | Item Name | Qty');

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (Number.isNaN(qty) || qty <= 0) return sendMessage(chatId, '⚠️ Qty must be greater than 0');

    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const oldBalance = currentIn - currentOut;

    if (qty > oldBalance) {
      return sendMessage(chatId, `❌ Not enough stock\n📦 Balance: ${oldBalance} ${unit}`);
    }

    const newOut = currentOut + qty;
    const newBalance = currentIn - newOut;

    const updated = [r[0], currentIn, newOut, newBalance, minAlert, unit, nowIso()];
    await updateRow(row, updated);

    await appendLog([
      nowIso(), 'OUT', r[0], qty, oldBalance, newBalance, unit,
      meta.chatId, meta.chatTitle, username, role, ''
    ]);

    await appendReport('OUT', `By=@${username}, Item=${r[0]}, Qty=${qty}, Before=${oldBalance}, After=${newBalance}`);

    await sendMessage(
      chatId,
      `📤 Stock Updated\n\n💊 Item: ${r[0]}\n➖ Qty Out: ${qty}\n📦 Balance: ${newBalance} ${unit}`
    );

    await checkAndSendLowStockAlert(chatId, updated);
    return;
  }

  if (command === '/adjust') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/adjust | Item Name | NewBalance');

    const itemName = clean(parts[1]);
    const newBalance = Number(parts[2]);

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (Number.isNaN(newBalance) || newBalance < 0) {
      return sendMessage(chatId, '⚠️ NewBalance must be a valid number >= 0');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentBalance = toNumber(r[3]);
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
  }

  if (command === '/search') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/search | keyword');

    const keyword = norm(parts[1]);
    if (!keyword) return sendMessage(chatId, '⚠️ Keyword required');

    const matched = data.filter(r => norm(r[0]).includes(keyword));

    if (matched.length === 0) {
      return sendMessage(chatId, `📭 No items found for: ${parts[1]}`);
    }

    let msgOut = `🔎 Search Result: ${parts[1]}\n\n`;
    for (const r of matched.slice(0, 50)) {
      const item = clean(r[0] || '');
      const balance = toNumber(r[3]);
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
    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const balance = toNumber(r[3]);
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
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data');

    let msgOut = '📋 All Stock\n\n';
    for (const r of data) {
      const item = clean(r[0] || '');
      const balance = toNumber(r[3]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const status = balance <= minAlert ? ' 🚨LOW' : ' ✅OK';
      msgOut += `💊 ${item}: ${balance} ${unit}${status}\n`;
    }
    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/lowstock' || command === '/alertstock') {
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data');

    const lowItems = data.filter(r => toNumber(r[3]) <= toNumber(r[4]));
    if (lowItems.length === 0) return sendMessage(chatId, '✅ No low stock items');

    let msgOut = '🚨 Low Stock Items\n\n';
    for (const r of lowItems) {
      const item = clean(r[0] || '');
      const balance = toNumber(r[3]);
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

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const balance = toNumber(r[3]);
    const unit = clean(r[5] || '');

    await updateRow(row, [r[0], currentIn, currentOut, balance, minAlert, unit, nowIso()]);
    await appendReport('SET_ALERT', `By=@${username}, Item=${r[0]}, MinAlert=${minAlert}`);

    return sendMessage(
      chatId,
      `✅ MinAlert Updated\n\n💊 Item: ${r[0]}\n⚠️ New MinAlert: ${minAlert}`
    );
  }

  if (command === '/setunit') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/setunit | Item Name | Unit');

    const itemName = clean(parts[1]);
    const unit = clean(parts[2]);

    if (!itemName) return sendMessage(chatId, '⚠️ Item name required');
    if (!unit) return sendMessage(chatId, '⚠️ Unit required');

    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const r = data[row];
    await updateRow(row, [r[0], toNumber(r[1]), toNumber(r[2]), toNumber(r[3]), toNumber(r[4]), unit, nowIso()]);
    await appendReport('SET_UNIT', `By=@${username}, Item=${r[0]}, Unit=${unit}`);

    return sendMessage(
      chatId,
      `✅ Unit Updated\n\n💊 Item: ${r[0]}\n📦 New Unit: ${unit}`
    );
  }

  if (command === '/renameitem') {
    if (parts.length < 3) return sendMessage(chatId, '⚠️ Usage:\n/renameitem | Old Name | New Name');

    const oldName = clean(parts[1]);
    const newName = clean(parts[2]);

    if (!oldName || !newName) {
      return sendMessage(chatId, '⚠️ Old name and new name are required');
    }

    const oldRow = findRowIndex(data, oldName);
    if (oldRow === -1) return sendMessage(chatId, `❌ Item not found: ${oldName}`);

    const existingNew = findRowIndex(data, newName);
    if (existingNew !== -1) return sendMessage(chatId, `⚠️ Item already exists: ${newName}`);

    const r = data[oldRow];
    await updateRow(oldRow, [
      newName,
      toNumber(r[1]),
      toNumber(r[2]),
      toNumber(r[3]),
      toNumber(r[4]),
      clean(r[5] || ''),
      nowIso()
    ]);

    await appendReport('RENAME_ITEM', `By=@${username}, ${oldName} -> ${newName}`);
    return sendMessage(chatId, `✅ Item Renamed\n\n📝 Old: ${oldName}\n✨ New: ${newName}`);
  }

  if (command === '/deleteitem') {
    if (parts.length < 2) return sendMessage(chatId, '⚠️ Usage:\n/deleteitem | Item Name');

    const itemName = clean(parts[1]);
    const row = findRowIndex(data, itemName);
    if (row === -1) return sendMessage(chatId, `❌ Item not found: ${itemName}`);

    const pending = await createPendingAction(username, chatId, 'deleteitem', {
      itemName
    });

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
    const today = todayDateStringUTC();
    const todayLogs = logs.filter(r => String(r[0] || '').slice(0, 10) === today);

    if (todayLogs.length === 0) {
      return sendMessage(chatId, `📭 No transactions today (${today})`);
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
      `📅 Today Summary (${today})\n\n` +
      `📥 IN Transactions: ${inCount}\n` +
      `➕ Total IN Qty: ${inQty}\n\n` +
      `📤 OUT Transactions: ${outCount}\n` +
      `➖ Total OUT Qty: ${outQty}\n\n` +
      `🛠 Adjust Transactions: ${adjustCount}\n\n` +
      `🧾 Total Transactions: ${todayLogs.length}`;

    return sendMessage(chatId, msgOut);
  }

  if (command === '/report') {
    const summary = summarizeStock(data);

    let msgOut =
      `📊 Stock Report\n\n` +
      `📦 Total Items: ${summary.totalItems}\n` +
      `🚨 Low Stock Items: ${summary.lowStockCount}\n` +
      `🔢 Total Balance Qty: ${summary.totalBalance}\n\n`;

    const lowItems = data.filter(r => toNumber(r[3]) <= toNumber(r[4]));
    if (lowItems.length > 0) {
      msgOut += `🚨 Low Stock List:\n`;
      for (const r of lowItems) {
        msgOut += `💊 ${r[0]}: ${toNumber(r[3])} ${clean(r[5] || '')} (Min: ${toNumber(r[4])})\n`;
      }
    } else {
      msgOut += `✅ No low stock items`;
    }

    await appendReport(
      'REPORT_VIEW',
      `By=@${username}, TotalItems=${summary.totalItems}, LowStock=${summary.lowStockCount}, TotalBalance=${summary.totalBalance}`
    );

    return sendLongMessage(chatId, msgOut);
  }

  if (command === '/exportsummary') {
    if (data.length === 0) return sendMessage(chatId, '📭 No stock data to export');

    const rows = [['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt']];
    for (const r of data) {
      rows.push([
        clean(r[0] || ''),
        toNumber(r[1]),
        toNumber(r[2]),
        toNumber(r[3]),
        toNumber(r[4]),
        clean(r[5] || ''),
        clean(r[6] || '')
      ]);
    }

    const csv = rows.map(row => row.map(escapeCsv).join(',')).join('\n');
    await appendReport('EXPORT_SUMMARY', `By=@${username}, Exported ${data.length} items`);

    return sendDocument(chatId, `stock-summary-${todayDateStringUTC()}.csv`, csv);
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
    await appendReport('EXPORT_LOGS', `By=@${username}, Exported ${logs.length} logs`);

    return sendDocument(chatId, `stock-logs-${todayDateStringUTC()}.csv`, csv);
  }

  return sendMessage(chatId, '❌ Unknown command. Use /help');
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
app.listen(PORT, async () => {
  console.log('🚀 Server running on port ' + PORT);
  try {
    await setupSheet();
    console.log('✅ setupSheet done');
  } catch (err) {
    console.error('❌ Setup error:', err.response?.data || err.message || err);
  }
});
