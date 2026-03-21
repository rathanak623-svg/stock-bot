const express = require('express');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

/**************** CONFIG ****************/
if (!process.env.TELEGRAM_TOKEN) {
  throw new Error('Missing TELEGRAM_TOKEN');
}

if (!process.env.SPREADSHEET_ID) {
  throw new Error('Missing SPREADSHEET_ID');
}

if (!process.env.GOOGLE_CLIENT_EMAIL) {
  throw new Error('Missing GOOGLE_CLIENT_EMAIL');
}

if (!process.env.GOOGLE_PRIVATE_KEY) {
  throw new Error('Missing GOOGLE_PRIVATE_KEY');
}

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const PORT = process.env.PORT || 10000;

const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;

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

function escapeCsv(value) {
  const s = String(value ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
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
    if (norm(data[i][0]) === target) {
      return i;
    }
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
  Logs: ['Timestamp', 'Type', 'Item', 'Qty', 'BalanceAfter', 'Unit', 'ChatId', 'ChatTitle', 'Username'],
  Reports: ['Timestamp', 'Type', 'Details']
};

async function getSpreadsheetMeta() {
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID
  });
  return meta.data;
}

async function ensureSheetExists(title) {
  const meta = await getSpreadsheetMeta();
  const existing = (meta.sheets || []).find(s => s.properties.title === title);

  if (existing) {
    return existing.properties.sheetId;
  }

  const res = await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: { title }
          }
        }
      ]
    }
  });

  const addedSheet = res.data.replies?.[0]?.addSheet?.properties;
  console.log(`✅ Created sheet: ${title}`);
  return addedSheet?.sheetId;
}

async function ensureHeader(title, headers) {
  const range = `'${title}'!A1:${String.fromCharCode(64 + headers.length)}1`;

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
      requestBody: {
        values: [headers]
      }
    });
    console.log(`✅ Header created: ${title}`);
  } else {
    console.log(`ℹ️ Header already exists: ${title}`);
  }
}

async function setupSheet() {
  await ensureSheetExists('Stock');
  await ensureSheetExists('Logs');
  await ensureSheetExists('Reports');

  await ensureHeader('Stock', SHEET_HEADERS.Stock);
  await ensureHeader('Logs', SHEET_HEADERS.Logs);
  await ensureHeader('Reports', SHEET_HEADERS.Reports);
}

/**************** STOCK DATA ****************/
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
    range: "'Logs'!A2:I"
  });

  return res.data.values || [];
}

async function appendRow(values) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Stock'!A:G",
    valueInputOption: 'RAW',
    requestBody: {
      values: [values]
    }
  });
}

async function updateRow(rowIndex, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `'Stock'!A${rowIndex + 2}:G${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: {
      values: [values]
    }
  });
}

async function appendLog(values) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Logs'!A:I",
    valueInputOption: 'RAW',
    requestBody: {
      values: [values]
    }
  });
}

async function appendReport(type, details) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Reports'!A:C",
    valueInputOption: 'RAW',
    requestBody: {
      values: [[nowIso(), type, details]]
    }
  });
}

async function deleteRow(rowIndex) {
  const meta = await getSpreadsheetMeta();
  const stockSheet = (meta.sheets || []).find(s => s.properties.title === 'Stock');

  if (!stockSheet) {
    throw new Error('Stock sheet not found');
  }

  const sheetId = stockSheet.properties.sheetId;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          deleteDimension: {
            range: {
              sheetId,
              dimension: 'ROWS',
              startIndex: rowIndex + 1,
              endIndex: rowIndex + 2
            }
          }
        }
      ]
    }
  });
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
    if (row.balance <= row.minAlert) {
      lowStockCount += 1;
    }
  }

  return { totalItems, lowStockCount, totalBalance };
}

/**************** COMMAND HANDLER ****************/
async function handleCommand(msg) {
  const chatId = msg.chat.id;
  const text = msg.text;
  const meta = getChatMeta(msg);

  const parts = parsePipe(text);
  const command = norm(parts[0]);

  if (!command) {
    return sendMessage(chatId, '⚠️ Invalid command');
  }

  if (command === '/start' || command === '/help') {
    return sendMessage(
      chatId,
      '🤖 Stock Bot\n\n' +
      '📘 Commands:\n' +
      '/additem | Item Name | MinAlert | Unit\n' +
      '/in | Item Name | Qty\n' +
      '/out | Item Name | Qty\n' +
      '/stock | Item Name\n' +
      '/allstock\n' +
      '/lowstock\n' +
      '/setalert | Item Name | MinAlert\n' +
      '/setunit | Item Name | Unit\n' +
      '/renameitem | Old Name | New Name\n' +
      '/deleteitem | Item Name\n' +
      '/history | Item Name\n' +
      '/today\n' +
      '/report\n' +
      '/exportsummary'
    );
  }

  const data = await getData();

  if (command === '/additem') {
    if (parts.length < 4) {
      return sendMessage(chatId, '⚠️ Usage:\n/additem | Item Name | MinAlert | Unit');
    }

    const itemName = clean(parts[1]);
    const minAlert = Number(parts[2]);
    const unit = clean(parts[3]);

    if (!itemName) {
      return sendMessage(chatId, '⚠️ Item name required');
    }

    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }

    if (!unit) {
      return sendMessage(chatId, '⚠️ Unit required');
    }

    const existing = findRowIndex(data, itemName);
    if (existing !== -1) {
      return sendMessage(chatId, `⚠️ Item already exists: ${itemName}`);
    }

    const row = [
      itemName,
      0,
      0,
      0,
      minAlert,
      unit,
      nowIso()
    ];

    await appendRow(row);
    await appendReport('ADD_ITEM', `Added item: ${itemName}, MinAlert=${minAlert}, Unit=${unit}`);

    return sendMessage(
      chatId,
      `✅ Item Added\n\n` +
      `💊 Item: ${itemName}\n` +
      `⚠️ MinAlert: ${minAlert}\n` +
      `📦 Unit: ${unit}`
    );
  }

  if (command === '/in') {
    if (parts.length < 3) {
      return sendMessage(chatId, '⚠️ Usage:\n/in | Item Name | Qty');
    }

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!itemName) {
      return sendMessage(chatId, '⚠️ Item name required');
    }

    if (Number.isNaN(qty) || qty <= 0) {
      return sendMessage(chatId, '⚠️ Qty must be greater than 0');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');

    const newIn = currentIn + qty;
    const balance = newIn - currentOut;

    const updated = [
      r[0],
      newIn,
      currentOut,
      balance,
      minAlert,
      unit,
      nowIso()
    ];

    await updateRow(row, updated);

    await appendLog([
      nowIso(),
      'IN',
      r[0],
      qty,
      balance,
      unit,
      meta.chatId,
      meta.chatTitle,
      meta.username
    ]);

    await appendReport('IN', `Item=${r[0]}, Qty=${qty}, Balance=${balance} ${unit}`);

    return sendMessage(
      chatId,
      `📥 Stock Updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `➕ Qty In: ${qty}\n` +
      `📦 Balance: ${balance} ${unit}`
    );
  }

  if (command === '/out') {
    if (parts.length < 3) {
      return sendMessage(chatId, '⚠️ Usage:\n/out | Item Name | Qty');
    }

    const itemName = clean(parts[1]);
    const qty = Number(parts[2]);

    if (!itemName) {
      return sendMessage(chatId, '⚠️ Item name required');
    }

    if (Number.isNaN(qty) || qty <= 0) {
      return sendMessage(chatId, '⚠️ Qty must be greater than 0');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const minAlert = toNumber(r[4]);
    const unit = clean(r[5] || '');
    const balance = currentIn - currentOut;

    if (qty > balance) {
      return sendMessage(chatId, `❌ Not enough stock\n📦 Balance: ${balance} ${unit}`);
    }

    const newOut = currentOut + qty;
    const newBalance = currentIn - newOut;

    const updated = [
      r[0],
      currentIn,
      newOut,
      newBalance,
      minAlert,
      unit,
      nowIso()
    ];

    await updateRow(row, updated);

    await appendLog([
      nowIso(),
      'OUT',
      r[0],
      qty,
      newBalance,
      unit,
      meta.chatId,
      meta.chatTitle,
      meta.username
    ]);

    await appendReport('OUT', `Item=${r[0]}, Qty=${qty}, Balance=${newBalance} ${unit}`);

    await sendMessage(
      chatId,
      `📤 Stock Updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `➖ Qty Out: ${qty}\n` +
      `📦 Balance: ${newBalance} ${unit}`
    );

    await checkAndSendLowStockAlert(chatId, updated);
    return;
  }

  if (command === '/stock') {
    if (parts.length < 2) {
      return sendMessage(chatId, '⚠️ Usage:\n/stock | Item Name');
    }

    const itemName = clean(parts[1]);
    const row = findRowIndex(data, itemName);

    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

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
    if (data.length === 0) {
      return sendMessage(chatId, '📭 No stock data');
    }

    let msg = '📋 All Stock\n\n';

    for (const r of data) {
      const item = clean(r[0] || '');
      const balance = toNumber(r[3]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const status = balance <= minAlert ? ' 🚨LOW' : ' ✅OK';

      msg += `💊 ${item}: ${balance} ${unit}${status}\n`;
    }

    return sendLongMessage(chatId, msg);
  }

  if (command === '/lowstock') {
    if (data.length === 0) {
      return sendMessage(chatId, '📭 No stock data');
    }

    const lowItems = data.filter(r => {
      const balance = toNumber(r[3]);
      const minAlert = toNumber(r[4]);
      return balance <= minAlert;
    });

    if (lowItems.length === 0) {
      return sendMessage(chatId, '✅ No low stock items');
    }

    let msg = '🚨 Low Stock Items\n\n';

    for (const r of lowItems) {
      const item = clean(r[0] || '');
      const balance = toNumber(r[3]);
      const minAlert = toNumber(r[4]);
      const unit = clean(r[5] || '');
      msg += `💊 ${item}: ${balance} ${unit} (Min: ${minAlert})\n`;
    }

    return sendLongMessage(chatId, msg);
  }

  if (command === '/setalert') {
    if (parts.length < 3) {
      return sendMessage(chatId, '⚠️ Usage:\n/setalert | Item Name | MinAlert');
    }

    const itemName = clean(parts[1]);
    const minAlert = Number(parts[2]);

    if (!itemName) {
      return sendMessage(chatId, '⚠️ Item name required');
    }

    if (Number.isNaN(minAlert) || minAlert < 0) {
      return sendMessage(chatId, '⚠️ MinAlert must be a valid number');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const balance = toNumber(r[3]);
    const unit = clean(r[5] || '');

    await updateRow(row, [
      r[0],
      currentIn,
      currentOut,
      balance,
      minAlert,
      unit,
      nowIso()
    ]);

    await appendReport('SET_ALERT', `Item=${r[0]}, MinAlert=${minAlert}`);

    return sendMessage(
      chatId,
      `✅ MinAlert Updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `⚠️ New MinAlert: ${minAlert}`
    );
  }

  if (command === '/setunit') {
    if (parts.length < 3) {
      return sendMessage(chatId, '⚠️ Usage:\n/setunit | Item Name | Unit');
    }

    const itemName = clean(parts[1]);
    const unit = clean(parts[2]);

    if (!itemName) {
      return sendMessage(chatId, '⚠️ Item name required');
    }

    if (!unit) {
      return sendMessage(chatId, '⚠️ Unit required');
    }

    const row = findRowIndex(data, itemName);
    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

    const r = data[row];
    const currentIn = toNumber(r[1]);
    const currentOut = toNumber(r[2]);
    const balance = toNumber(r[3]);
    const minAlert = toNumber(r[4]);

    await updateRow(row, [
      r[0],
      currentIn,
      currentOut,
      balance,
      minAlert,
      unit,
      nowIso()
    ]);

    await appendReport('SET_UNIT', `Item=${r[0]}, Unit=${unit}`);

    return sendMessage(
      chatId,
      `✅ Unit Updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `📦 New Unit: ${unit}`
    );
  }

  if (command === '/renameitem') {
    if (parts.length < 3) {
      return sendMessage(chatId, '⚠️ Usage:\n/renameitem | Old Name | New Name');
    }

    const oldName = clean(parts[1]);
    const newName = clean(parts[2]);

    if (!oldName || !newName) {
      return sendMessage(chatId, '⚠️ Old name and new name are required');
    }

    const oldRow = findRowIndex(data, oldName);
    if (oldRow === -1) {
      return sendMessage(chatId, `❌ Item not found: ${oldName}`);
    }

    const existingNew = findRowIndex(data, newName);
    if (existingNew !== -1) {
      return sendMessage(chatId, `⚠️ Item already exists: ${newName}`);
    }

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

    await appendReport('RENAME_ITEM', `${oldName} -> ${newName}`);

    return sendMessage(
      chatId,
      `✅ Item Renamed\n\n` +
      `📝 Old: ${oldName}\n` +
      `✨ New: ${newName}`
    );
  }

  if (command === '/deleteitem') {
    if (parts.length < 2) {
      return sendMessage(chatId, '⚠️ Usage:\n/deleteitem | Item Name');
    }

    const itemName = clean(parts[1]);
    const row = findRowIndex(data, itemName);

    if (row === -1) {
      return sendMessage(chatId, `❌ Item not found: ${itemName}`);
    }

    await deleteRow(row);
    await appendReport('DELETE_ITEM', `Deleted item: ${itemName}`);

    return sendMessage(chatId, `🗑️ Deleted Item: ${itemName}`);
  }

  if (command === '/history') {
    if (parts.length < 2) {
      return sendMessage(chatId, '⚠️ Usage:\n/history | Item Name');
    }

    const itemName = clean(parts[1]);
    const logs = await getLogs();

    const filtered = logs.filter(r => norm(r[2]) === norm(itemName));

    if (filtered.length === 0) {
      return sendMessage(chatId, `📭 No history found for: ${itemName}`);
    }

    const recent = filtered.slice(-20);
    let msg = `📜 History: ${itemName}\n\n`;

    for (const r of recent) {
      const ts = clean(r[0] || '');
      const type = clean(r[1] || '');
      const qty = toNumber(r[3]);
      const bal = toNumber(r[4]);
      const unit = clean(r[5] || '');
      const user = clean(r[8] || '');
      const emoji = type === 'IN' ? '📥' : '📤';

      msg += `${emoji} ${type} | Qty: ${qty} | Balance: ${bal} ${unit}\n`;
      msg += `🕒 ${ts}`;
      if (user) msg += ` | 👤 ${user}`;
      msg += '\n\n';
    }

    return sendLongMessage(chatId, msg.trim());
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
      }
    }

    let msg =
      `📅 Today Summary (${today})\n\n` +
      `📥 IN Transactions: ${inCount}\n` +
      `➕ Total IN Qty: ${inQty}\n\n` +
      `📤 OUT Transactions: ${outCount}\n` +
      `➖ Total OUT Qty: ${outQty}\n\n` +
      `🧾 Total Transactions: ${todayLogs.length}`;

    return sendMessage(chatId, msg);
  }

  if (command === '/report') {
    const summary = summarizeStock(data);

    let msg =
      `📊 Stock Report\n\n` +
      `📦 Total Items: ${summary.totalItems}\n` +
      `🚨 Low Stock Items: ${summary.lowStockCount}\n` +
      `🔢 Total Balance Qty: ${summary.totalBalance}\n\n`;

    const lowItems = data.filter(r => toNumber(r[3]) <= toNumber(r[4]));

    if (lowItems.length > 0) {
      msg += `🚨 Low Stock List:\n`;
      for (const r of lowItems) {
        msg += `💊 ${r[0]}: ${toNumber(r[3])} ${clean(r[5] || '')} (Min: ${toNumber(r[4])})\n`;
      }
    } else {
      msg += `✅ No low stock items`;
    }

    await appendReport(
      'REPORT_VIEW',
      `TotalItems=${summary.totalItems}, LowStock=${summary.lowStockCount}, TotalBalance=${summary.totalBalance}`
    );

    return sendLongMessage(chatId, msg);
  }

  if (command === '/exportsummary') {
    if (data.length === 0) {
      return sendMessage(chatId, '📭 No stock data to export');
    }

    const rows = [
      ['Item', 'In', 'Out', 'Balance', 'MinAlert', 'Unit', 'UpdatedAt']
    ];

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

    const csv = rows
      .map(row => row.map(escapeCsv).join(','))
      .join('\n');

    await appendReport('EXPORT_SUMMARY', `Exported ${data.length} items`);
    return sendDocument(chatId, `stock-summary-${todayDateStringUTC()}.csv`, csv);
  }

  return sendMessage(chatId, '❌ Unknown command. Use /help');
}

/**************** WEBHOOK ****************/
app.post('/webhook', async (req, res) => {
  try {
    const msg = req.body.message;

    if (!msg || !msg.text) {
      return res.sendStatus(200);
    }

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
