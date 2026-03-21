const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

/**************** CONFIG ****************/
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN || 'PUT_YOUR_TELEGRAM_TOKEN';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || 'PUT_YOUR_SPREADSHEET_ID';

const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;

/**************** ENV CHECK ****************/
if (!process.env.GOOGLE_CLIENT_EMAIL) {
  throw new Error('Missing GOOGLE_CLIENT_EMAIL');
}

if (!process.env.GOOGLE_PRIVATE_KEY) {
  throw new Error('Missing GOOGLE_PRIVATE_KEY');
}

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

async function sendMessage(chatId, text) {
  await axios.post(`${TELEGRAM_API}/sendMessage`, {
    chat_id: chatId,
    text: text
  });
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

/**************** SHEET SETUP ****************/
async function setupSheet() {
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID
  });

  const sheetTitles = (meta.data.sheets || []).map(s => s.properties.title);

  if (!sheetTitles.includes('Stock')) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [
          {
            addSheet: {
              properties: {
                title: 'Stock'
              }
            }
          }
        ]
      }
    });

    console.log('✅ Created Stock sheet');
  }

  const headerRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Stock'!A1:G1"
  });

  const header = headerRes.data.values || [];

  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: "'Stock'!A1:G1",
      valueInputOption: 'RAW',
      requestBody: {
        values: [[
          'Item',
          'In',
          'Out',
          'Balance',
          'MinAlert',
          'Unit',
          'UpdatedAt'
        ]]
      }
    });

    console.log('✅ Header created');
  } else {
    console.log('ℹ️ Header already exists');
  }
}

/**************** STOCK DATA ****************/
async function getData() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: "'Stock'!A2:G"
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

/**************** COMMAND HANDLER ****************/
async function handleCommand(chatId, text) {
  const parts = parsePipe(text);
  const command = norm(parts[0]);

  if (!command) {
    return sendMessage(chatId, '⚠️ Invalid command');
  }

  if (command === '/start' || command === '/help') {
    return sendMessage(
      chatId,
      '🤖 Stock Bot\n\n' +
      'Commands:\n' +
      '/additem | Item Name | MinAlert | Unit\n' +
      '/in | Item Name | Qty\n' +
      '/out | Item Name | Qty\n' +
      '/stock | Item Name\n' +
      '/allstock'
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

    await appendRow([
      itemName,
      0,
      0,
      0,
      minAlert,
      unit,
      new Date().toISOString()
    ]);

    return sendMessage(chatId, `✅ Added: ${itemName}`);
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
    const currentIn = Number(r[1] || 0);
    const currentOut = Number(r[2] || 0);
    const minAlert = Number(r[4] || 0);
    const unit = r[5] || '';

    const newIn = currentIn + qty;
    const balance = newIn - currentOut;

    await updateRow(row, [
      r[0],
      newIn,
      currentOut,
      balance,
      minAlert,
      unit,
      new Date().toISOString()
    ]);

    return sendMessage(
      chatId,
      `📥 Stock updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `➕ Qty: ${qty}\n` +
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
    const currentIn = Number(r[1] || 0);
    const currentOut = Number(r[2] || 0);
    const minAlert = Number(r[4] || 0);
    const unit = r[5] || '';
    const balance = currentIn - currentOut;

    if (qty > balance) {
      return sendMessage(chatId, `❌ Not enough stock\n📦 Balance: ${balance} ${unit}`);
    }

    const newOut = currentOut + qty;
    const newBalance = currentIn - newOut;

    await updateRow(row, [
      r[0],
      currentIn,
      newOut,
      newBalance,
      minAlert,
      unit,
      new Date().toISOString()
    ]);

    return sendMessage(
      chatId,
      `📤 Stock updated\n\n` +
      `💊 Item: ${r[0]}\n` +
      `➖ Qty: ${qty}\n` +
      `📦 Balance: ${newBalance} ${unit}`
    );
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
    const currentIn = Number(r[1] || 0);
    const currentOut = Number(r[2] || 0);
    const balance = Number(r[3] || 0);
    const minAlert = Number(r[4] || 0);
    const unit = r[5] || '';

    return sendMessage(
      chatId,
      `📊 Stock Info\n\n` +
      `💊 Item: ${r[0]}\n` +
      `📥 In: ${currentIn}\n` +
      `📤 Out: ${currentOut}\n` +
      `📦 Balance: ${balance} ${unit}\n` +
      `⚠️ MinAlert: ${minAlert}`
    );
  }

  if (command === '/allstock') {
    if (data.length === 0) {
      return sendMessage(chatId, '📭 No stock data');
    }

    let msg = '📋 All Stock\n\n';

    for (const r of data) {
      const item = r[0] || '';
      const balance = Number(r[3] || 0);
      const minAlert = Number(r[4] || 0);
      const unit = r[5] || '';
      const status = balance <= minAlert ? ' 🚨LOW' : ' ✅';

      msg += `💊 ${item}: ${balance} ${unit}${status}\n`;
    }

    return sendMessage(chatId, msg);
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

    const chatId = msg.chat.id;
    const text = msg.text;

    await handleCommand(chatId, text);
    return res.sendStatus(200);
  } catch (err) {
    console.error('Webhook error:', err.message);
    return res.sendStatus(200);
  }
});

app.get('/', (req, res) => {
  res.status(200).send('Bot running');
});

/**************** START SERVER ****************/
const PORT = process.env.PORT || 10000;

app.listen(PORT, async () => {
  console.log('Server running on port ' + PORT);

  try {
    await setupSheet();
    console.log('✅ setupSheet done');
  } catch (err) {
    console.error('❌ Setup error:', err.message);
  }
});
