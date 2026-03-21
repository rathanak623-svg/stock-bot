const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = '8309116881:AAGgmKDI_OTf5Cdzm1Gf_mhrhkqqEcsG7qA';
const SPREADSHEET_ID = '1wTsR0u2pJIoYSz9PpDwBGMxl177TLm4q7Cgu2uknXSg';

const TELEGRAM_API = `https://api.telegram.org/bot${'8309116881:AAGgmKDI_OTf5Cdzm1Gf_mhrhkqqEcsG7qA'}`;

if (!process.env.GOOGLE_CLIENT_EMAIL) {
  throw new Error('Missing GOOGLE_CLIENT_EMAIL');
}

if (!process.env.GOOGLE_PRIVATE_KEY) {
  throw new Error('Missing GOOGLE_PRIVATE_KEY');
}

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_CLIENT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const sheets = google.sheets({ version: 'v4', auth });

async function send(chatId, text) {
  await axios.post(`${TELEGRAM_API}/sendMessage`, {
    chat_id: chatId,
    text: text
  });
}

function norm(t) {
  return (t || '').toLowerCase().trim();
}

async function getData() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'Stock!A2:G'
  });
  return res.data.values || [];
}

function find(data, name) {
  return data.findIndex(r => norm(r[0]) === norm(name));
}

async function update(row, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `Stock!A${row + 2}:G${row + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [values] }
  });
}

async function handle(chatId, text) {
  const p = text.split('|').map(x => x.trim());
  const cmd = p[0].toLowerCase();
  const data = await getData();

  if (cmd === '/help') {
    return send(chatId, '/additem | name | min | unit\n/in | name | qty\n/out | name | qty\n/stock | name');
  }

  if (cmd === '/additem') {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Stock!A:G',
      valueInputOption: 'RAW',
      requestBody: { values: [[p[1],0,0,0,p[2],p[3],new Date()]] }
    });
    return send(chatId, '✅ Added');
  }

  if (cmd === '/in') {
    const i = find(data, p[1]);
    if (i === -1) return send(chatId, '❌ Not found');

    const r = data[i];
    const ni = (+r[1]||0) + (+p[2]);
    const out = +r[2]||0;

    await update(i, [r[0], ni, out, ni-out, r[4], r[5], new Date()]);
    return send(chatId, '📥 Updated');
  }

  if (cmd === '/out') {
    const i = find(data, p[1]);
    if (i === -1) return send(chatId, '❌ Not found');

    const r = data[i];
    const ni = +r[1]||0;
    const no = (+r[2]||0) + (+p[2]);

    if (ni-no < 0) return send(chatId, '❌ Not enough');

    await update(i, [r[0], ni, no, ni-no, r[4], r[5], new Date()]);
    return send(chatId, '📤 Updated');
  }

  if (cmd === '/stock') {
    const i = find(data, p[1]);
    if (i === -1) return send(chatId, '❌ Not found');

    const r = data[i];
    return send(chatId, `💊 ${r[0]}\n📦 ${r[3]} ${r[5]}`);
  }

  return send(chatId, '❌ Unknown');
}

app.post('/webhook', async (req, res) => {
  const msg = req.body.message;
  if (msg && msg.text) {
    await handle(msg.chat.id, msg.text);
  }
  res.sendStatus(200);
});

app.get('/', (req, res) => {
  res.send('Bot running');
});

const PORT = process.env.PORT || 10000;

app.listen(PORT, async () => {
  console.log('Server running on port ' + PORT);

  try {
    await setupSheet();
  } catch (err) {
    console.error('Setup error:', err.message);
  }
});
async function setupSheet() {
  const spreadsheetId = SPREADSHEET_ID;

  // 1. get sheet metadata
  const meta = await sheets.spreadsheets.get({
    spreadsheetId
  });

  const sheetsList = meta.data.sheets.map(s => s.properties.title);

  // 2. check if "Stock" exists
  if (!sheetsList.includes('Stock')) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
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

  // 3. check header
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: "'Stock'!A1:G1"
  });

  const header = res.data.values;

  if (!header || header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
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
if (cmd === '/setup') {
  await setupSheet();
  return send(chatId, '✅ Sheet ready');
}
