const express = require('express');
const axios = require('axios');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = '8309116881:AAGgmKDI_OTf5Cdzm1Gf_mhrhkqqEcsG7qA';
const SPREADSHEET_ID = '1wTsR0u2pJIoYSz9PpDwBGMxl177TLm4q7Cgu2uknXSg';

const TELEGRAM_API = `https://api.telegram.org/bot${'8309116881:AAGgmKDI_OTf5Cdzm1Gf_mhrhkqqEcsG7qA'}`;

const auth = new google.auth.GoogleAuth({
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

app.listen(process.env.PORT || 10000);