# Stock Bot Deploy Guide

## Files in this package
- `updated_stock_bot.js` — main bot server
- `.env.example` — environment variable template

## Requirements
- Node.js 18+ recommended
- A Telegram bot token from BotFather
- A Google service account with access to Google Sheets API
- A Google Spreadsheet shared with that service account
- A public HTTPS URL for Telegram webhook

## 1) Install dependencies
```bash
npm install express axios form-data googleapis
```

## 2) Prepare environment
Copy `.env.example` to `.env` and fill in the real values.

Important fields:
- `TELEGRAM_TOKEN` — your bot token
- `TELEGRAM_WEBHOOK_SECRET` — a strong random secret string
- `SPREADSHEET_ID` — target Google Sheet ID
- `GOOGLE_CLIENT_EMAIL` — service account email
- `GOOGLE_PRIVATE_KEY` — private key from service account JSON

For `GOOGLE_PRIVATE_KEY`, keep the `\n` characters exactly as shown in `.env.example`.

## 3) Google setup
Enable:
- Google Sheets API

Then:
- create a service account
- download the JSON key
- copy `client_email` and `private_key` into `.env`
- share the spreadsheet with the service account email

## 4) Start the bot
```bash
node updated_stock_bot.js
```

If startup succeeds, you should see logs like:
- `setupSheet done`
- `Server running on port ...`

## 5) Set Telegram webhook
Replace the values below with your real bot token and public URL.

```bash
curl -X POST "https://api.telegram.org/bot<TELEGRAM_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://your-domain.com/webhook",
    "secret_token": "your_random_webhook_secret"
  }'
```

Your server verifies this header:
- `x-telegram-bot-api-secret-token`

So `secret_token` must match `TELEGRAM_WEBHOOK_SECRET`.

## 6) Health checks
Useful endpoints:
- `GET /` → bot running check
- `GET /webhook` → webhook endpoint check

## 7) Deploy tips
### Render / Railway / VPS
Make sure:
- environment variables are added
- port is exposed from `PORT`
- app is served over HTTPS
- the bot process stays running

### PM2 example
```bash
npm install -g pm2
pm2 start updated_stock_bot.js --name stock-bot
pm2 save
```

## 8) First-time bot setup
After deploy:
1. Add your Telegram username into `SUPER_ADMINS`
2. Start the bot
3. In Telegram, send `/start`
4. In your target group, run `/allowgroup`
5. Test `/additem`, `/in`, `/out`, `/dashboard`

## 9) Common problems
### `Missing TELEGRAM_WEBHOOK_SECRET`
Add `TELEGRAM_WEBHOOK_SECRET` to `.env`.

### `Missing GOOGLE_PRIVATE_KEY`
Copy the private key from the service account JSON.

### Telegram webhook works but commands fail
Check:
- spreadsheet is shared with service account
- Sheets API is enabled
- `SPREADSHEET_ID` is correct

### Group says not allowed
Run `/allowgroup` inside that group using a super admin account.

### Duplicate or retry concerns
This updated version already includes improved:
- webhook verification
- processed message handling
- pending action locking
- daily report recovery logic

## 10) Suggested production extras
- add `dotenv` if you want local `.env` loading
- add structured logs
- add process manager restart policy
- back up the spreadsheet periodically
