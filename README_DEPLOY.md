# Telegram Stock Bot (3 Departments, Full Feature)

This package supports:
- 1 bot
- 1 Render service
- 3 Telegram groups
- 3 departments:
  - medicine
  - supplies
  - inventory

## Full features included
- `/menu` shows menu + quick actions
- `/allstock` now supports `low`, `ok`, and `detail` modes
- `/restocklist` shows items that need purchasing
- quick actions follow roles
- ADDITEM / IN / OUT / STOCK support step-by-step input
- bulk quick actions show prompt format
- legacy commands still work:
  - `/additem | ...`
  - `/in | ...`
  - `/out | ...`
- `/cancelinput` supported
- group -> department routing handled automatically

## Required Google Sheets tabs
### Stock sheets
- `Stock_Medicine`
- `Stock_Supplies`
- `Stock_Inventory`

Header:
`Item | In | Out | Balance | MinAlert | Unit | UpdatedAt`

### Log sheets
- `Logs_Medicine`
- `Logs_Supplies`
- `Logs_Inventory`

Header:
`Timestamp | Type | Item | Qty | BalanceBefore | BalanceAfter | Unit | ChatId | ChatTitle | Username | Role | Note`

### Other sheets
- `Reports`
- `Roles`
- `AllowedChats`
- `PendingActions`
- `ProcessedMessages`
- `GroupSettings`

The bot auto-creates missing sheets and headers on startup.

## AllowedChats format
Use this header:
`ChatId | ChatTitle | ChatType | Department | AddedAt`

Departments allowed:
- `medicine`
- `supplies`
- `inventory`

## Allow groups
Run these inside each target Telegram group:

Medicine group:
`/allowgroup | medicine`

Supplies group:
`/allowgroup | supplies`

Inventory group:
`/allowgroup | inventory`

## Install
```bash
npm install
```

## Run
```bash
npm start
```

## New stock commands
- `/allstock`
- `/allstock | low`
- `/allstock | ok`
- `/allstock | detail`
- `/restocklist`

## Render deploy
1. Upload these files to GitHub
2. Create a Render Web Service
3. Use:
   - Build Command: `npm install`
   - Start Command: `npm start`
4. Add env vars from `.env.example`
5. Keep the service at **1 instance only**
6. Deploy
7. Check health:
   - `https://YOUR-APP.onrender.com/healthz`
8. Set webhook to:
   `https://YOUR-APP.onrender.com/webhook`

## Telegram webhook example
```bash
curl -X POST "https://api.telegram.org/bot<TELEGRAM_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://YOUR-APP.onrender.com/webhook",
    "secret_token": "YOUR_TELEGRAM_WEBHOOK_SECRET"
  }'
```

## Notes
- This build is designed for a single customer using 3 operational groups/departments.
- Stock and logs are separated by department, but the bot/service stays single-instance.
- Root path `/` and `/healthz` can be used for Render health/debug.
- For Render, store `GOOGLE_PRIVATE_KEY` in one line with `\n` characters.
