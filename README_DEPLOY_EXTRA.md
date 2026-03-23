# Quick Deploy Notes

## Render
1. Create a new Web Service
2. Upload this project or connect your Git repo
3. Render will read `render.yaml`
4. Fill all secret env vars in Render dashboard
5. Deploy
6. After deploy, set Telegram webhook to:
   `https://YOUR-RENDER-URL.onrender.com/webhook`

## Railway
1. Create a new project
2. Upload this project or connect your Git repo
3. Railway will use `npm start`
4. Add all env vars in Railway Variables
5. Deploy
6. Set Telegram webhook to:
   `https://YOUR-RAILWAY-URL.up.railway.app/webhook`

## Local run
```bash
npm install
npm start
```
