# DerivBot Pro — SMC Alert Engine

## Deploy on Render.com (Free)

### Step 1 — GitHub Setup
1. Go to github.com → Sign up free
2. Click "New repository" → Name: "derivbot-pro"
3. Upload all files from this folder

### Step 2 — Render Setup
1. Go to render.com → Sign up free
2. Click "New" → "Background Worker"
3. Connect your GitHub account
4. Select your "derivbot-pro" repo
5. Render auto-detects settings from render.yaml

### Step 3 — Add Tokens
In Render dashboard → Environment:
- TELEGRAM_TOKEN = your BotFather token
- DERIV_DEMO_TOKEN = your Deriv demo token
- DERIV_REAL_TOKEN = your Deriv real token

### Step 4 — Deploy
Click "Deploy" — bot starts automatically!

## Files
- bot.py — Main bot (Phase 2 + 3)
- requirements.txt — Python libraries
- Procfile — Render process config
- render.yaml — Auto-configuration

## Features Phase 2+3
✅ Order Blocks & Breaker Blocks
✅ FVG & IFVG Detection
✅ BOS & CHoC with body closure
✅ EQH/EQL Sweeps
✅ Inducement Detection
✅ Premium/Discount + OTE zones
✅ 21 EMA | 20 SMA | 200 SMA
✅ Entry/SL/TP in every alert
✅ LTF Confirmation guidance
✅ Session filter (London/NY/Asian)
✅ Signal scoring 7+/10

## Add Instruments in Telegram
Send: SYMBOL NAME
Example: frxGBPUSD GBPUSD
