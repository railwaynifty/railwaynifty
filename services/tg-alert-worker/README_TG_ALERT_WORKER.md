# NSE360 Telegram COI/Volume alert worker

This Railway worker reads the existing Upstox-normalised option data from
`options."NIFTY"` and sends the same style of current/next-expiry COI/Volume
alerts as the local script.

## Important security step

The old Telegram bot token was embedded in the uploaded local Python file.
Revoke/regenerate it in BotFather and use the fresh token only as a Railway
service variable. Never commit it to GitHub.

## Railway service

Create a fourth service from the same GitHub repository.

- Service name: `nse360-tg-alert-worker`
- Root Directory: `/services/tg-alert-worker`
- Config-as-code: `/services/tg-alert-worker/railway.json`
- No public domain
- No cron schedule

## Required Railway variables

```env
DATABASE_URL=${{Postgres.DATABASE_URL}}
SCHEMA_OPTIONS=options
TELEGRAM_BOT_TOKEN=PASTE_FRESH_BOT_TOKEN
TELEGRAM_CHAT_ID=PASTE_CHAT_OR_CHANNEL_ID
```

## Default alert behaviour

```env
TG_ALERT_SYMBOL=NIFTY
TG_ALERT_THRESHOLD=0.25
TG_ALERT_POLL_SECONDS=60
TG_ALERT_REPEAT_MINUTES=15
TG_ALERT_REALERT_DELTA=0.05
TG_ALERT_MIN_VOLUME=1000
TG_ALERT_MIN_ABS_COI=100
TG_ALERT_ATM_STEPS=10
TG_ALERT_INCLUDE_NEGATIVE=0
TG_ALERT_MARKET_START=09:14
TG_ALERT_MARKET_END=15:50
TG_ALERT_RUN_OUTSIDE_MARKET=0
TG_ALERT_TELEGRAM_DELAY=1.0
TG_ALERT_TELEGRAM_MAX_RETRIES=8
```

The state is stored in PostgreSQL table
`public.tg_coi_volume_alert_state`, so service restarts and redeployments do not
lose the cooldown/deduplication state.

## Telegram connection test

Temporarily add:

```env
TG_ALERT_SEND_STARTUP_MESSAGE=1
```

Redeploy and confirm the test message arrives. Then set it back to `0` to avoid
sending a message on every future restart.

## Volume mapping

The worker automatically uses the first available value from:

1. `tradedVolume`
2. `volume`
3. `tradedContracts`
4. `noOfTrades`

This keeps the alert calculation compatible with the Upstox adapter and older
NSE-style data.
