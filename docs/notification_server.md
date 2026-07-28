# Notification Server

`notification_server` is a standalone local notification daemon. Producers submit structured
events over HTTP; the daemon owns Telegram credentials, message formatting, retries, and
delivery metrics.

## Telegram Setup

1. Open the official `@BotFather` account, run `/newbot`, and save the bot token.
2. Open a private chat with the new bot and send `/start`. Telegram bots cannot initiate a
   private conversation.
3. Call `getUpdates` and read `result[].message.chat.id` to obtain the numeric chat ID:

```bash
TELEGRAM_BOT_TOKEN='<token>'
curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates"
```

For a group, add the bot to the group, send it a command, and use the negative
`message.chat.id` returned by `getUpdates`. For a public channel,
`TELEGRAM_CHAT_ID=@channelusername` is also accepted, provided the bot can post there.

Keep the token out of Git and shared shell history. Store it only in the deployed
`config/notification_server.env` file.

## API

`POST /v1/notify` returns `202 Accepted` after placing a valid event in the bounded queue.

```json
{
  "source": "fr_pre_trade",
  "severity": "warning",
  "title": "FR仓位风控",
  "message": "gate-fr-arb01｜告警1\nBTCUSDT 11.20% 告警",
  "dedup_key": "gate-fr-arb01:fr_position_concentration:summary"
}
```

`severity` accepts `info`, `warning`, or `critical`. `fields` and `dedup_key` are optional.
`dedup_key` is currently included as correlation metadata; it does not suppress duplicate events.
If `NOTIFICATION_API_TOKEN` is configured, send either `Authorization: Bearer <token>` or
`X-Notification-Token: <token>`.

`GET /healthz` returns queue capacity and cumulative delivery counters. A `202` response means
the event was accepted locally; final Telegram delivery is reflected in logs and health counters.
Messages are sent as plain text and must fit Telegram's 4096-character `sendMessage` limit.
Telegram renders only `title`, `message`, and optional `fields`; `source`, `event_id`,
`accepted_at`, and `dedup_key` remain internal metadata and logs.

## Pre-trade Producer

FR pre-trade submits concentration events synchronously during its existing 60-second
parameter refresh round. The round drops queued open signals before Redis and HTTP work,
then drops open signals that accumulated during the round; close signals are preserved.
A successful request means the local daemon returned `202 Accepted`. Telegram delivery and
retry remain asynchronous inside `notification_server`.

The producer requires `PRE_TRADE_NOTIFICATION_URL`; there is no compiled-in endpoint. The socket
timeout still defaults to 250 ms. Configure the pre-trade environment with:

```bash
PRE_TRADE_NOTIFICATION_URL=http://127.0.0.1:18100/v1/notify  # required
PRE_TRADE_NOTIFICATION_TIMEOUT_MS=250                         # optional
NOTIFICATION_API_TOKEN=                                       # optional
```

The URL must resolve only to loopback addresses. If API authentication is enabled on the
daemon, configure the same `NOTIFICATION_API_TOKEN` in the pre-trade environment. A local
notification failure is logged but never rolls back or interrupts the position lock or dump
update.

Each 60-second FR scan produces at most one aggregate notification. Crossing 12% emits one
warning and enables the reduce-only ArbOpen lock; subsequent 12%-15% rounds remain silent.
Reaching 15% adds the position dump and starts continuous-close notifications. Notifications are
sent immediately when the symbol set or semantic status changes; percentage-only changes for the
same state are throttled to once every five minutes. That continuous state is retained while the
ratio remains at or above 12%. Falling below 12% removes the position dump, emits one recovery,
and returns to silence. Severity is critical for continuous close or Redis sync failure, warning
for the initial 12% edge, and info for recovery. The Telegram body is intentionally concise:

```text
FR仓位风控
gate-fr-arb01｜告警1｜强平1｜恢复1
BTCUSDT 12.20% 告警
ETHUSDT 15.10% 强平中
XRPUSDT 11.90% 恢复
```

A Redis synchronization error replaces the affected symbol state with `强平写入失败` or
`强平移除失败`.

FR and intra pre-trade also maintain a per-account UniMMR open lock. Below the configured
`unimmr_trigger_line`, only position-reducing `ArbOpen` signals are accepted. The lock recovers
only above `unimmr_recover_line`. Active risk is repeated in each 60-second maintenance round;
recovery is sent once. On full UniMMR recovery, pre-trade also submits a special cancel for
active `ArbCloseStrategy` orders whose symbols are only in `unimmr_close_symbols`. Membership
in `dump_symbols` or `pos_dump_symbols` always wins and prevents this cancel:

```text
UniMMR风控
binance-fr-arb01｜风险1
Binance统一账户 1.87<2.00 只减仓
```

```text
UniMMR风控
binance-fr-arb01｜恢复1
Binance统一账户 2.23>2.20 恢复
```

## PM2 Deployment

```bash
scripts/deploy_notification_server.sh
cd ~/notification_server
$EDITOR config/notification_server.env
./scripts/start_notification_server.sh
npx pm2 logs --namespace notification_server notification_server
```

The deploy script creates `config/notification_server.env` only when it is missing and never
overwrites an existing credential file.

## Local Dry Run

```bash
cargo build -p notification_server
NOTIFICATION_DRY_RUN=1 NOTIFICATION_PORT=18100 cargo run -p notification_server
curl -sS -X POST http://127.0.0.1:18100/v1/notify \
  -H 'Content-Type: application/json' \
  -d '{"source":"manual","severity":"info","title":"test","message":"hello"}'
curl -sS http://127.0.0.1:18100/healthz
```

Provider credentials are never accepted through the local API and must remain in the deployed
environment file.
