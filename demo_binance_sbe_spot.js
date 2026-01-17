const WebSocket = require('ws');

const apiKey = process.env.BINANCE_API_KEY;
if (!apiKey) {
  console.error('Missing BINANCE_API_KEY');
  process.exit(1);
}

const arg = process.argv[2];
const channel = process.argv[3] || process.env.BINANCE_SBE_CHANNEL || 'depth20';
const stream =
  process.env.BINANCE_SBE_STREAM ||
  (arg && arg.includes('@') ? arg.toLowerCase() : `${(arg || 'btcusdt').toLowerCase()}@${channel}`);

const url = `wss://stream-sbe.binance.com:9443/ws/${stream}`;
const ws = new WebSocket(url, {
  headers: { 'X-MBX-APIKEY': apiKey },
});

ws.on('open', () => {
  console.log(`[open] ${url}`);
});

ws.on('ping', (data) => {
  ws.pong(data);
  console.log(`[ping] len=${data.length}`);
});

ws.on('message', (data, isBinary) => {
  if (!isBinary) {
    console.log(`[text] ${data.toString()}`);
    return;
  }
  const buf = Buffer.from(data);
  const head = buf.subarray(0, 32).toString('hex');
  console.log(`[binary] len=${buf.length} head=${head}`);
});

ws.on('close', (code, reason) => {
  const msg = reason && reason.length ? reason.toString() : '';
  console.log(`[close] code=${code} reason=${msg}`);
});

ws.on('error', (err) => {
  console.error(`[error] ${err.message}`);
});
