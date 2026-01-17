const WebSocket = require('ws');

const apiKey = process.env.BINANCE_SBE_API_KEY || process.env.BINANCE_API_KEY;
if (!apiKey) {
  console.error('Missing BINANCE_SBE_API_KEY or BINANCE_API_KEY');
  process.exit(1);
}

const channel = process.env.BINANCE_SBE_CHANNEL || 'depth20';
const quoteAsset = (process.env.BINANCE_SBE_QUOTE || 'USDT').toUpperCase();
const batchSize = Number.parseInt(process.env.BINANCE_SBE_BATCH_SIZE || '100', 10);
const maxSymbols = Number.parseInt(process.env.BINANCE_SBE_MAX_SYMBOLS || '0', 10);
const combined =
  (process.env.BINANCE_SBE_COMBINED || '').toLowerCase() === '1' ||
  (process.env.BINANCE_SBE_COMBINED || '').toLowerCase() === 'true';
const exchangeInfoUrl =
  process.env.BINANCE_EXCHANGE_INFO_URL || 'https://api.binance.com/api/v3/exchangeInfo';

if (!Number.isFinite(batchSize) || batchSize <= 0) {
  console.error('Invalid BINANCE_SBE_BATCH_SIZE');
  process.exit(1);
}

async function fetchSymbols() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10_000);
  let data;
  try {
    const res = await fetch(exchangeInfoUrl, { signal: controller.signal });
    if (!res.ok) {
      throw new Error(`exchangeInfo status ${res.status}`);
    }
    data = await res.json();
  } finally {
    clearTimeout(timer);
  }
  const raw = data && data.symbols ? data.symbols : [];
  const symbols = raw
    .filter((s) => s && s.status === 'TRADING')
    .filter((s) => s && s.quoteAsset === quoteAsset)
    .filter((s) => s.isSpotTradingAllowed !== false)
    .map((s) => s.symbol)
    .filter(Boolean);

  if (maxSymbols > 0 && symbols.length > maxSymbols) {
    return symbols.slice(0, maxSymbols);
  }
  return symbols;
}

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function connectBatch(index, streams) {
  const baseUrl = 'wss://stream-sbe.binance.com:9443';
  const url = combined ? `${baseUrl}/stream?streams=${streams.join('/')}` : `${baseUrl}/ws`;
  const ws = new WebSocket(url, {
    headers: { 'X-MBX-APIKEY': apiKey },
  });

  let textCount = 0;
  let binaryCount = 0;

  ws.on('open', () => {
    console.log(`[open] batch=${index} streams=${streams.length} url=${url}`);
    if (!combined) {
      const subMsg = { method: 'SUBSCRIBE', params: streams, id: index + 1 };
      ws.send(JSON.stringify(subMsg));
      console.log(`[subscribe] batch=${index} streams=${streams.length}`);
    }
  });

  ws.on('ping', (data) => {
    ws.pong(data);
  });

  ws.on('message', (data, isBinary) => {
    const isBuffer = Buffer.isBuffer(data);
    if (isBinary || isBuffer) {
      binaryCount += 1;
      return;
    }
    textCount += 1;
    try {
      const obj = JSON.parse(data.toString());
      if (Object.prototype.hasOwnProperty.call(obj, 'result') && obj.result === null) {
        console.log(`[ack] batch=${index} id=${obj.id}`);
      } else if (obj && obj.msg) {
        console.log(`[text] batch=${index} msg=${obj.msg}`);
      }
    } catch {
      console.log(`[text] batch=${index} ${data.toString()}`);
    }
  });

  ws.on('close', (code, reason) => {
    const msg = reason && reason.length ? reason.toString() : '';
    console.log(
      `[close] batch=${index} code=${code} reason=${msg} text=${textCount} binary=${binaryCount}`
    );
  });

  ws.on('error', (err) => {
    console.error(`[error] batch=${index} ${err.message}`);
  });

  setInterval(() => {
    console.log(`[stats] batch=${index} text=${textCount} binary=${binaryCount}`);
  }, 10_000);
}

async function main() {
  const symbols = await fetchSymbols();
  if (!symbols.length) {
    console.error('No symbols found from exchangeInfo');
    process.exit(1);
  }
  const streams = symbols.map((s) => `${s.toLowerCase()}@${channel}`);
  const perConn = Math.min(batchSize, 1024);
  const batches = chunkArray(streams, perConn);

  console.log(
    `[init] symbols=${symbols.length} channel=${channel} batchSize=${perConn} connections=${batches.length} combined=${combined}`
  );

  batches.forEach((batch, index) => {
    connectBatch(index, batch);
  });
}

main().catch((err) => {
  console.error(`[fatal] ${err.message}`);
  process.exit(1);
});
