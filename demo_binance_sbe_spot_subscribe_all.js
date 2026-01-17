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
const decodeEnabled =
  (process.env.BINANCE_SBE_DECODE || '').toLowerCase() === '1' ||
  (process.env.BINANCE_SBE_DECODE || '').toLowerCase() === 'true';
const decodeLimit = Number.parseInt(process.env.BINANCE_SBE_DECODE_LIMIT || '5', 10);
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

function readU16LE(buf, offset) {
  if (buf.length < offset + 2) {
    return null;
  }
  return buf.readUInt16LE(offset);
}

function readU32LE(buf, offset) {
  if (buf.length < offset + 4) {
    return null;
  }
  return buf.readUInt32LE(offset);
}

function readI64LE(buf, offset) {
  if (buf.length < offset + 8) {
    return null;
  }
  return buf.readBigInt64LE(offset);
}

function readI8(buf, offset) {
  if (buf.length < offset + 1) {
    return null;
  }
  return buf.readInt8(offset);
}

function readVarString8(buf, offset) {
  if (buf.length < offset + 1) {
    return { value: '', next: offset };
  }
  const len = buf.readUInt8(offset);
  const start = offset + 1;
  const end = Math.min(start + len, buf.length);
  return { value: buf.subarray(start, end).toString('utf8'), next: end };
}

function scaleMantissa(mantissa, exponent) {
  return Number(mantissa) * 10 ** exponent;
}

function decodeTrades(buf) {
  if (buf.length < 8) {
    return null;
  }
  const blockLength = readU16LE(buf, 0);
  const templateId = readU16LE(buf, 2);
  if (blockLength === null || templateId === null || templateId !== 10000) {
    return null;
  }

  const base = 8;
  if (buf.length < base + blockLength) {
    return null;
  }
  const eventTime = readI64LE(buf, base);
  const priceExponent = readI8(buf, base + 16);
  const qtyExponent = readI8(buf, base + 17);
  if (eventTime === null || priceExponent === null || qtyExponent === null) {
    return null;
  }

  let offset = base + blockLength;
  const groupBlockLength = readU16LE(buf, offset);
  const numInGroup = readU32LE(buf, offset + 2);
  if (groupBlockLength === null || numInGroup === null) {
    return null;
  }
  offset += 6;

  const trades = [];
  for (let i = 0; i < numInGroup; i += 1) {
    if (buf.length < offset + groupBlockLength || groupBlockLength < 25) {
      break;
    }
    const id = readI64LE(buf, offset);
    const price = readI64LE(buf, offset + 8);
    const qty = readI64LE(buf, offset + 16);
    const isBuyerMaker = buf.readUInt8(offset + 24) !== 0;
    if (id === null || price === null || qty === null) {
      break;
    }
    trades.push({
      id,
      price: scaleMantissa(price, priceExponent),
      qty: scaleMantissa(qty, qtyExponent),
      isBuyerMaker,
    });
    offset += groupBlockLength;
  }

  const symbol = readVarString8(buf, offset).value;
  return { eventTime, symbol, trades };
}

function connectBatch(index, streams) {
  const baseUrl = 'wss://stream-sbe.binance.com:9443';
  const url = combined ? `${baseUrl}/stream?streams=${streams.join('/')}` : `${baseUrl}/ws`;
  const ws = new WebSocket(url, {
    headers: { 'X-MBX-APIKEY': apiKey },
  });

  let textCount = 0;
  let binaryCount = 0;
  let decodedCount = 0;

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
      if (decodeEnabled && decodedCount < decodeLimit) {
        const buf = isBuffer ? data : Buffer.from(data);
        const trade = decodeTrades(buf);
        if (trade && trade.trades.length) {
          const head = trade.trades[0];
          console.log(
            `[trade] batch=${index} symbol=${trade.symbol} eventTime=${trade.eventTime.toString()} count=${trade.trades.length} head=${head.price}@${head.qty}${head.isBuyerMaker ? 'M' : ''}`
          );
          decodedCount += 1;
        }
      }
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
