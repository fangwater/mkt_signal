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

const jsonOutput =
  (process.env.BINANCE_SBE_JSON || '').toLowerCase() === '1' ||
  (process.env.BINANCE_SBE_JSON || '').toLowerCase() === 'true';

const TEMPLATE_NAMES = {
  10000: 'TradesStreamEvent',
  10001: 'BestBidAskStreamEvent',
  10002: 'DepthSnapshotStreamEvent',
  10003: 'DepthDiffStreamEvent',
};

function formatDecimal(mantissa, exponent) {
  const neg = mantissa < 0n;
  let s = (neg ? -mantissa : mantissa).toString();
  if (exponent === 0) {
    return (neg ? '-' : '') + s;
  }
  if (exponent > 0) {
    return (neg ? '-' : '') + s + '0'.repeat(exponent);
  }
  const exp = -exponent;
  if (s.length <= exp) {
    return (neg ? '-' : '') + '0.' + '0'.repeat(exp - s.length) + s;
  }
  const idx = s.length - exp;
  return (neg ? '-' : '') + s.slice(0, idx) + '.' + s.slice(idx);
}

function readHeader(buf) {
  if (buf.length < 8) {
    return null;
  }
  return {
    blockLength: buf.readUInt16LE(0),
    templateId: buf.readUInt16LE(2),
    schemaId: buf.readUInt16LE(4),
    version: buf.readUInt16LE(6),
    bodyOffset: 8,
  };
}

function readVarString8(buf, offset) {
  if (offset >= buf.length) {
    return { value: '', next: offset };
  }
  const len = buf.readUInt8(offset);
  const start = offset + 1;
  const end = Math.min(start + len, buf.length);
  return { value: buf.subarray(start, end).toString('utf8'), next: end };
}

function readGroup(buf, offset, priceExponent, qtyExponent, limit) {
  if (offset + 4 > buf.length) {
    return { blockLength: 0, numInGroup: 0, levels: [], next: offset };
  }
  const blockLength = buf.readUInt16LE(offset);
  const numInGroup = buf.readUInt16LE(offset + 2);
  let pos = offset + 4;
  const levels = [];
  for (let i = 0; i < numInGroup; i += 1) {
    if (pos + blockLength > buf.length) {
      break;
    }
    if (blockLength >= 16 && i < limit) {
      const price = buf.readBigInt64LE(pos);
      const qty = buf.readBigInt64LE(pos + 8);
      levels.push([formatDecimal(price, priceExponent), formatDecimal(qty, qtyExponent)]);
    }
    pos += blockLength;
  }
  return { blockLength, numInGroup, levels, next: pos };
}

function decodeDepthSnapshot(buf, header) {
  const base = header.bodyOffset;
  if (buf.length < base + header.blockLength) {
    return null;
  }
  const eventTime = buf.readBigInt64LE(base);
  const bookUpdateId = buf.readBigInt64LE(base + 8);
  const priceExponent = buf.readInt8(base + 16);
  const qtyExponent = buf.readInt8(base + 17);
  let pos = base + header.blockLength;
  const bids = readGroup(buf, pos, priceExponent, qtyExponent, 5);
  pos = bids.next;
  const asks = readGroup(buf, pos, priceExponent, qtyExponent, 5);
  pos = asks.next;
  const symbol = readVarString8(buf, pos).value;
  return {
    templateId: header.templateId,
    eventTime,
    bookUpdateId,
    priceExponent,
    qtyExponent,
    bids,
    asks,
    symbol,
  };
}

function decodeDepthDiff(buf, header) {
  const base = header.bodyOffset;
  if (buf.length < base + header.blockLength) {
    return null;
  }
  const eventTime = buf.readBigInt64LE(base);
  const firstBookUpdateId = buf.readBigInt64LE(base + 8);
  const lastBookUpdateId = buf.readBigInt64LE(base + 16);
  const priceExponent = buf.readInt8(base + 24);
  const qtyExponent = buf.readInt8(base + 25);
  let pos = base + header.blockLength;
  const bids = readGroup(buf, pos, priceExponent, qtyExponent, 5);
  pos = bids.next;
  const asks = readGroup(buf, pos, priceExponent, qtyExponent, 5);
  pos = asks.next;
  const symbol = readVarString8(buf, pos).value;
  return {
    templateId: header.templateId,
    eventTime,
    firstBookUpdateId,
    lastBookUpdateId,
    priceExponent,
    qtyExponent,
    bids,
    asks,
    symbol,
  };
}

function decodeBestBidAsk(buf, header) {
  const base = header.bodyOffset;
  if (buf.length < base + header.blockLength) {
    return null;
  }
  const eventTime = buf.readBigInt64LE(base);
  const bookUpdateId = buf.readBigInt64LE(base + 8);
  const priceExponent = buf.readInt8(base + 16);
  const qtyExponent = buf.readInt8(base + 17);
  const bidPrice = buf.readBigInt64LE(base + 18);
  const bidQty = buf.readBigInt64LE(base + 26);
  const askPrice = buf.readBigInt64LE(base + 34);
  const askQty = buf.readBigInt64LE(base + 42);
  const symbol = readVarString8(buf, base + header.blockLength).value;
  return {
    templateId: header.templateId,
    eventTime,
    bookUpdateId,
    priceExponent,
    qtyExponent,
    bidPrice: formatDecimal(bidPrice, priceExponent),
    bidQty: formatDecimal(bidQty, qtyExponent),
    askPrice: formatDecimal(askPrice, priceExponent),
    askQty: formatDecimal(askQty, qtyExponent),
    symbol,
  };
}

function decodeTrades(buf, header) {
  const base = header.bodyOffset;
  if (buf.length < base + header.blockLength) {
    return null;
  }
  const eventTime = buf.readBigInt64LE(base);
  const transactTime = buf.readBigInt64LE(base + 8);
  const priceExponent = buf.readInt8(base + 16);
  const qtyExponent = buf.readInt8(base + 17);
  let pos = base + header.blockLength;
  if (pos + 4 > buf.length) {
    return null;
  }
  const blockLength = buf.readUInt16LE(pos);
  const numInGroup = buf.readUInt16LE(pos + 2);
  pos += 4;
  const trades = [];
  for (let i = 0; i < numInGroup; i += 1) {
    if (pos + blockLength > buf.length) {
      break;
    }
    const id = buf.readBigInt64LE(pos);
    const price = buf.readBigInt64LE(pos + 8);
    const qty = buf.readBigInt64LE(pos + 16);
    const isBuyerMaker = buf.readUInt8(pos + 24);
    trades.push({
      id,
      price: formatDecimal(price, priceExponent),
      qty: formatDecimal(qty, qtyExponent),
      isBuyerMaker: isBuyerMaker === 1,
    });
    pos += blockLength;
  }
  const symbol = readVarString8(buf, pos).value;
  return {
    templateId: header.templateId,
    eventTime,
    transactTime,
    priceExponent,
    qtyExponent,
    trades,
    symbol,
  };
}

function emit(obj, textLine) {
  if (jsonOutput) {
    console.log(JSON.stringify(obj));
  } else {
    console.log(textLine);
  }
}

function logDepthSnapshot(msg) {
  const bidsSample = msg.bids.levels.map((v) => v.join('@')).join(', ');
  const asksSample = msg.asks.levels.map((v) => v.join('@')).join(', ');
  emit(
    {
      type: 'depth20',
      symbol: msg.symbol,
      eventTime: msg.eventTime.toString(),
      bookUpdateId: msg.bookUpdateId.toString(),
      bids: msg.bids.numInGroup,
      asks: msg.asks.numInGroup,
      sampleBids: msg.bids.levels,
      sampleAsks: msg.asks.levels,
    },
    `[depth20] symbol=${msg.symbol} eventTime=${msg.eventTime.toString()} bookUpdateId=${msg.bookUpdateId.toString()} bids=${msg.bids.numInGroup} asks=${msg.asks.numInGroup} sampleBids=${bidsSample} sampleAsks=${asksSample}`
  );
}

function logDepthDiff(msg) {
  const bidsSample = msg.bids.levels.map((v) => v.join('@')).join(', ');
  const asksSample = msg.asks.levels.map((v) => v.join('@')).join(', ');
  emit(
    {
      type: 'depth',
      symbol: msg.symbol,
      eventTime: msg.eventTime.toString(),
      firstUpdateId: msg.firstBookUpdateId.toString(),
      lastUpdateId: msg.lastBookUpdateId.toString(),
      bids: msg.bids.numInGroup,
      asks: msg.asks.numInGroup,
      sampleBids: msg.bids.levels,
      sampleAsks: msg.asks.levels,
    },
    `[depth] symbol=${msg.symbol} eventTime=${msg.eventTime.toString()} firstUpdateId=${msg.firstBookUpdateId.toString()} lastUpdateId=${msg.lastBookUpdateId.toString()} bids=${msg.bids.numInGroup} asks=${msg.asks.numInGroup} sampleBids=${bidsSample} sampleAsks=${asksSample}`
  );
}

function logBestBidAsk(msg) {
  emit(
    {
      type: 'bestBidAsk',
      symbol: msg.symbol,
      eventTime: msg.eventTime.toString(),
      bookUpdateId: msg.bookUpdateId.toString(),
      bidPrice: msg.bidPrice,
      bidQty: msg.bidQty,
      askPrice: msg.askPrice,
      askQty: msg.askQty,
    },
    `[bestBidAsk] symbol=${msg.symbol} eventTime=${msg.eventTime.toString()} bookUpdateId=${msg.bookUpdateId.toString()} bid=${msg.bidPrice}@${msg.bidQty} ask=${msg.askPrice}@${msg.askQty}`
  );
}

function logTrades(msg) {
  const head = msg.trades.slice(0, 3);
  emit(
    {
      type: 'trades',
      symbol: msg.symbol,
      eventTime: msg.eventTime.toString(),
      transactTime: msg.transactTime.toString(),
      trades: msg.trades,
    },
    `[trades] symbol=${msg.symbol} eventTime=${msg.eventTime.toString()} transactTime=${msg.transactTime.toString()} count=${msg.trades.length} head=${head
      .map((t) => `${t.price}@${t.qty}${t.isBuyerMaker ? 'M' : ''}`)
      .join(', ')}`
  );
}

ws.on('open', () => {
  console.log(`[open] ${url}`);
});

ws.on('ping', (data) => {
  ws.pong(data);
  console.log(`[ping] len=${data.length}`);
});

ws.on('message', (data, isBinary) => {
  const isBuffer = Buffer.isBuffer(data);
  if (!isBinary && !isBuffer) {
    console.log(`[text] ${data.toString()}`);
    return;
  }
  const buf = isBuffer ? data : Buffer.from(data);
  const header = readHeader(buf);
  if (!header) {
    console.log(`[binary] len=${buf.length} head=${buf.subarray(0, 16).toString('hex')}`);
    return;
  }
  const name = TEMPLATE_NAMES[header.templateId] || 'Unknown';
  if (header.templateId === 10002) {
    const msg = decodeDepthSnapshot(buf, header);
    if (msg) {
      logDepthSnapshot(msg);
      return;
    }
  } else if (header.templateId === 10003) {
    const msg = decodeDepthDiff(buf, header);
    if (msg) {
      logDepthDiff(msg);
      return;
    }
  } else if (header.templateId === 10001) {
    const msg = decodeBestBidAsk(buf, header);
    if (msg) {
      logBestBidAsk(msg);
      return;
    }
  } else if (header.templateId === 10000) {
    const msg = decodeTrades(buf, header);
    if (msg) {
      logTrades(msg);
      return;
    }
  }
  console.log(
    `[binary] templateId=${header.templateId} (${name}) schemaId=${header.schemaId} version=${header.version} len=${buf.length}`
  );
});

ws.on('close', (code, reason) => {
  const msg = reason && reason.length ? reason.toString() : '';
  console.log(`[close] code=${code} reason=${msg}`);
});

ws.on('error', (err) => {
  console.error(`[error] ${err.message}`);
});
