增加稳定的时间信号

币安
现货的depth5的推送频率是1000ms 太慢
因此现货只能用标准的btc inc作为驱动信号

spot 
{
  "e": "depthUpdate", // Event type
  "E": 1672515782136, // Event time
  "s": "BNBBTC",      // Symbol
  "U": 157,           // First update ID in event
  "u": 160,           // Final update ID in event
  "b": [              // Bids to be updated
    [
      "0.0024",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "0.0026",       // Price level to be updated
      "100"           // Quantity
    ]
  ]
}
futures
<symbol>@depth<levels>@100ms

  "E": 1571889248277, // Event time 



  {
    "topic": "kline.5.BTCUSDT",
    "data": [
        {
            "start": 1672324800000,
            "end": 1672325099999,
            "interval": "5",
            "open": "16649.5",
            "close": "16677",
            "high": "16677",
            "low": "16608",
            "volume": "2.081",
            "turnover": "34666.4005",
            "confirm": false,
            "timestamp": 1672324988882
        }
    ],
    "ts": 1672324988882,
    "type": "snapshot"
}
