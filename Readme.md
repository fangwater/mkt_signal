- executionReport → BasicAccountEventType::OrderUpdate
    - payload：BinanceBasicOrderMsg
    - venue = VENUE_MARGIN
    - 
- outboundAccountPosition → BasicAccountEventType::BalanceUpdate
    - payload：BasicBalanceMsg
    - 只取 free balance (f)，忽略 locked (l)

- liabilityChange → BasicAccountEventType::BorrowInterest
    - payload：BasicBorrowInterestMsg