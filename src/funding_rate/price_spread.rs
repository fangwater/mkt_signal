//现在，用中文，新增一个价差因子.rs 我来描述我的需求。
// 写成单例类
// 维护两个哈希表，一个askbid，一个bidask
// key = (TradingVenue,TradingVenue) 表示价差是在哪两个exchange之间
// value = hashmap, key = (symbol, symbol) value是价差因子的值
// askbid 和 bidask 的计算参考旧代码

// 维护一个哈希表，即阈值表，key是 (TradingVenue,symbol，TradingVenue，symbol)
// # 正套
// "forward_arb_open_tr": ("quantile", "bidask", 10.0),
// "forward_arb_cancel_tr": ("quantile", "bidask", 15.0),
// "forward_arb_close_tr": ("quantile", "askbid", 90.0),
// "forward_arb_cancel_close_tr": ("quantile", "askbid", 85.0),
// # 反套
// "backward_arb_open_tr": ("quantile", "askbid", 5.0),
// "backward_arb_cancel_tr": ("quantile", "askbid", 10.0),
// "backward_arb_close_tr": ("quantile", "bidask", 95.0),
// "backward_arb_cancel_close_tr": ("quantile", "bidask", 90.0), quantile和90.0对程序无意义


///map存储的sturct "bidask" 







