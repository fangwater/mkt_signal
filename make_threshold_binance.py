import json
import websocket
import threading
import pandas as pd
from collections import deque
import time
import numpy as np
import redis
import logging
import traceback

# ==================== 配置区 ====================
# 测试标的：
# SYMBOLS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT",
#            "DOGEUSDT","TRXUSDT","DOTUSDT","LTCUSDT","BCHUSDT","LINKUSDT",
#            "AVAXUSDT","ATOMUSDT","FILUSDT","APTUSDT","OPUSDT","ARBUSDT","NEARUSDT"]
# 正式实盘标的：20250928
SYMBOLS = ['HEIUSDT', 'SANTOSUSDT', 'KDAUSDT', 'AIUSDT', 'FLUXUSDT', 'TNSRUSDT', 'NFPUSDT', 'USTCUSDT', 'TWTUSDT', 'INITUSDT', 'VANRYUSDT', 'PUMPUSDT', # 4h
           'HIGHUSDT', 'KAVAUSDT', 'SFPUSDT', 'DUSKUSDT', 'TLMUSDT', 'COTIUSDT', 'C98USDT', 'STORJUSDT', 'IOTXUSDT', 'CELOUSDT' # 8h
           ] 
MAX_LEN = 100000 
ROLLING_WINDOW = 100000 
MIN_PERIODS = 90000 

SPOT_URL_TEMPLATE = "wss://stream.binance.com:9443/ws/{}@bookTicker" 
SWAP_URL_TEMPLATE = "wss://fstream.binance.com/ws/{}@bookTicker" 

# ==================== Redis 配置 ====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY_PREFIX = "binance" 

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True) 

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[
        logging.FileHandler("make_threshold_binance.log"),
        logging.StreamHandler()
    ] 
) 

# ==================== 数据结构 ==================== 
records_dict = {symbol: deque(maxlen=MAX_LEN) for symbol in SYMBOLS} 
latest_spot_dict = {symbol: {} for symbol in SYMBOLS} 
latest_swap_dict = {symbol: {} for symbol in SYMBOLS} 
lock = threading.Lock() 

# ==================== 因子计算 ====================
def compute_spread_and_threshold(records):
    try:
        if len(records) < MIN_PERIODS:
            return {
            'bidask_sr': np.nan, 
            'askbid_sr': np.nan, 
            'bidask_lower': np.nan, 
            'bidask_upper': np.nan, 
            'askbid_upper': np.nan, 
            'askbid_lower': np.nan, 
        } 

        df = pd.DataFrame(records) 
        df["binancespotbid_binanceswapask_sr"] = (df["binancespot_bid"] - df["binanceswap_ask"]) / df["binancespot_bid"]
        df["binancespotask_binanceswapbid_sr"] = (df["binancespot_ask"] - df["binanceswap_bid"]) / df["binancespot_ask"]

        threshold_dict = { 
            'bidask_sr': df["binancespotbid_binanceswapask_sr"].iloc[-1], 
            'askbid_sr': df["binancespotask_binanceswapbid_sr"].iloc[-1], 
            'bidask_lower': df["binancespotbid_binanceswapask_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.05).iloc[-1],
            'bidask_upper': df["binancespotask_binanceswapbid_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.70).iloc[-1],
            'askbid_upper': df["binancespotask_binanceswapbid_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.95).iloc[-1],
            'askbid_lower': df["binancespotbid_binanceswapask_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.30).iloc[-1],
        } 
        return threshold_dict 
    except Exception as e:
        logging.error(f"compute_spread_and_threshold error: {e}\n{traceback.format_exc()}")
        return {
            'bidask_sr': np.nan, 
            'askbid_sr': np.nan, 
            'bidask_lower': np.nan, 
            'bidask_upper': np.nan, 
            'askbid_upper': np.nan, 
            'askbid_lower': np.nan, 
        } 

# ==================== WebSocket 回调 ====================
def on_message(ws, message, market, symbol):
    try:
        data = json.loads(message)
        bid = float(data["b"])
        ask = float(data["a"])
        with lock:
            if market == "spot":
                latest_spot_dict[symbol] = {"bid": bid, "ask": ask}
            elif market == "swap":
                latest_swap_dict[symbol] = {"bid": bid, "ask": ask}
    except Exception as e:
        logging.error(f"on_message error for {symbol}-{market}: {e}\n{traceback.format_exc()}")

def run_ws(url, market, symbol):
    while True:
        try:
            ws = websocket.WebSocketApp(url, on_message=lambda ws, msg: on_message(ws, msg, market, symbol))
            logging.info(f"Starting {market}-{symbol} websocket")
            ws.run_forever() 
        except Exception as e:
            logging.error(f"{market}-{symbol} websocket error: {e}\n{traceback.format_exc()}") 
            time.sleep(5) 

# ==================== 数据更新线程 ====================
def update_records():
    while True:
        try:
            with lock:
                for symbol in SYMBOLS:
                    latest_spot = latest_spot_dict[symbol] 
                    latest_swap = latest_swap_dict[symbol] 
                    if latest_spot and latest_swap:
                        ts = int(time.time() * 1000) 
                        record = {
                            "ts": ts,
                            "symbol": symbol,
                            "binancespot_bid": latest_spot["bid"],
                            "binancespot_ask": latest_spot["ask"],
                            "binanceswap_bid": latest_swap["bid"],
                            "binanceswap_ask": latest_swap["ask"]
                        }
                        records_dict[symbol].append(record) 
                        spread_thresh = compute_spread_and_threshold(records_dict[symbol]) 
                        record.update(spread_thresh) 

                        redis_key = f"{REDIS_KEY_PREFIX}:{symbol}"
                        try:
                            r.set(redis_key, json.dumps(record))
                            logging.info(f"Updated Redis {redis_key}: {record}")
                        except Exception as e:
                            logging.error(f"Redis set error for {symbol}: {e}\n{traceback.format_exc()}")
        except Exception as e:
            logging.error(f"update_records loop error: {e}\n{traceback.format_exc()}")
        time.sleep(1)

# ==================== 主线程启动 ====================
if __name__ == "__main__":
    for symbol in SYMBOLS:
        spot_thread = threading.Thread(target=run_ws, args=(SPOT_URL_TEMPLATE.format(symbol.lower()), "spot", symbol), name=f"WS-Spot-{symbol}")
        swap_thread = threading.Thread(target=run_ws, args=(SWAP_URL_TEMPLATE.format(symbol.lower()), "swap", symbol), name=f"WS-Swap-{symbol}")
        spot_thread.start() 
        swap_thread.start() 

    record_thread = threading.Thread(target=update_records, name="RecordUpdater")
    record_thread.start()


# import redis
# import json

# # 连接 Redis
# r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# # 获取 key 列表
# keys = r.keys("*")  # 获取所有 key
# print("所有键：", keys)

# # 遍历读取
# for key in keys:
#     value = r.get(key)
#     try:
#         data = json.loads(value)   # 反序列化 JSON
#     except (TypeError, json.JSONDecodeError):
#         data = value  # 如果不是 JSON，就直接返回字符串

#     print(f"Key={key}, Value={data}") 

# 先有这个：再有下面的套利，本质是套利作为资费信号的下单算法：
# 历史资金费率阈值示例输出：
# signal: -1 合约short现货long 1 合约long现货short 0 不操作; -2 合约short现货long 2 合约long现货short: 2/-2作用于平仓和反方向建仓 
# fundingRate 历史资金费率 
# current_fundingRate 实时资金费率 
# current_fundingRate_ma 实时资金费率的平均值 
# predict_funding_rate 预测资金费率 

# 实盘：
# if symbol的资金费率收取频率 == ‘8小时一次’: 
#     interval, predict_num, upper_threshold, lower_threshold, close_lower_threshold, close_upper_threshold = 6, 1, 0.00008, -0.00008, -0.001, 0.001
# elif symbol的资金费率收取频率 == ‘4小时一次’: 
#     interval, predict_num, upper_threshold, lower_threshold, close_lower_threshold, close_upper_threshold = 6, 1, 0.00004, -0.00004, -0.0008, 0.0008
# else: 
#     raise ValueError("Unsupported funding rate interval") 

# interval, predict_num, upper_threshold, lower_threshold, close_lower_threshold, close_upper_threshold = 6, 1, 0.00008, -0.00008, -0.001, 0.001 这些参数和阈值都是要随时调整的;
# df_merged['predict_funding_rate'] = (df_merged['fundingRate'].rolling(interval).mean().shift(predict_num).fillna(0)) 
# df_merged['signal'] = np.where(df_merged['predict_funding_rate'] >= upper_threshold, -1, np.where(df_merged['predict_funding_rate'] <= lower_threshold, 1, 0)) 
# df_merged['current_fundingRate_ma'] = df_merged['current_fundingRate'].rolling(60).mean()  # 最近1min的平均值，这里假设的实时资金费率是1s1条)
# df_merged['signal'] = np.where(df_merged['current_fundingRate_ma'] > 0.001, -2, np.where(df_merged['current_fundingRate_ma'] < -0.001, 2, 0)) 

# {'ts': 1758614430899, # 阈值更新时间戳 
#  'symbol': 'NEARUSDT', # 交易对 
#  'binancespot_bid': 3.09, # BOOKTICKER 
#  'binancespot_ask': 3.091, 
#  'binanceswap_bid': 3.087, 
#  'binanceswap_ask': 3.088, 
#  'bidask_sr': nan, # (binancespot_bid - binanceswap_ask) / binancespot_bid 
#  'askbid_sr': nan, # (binancespot_ask - binanceswap_bid) / binancespot_ask 暂不启用 
#  'bidask_lower': nan, # bidask_sr 的 5% 分位数 小于这个值就做现货开多，合约开空; 作用于开仓 
#  'bidask_upper': nan, # bidask_sr 的 70% 分位数 大于这个值就做现货开空，合约开多； 作用于平仓 
#  'askbid_upper': nan} # askbid_sr 的 95% 分位数 大于这个值就做现货开空，合约开多；作用于开仓； 暂不启用  
#  'askbid_lower': nan} # askbid_sr 的 30% 分位数 小于这个值就做现货开多，合约开空；作用于平仓； 暂不启用 

# 注：如果获取到的值为空，则说明当前数据正在累计中，尚未达到 MIN_PERIODS 要求；可以实盘的话请提前一天告知，我这边需要累计阈值的历史数据

# 所有键： ['binance:BTCUSDT', 'binance:BCHUSDT', 'binance:ATOMUSDT', 'binance:TRXUSDT', 'binance:ADAUSDT', 'binance:ARBUSDT', 'binance:ETHUSDT', 'binance:DOTUSDT', 'binance:OPUSDT', 'binance:DOGEUSDT', 'binance:BNBUSDT', 'binance:SOLUSDT', 'binance:APTUSDT', 'binance:XRPUSDT', 'binance:FILUSDT', 'binance:LTCUSDT', 'binance:LINKUSDT', 'binance:AVAXUSDT', 'binance:NEARUSDT']
# 2025-09-28 04:04:39,566 [INFO] RecordUpdater: Updated Redis binance:BTCUSDT: {'ts': 1759032279565, 'symbol': 'BTCUSDT', 'binancespot_bid': 109428.18, 'binancespot_ask': 109428.19, 'binanceswap_bid': 109381.1, 'binanceswap_ask': 109381.2, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,566 [INFO] RecordUpdater: Updated Redis binance:ETHUSDT: {'ts': 1759032279566, 'symbol': 'ETHUSDT', 'binancespot_bid': 4000.47, 'binancespot_ask': 4000.48, 'binanceswap_bid': 3998.65, 'binanceswap_ask': 3998.66, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,566 [INFO] RecordUpdater: Updated Redis binance:BNBUSDT: {'ts': 1759032279566, 'symbol': 'BNBUSDT', 'binancespot_bid': 974.09, 'binancespot_ask': 974.1, 'binanceswap_bid': 974.33, 'binanceswap_ask': 974.34, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,566 [INFO] RecordUpdater: Updated Redis binance:SOLUSDT: {'ts': 1759032279566, 'symbol': 'SOLUSDT', 'binancespot_bid': 201.3, 'binancespot_ask': 201.31, 'binanceswap_bid': 201.17, 'binanceswap_ask': 201.18, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,566 [INFO] RecordUpdater: Updated Redis binance:XRPUSDT: {'ts': 1759032279566, 'symbol': 'XRPUSDT', 'binancespot_bid': 2.7791, 'binancespot_ask': 2.7792, 'binanceswap_bid': 2.7779, 'binanceswap_ask': 2.778, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:DOGEUSDT: {'ts': 1759032279566, 'symbol': 'DOGEUSDT', 'binancespot_bid': 0.22841, 'binancespot_ask': 0.22842, 'binanceswap_bid': 0.22831, 'binanceswap_ask': 0.22832, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:TRXUSDT: {'ts': 1759032279567, 'symbol': 'TRXUSDT', 'binancespot_bid': 0.3364, 'binancespot_ask': 0.3365, 'binanceswap_bid': 0.33622, 'binanceswap_ask': 0.33623, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:LTCUSDT: {'ts': 1759032279567, 'symbol': 'LTCUSDT', 'binancespot_bid': 103.78, 'binancespot_ask': 103.79, 'binanceswap_bid': 103.74, 'binanceswap_ask': 103.75, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:AVAXUSDT: {'ts': 1759032279567, 'symbol': 'AVAXUSDT', 'binancespot_bid': 28.21, 'binancespot_ask': 28.22, 'binanceswap_bid': 28.21, 'binanceswap_ask': 28.211, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:ATOMUSDT: {'ts': 1759032279567, 'symbol': 'ATOMUSDT', 'binancespot_bid': 4.063, 'binancespot_ask': 4.064, 'binanceswap_bid': 4.059, 'binanceswap_ask': 4.06, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
# 2025-09-28 04:04:39,567 [INFO] RecordUpdater: Updated Redis binance:FILUSDT: {'ts': 1759032279567, 'symbol': 'FILUSDT', 'binancespot_bid': 2.163, 'binancespot_ask': 2.164, 'binanceswap_bid': 2.161, 'binanceswap_ask': 2.162, 'bidask_sr': nan, 'askbid_sr': nan, 'bidask_lower': nan, 'bidask_upper': nan, 'askbid_upper': nan, 'askbid_lower': nan}
