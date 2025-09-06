# binance exchange
import numpy as np 
import pandas as pd 
import logging, warnings
import time, requests, redis, hashlib, hmac
from datetime import datetime, timedelta 
from urllib.parse import urlencode 

warnings.filterwarnings("ignore") 
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    filename='binance_signal.log',
                    filemode='w') 

logger = logging.getLogger(__name__) 

LIMIT = 1000 
DAYS_HISTORY = 3 
API_URL = 'https://fapi.binance.com/fapi/v1/fundingRate' 
redis_client = redis.Redis(host='localhost', port=6379, db=7) 

# 20240830 
# short_trade_list = ['DYDXUSDT', 'BAKEUSDT', 'MKRUSDT', 'ARUSDT', 'BBUSDT', 'TAOUSDT', 'FILUSDT', 'ETCUSDT', 'ENSUSDT', 'RUNEUSDT', 'ZKUSDT', 'LDOUSDT', 'ZECUSDT', 'OPUSDT', 'PEOPLEUSDT', 'BOMEUSDT', 'LTCUSDT', 'AEVOUSDT', 'VOXELUSDT', 'ORDIUSDT', 'SOLUSDT', 'ADAUSDT', 'DOGEUSDT', 'SAGAUSDT', 'XRPUSDT', 'LISTAUSDT', 'FTMUSDT', 'AMBUSDT', 'TRBUSDT', 'ARBUSDT', 'CRVUSDT', 'NOTUSDT', 'MATICUSDT']
# long_trade_list = ['ENAUSDT','AXSUSDT','TONUSDT','SEIUSDT','BEAMXUSDT','BCHUSDT','GLMUSDT','POLYXUSDT'] 
# SYMBOLS = ['DYDXUSDT', 'BAKEUSDT', 'MKRUSDT', 'ARUSDT', 'BBUSDT', 'TAOUSDT', 'FILUSDT', 'ETCUSDT', 'ENSUSDT', 'RUNEUSDT', 'ZKUSDT', 'LDOUSDT', 'ZECUSDT', 'OPUSDT', 'PEOPLEUSDT', 'BOMEUSDT', 'LTCUSDT', 'AEVOUSDT', 'VOXELUSDT', 'ORDIUSDT', 'SOLUSDT', 'ADAUSDT', 'DOGEUSDT', 'SAGAUSDT', 'XRPUSDT', 'LISTAUSDT', 'FTMUSDT', 'AMBUSDT', 'TRBUSDT', 'ARBUSDT', 'CRVUSDT', 'NOTUSDT', 'MATICUSDT','ENAUSDT','AXSUSDT','TONUSDT','SEIUSDT','BEAMXUSDT','BCHUSDT','GLMUSDT','POLYXUSDT']

# # 20240909 
# short_trade_list = ['CRVUSDT', 'VETUSDT', 'LINAUSDT', 'CKBUSDT', 'CELOUSDT', 'HOTUSDT', 'GTCUSDT', 'DYDXUSDT', 'NEOUSDT', 'CHRUSDT', 'ZECUSDT', 'BAKEUSDT', 'MINAUSDT', 'CFXUSDT', 'PHBUSDT', 'SFPUSDT', 'BELUSDT', 'SSVUSDT', 'EGLDUSDT', 'LEVERUSDT', 'LQTYUSDT', 'MKRUSDT', 'ARKMUSDT', 'ZENUSDT', 'COTIUSDT', 'LDOUSDT'] # 'LUNA2USDT', 'KEYUSDT', #### 'HIGHUSDT','ZRXUSDT','LITUSDT','SNXUSDT', 
# long_trade_list = [] # 'AXSUSDT', 'ENAUSDT', 'SEIUSDT', 'BCHUSDT', 'TONUSDT', 'TIAUSDT'
# SYMBOLS = short_trade_list + long_trade_list

# apikey = "mSMG2JPry9Wr5W1t13cCozRbeWXWIQUOoUaQOBjFWXyCYsSsx4LVAn2f6fK3i18T" 
# apisecret = "9OtPjrNbegbQBXfzGVOQXsC1jUbLYZjIrK9bfy2IIr7BBZpl7tBdB4Aa7qOJxmeE" 

# 20250207 
short_trade_list = ['ZECUSDT', 'EOSUSDT', 'IDUSDT', 'ROSEUSDT', 'MKRUSDT', 'ENJUSDT', 'DASHUSDT', 'YFIUSDT', 'COTIUSDT', 'VETUSDT', 'EGLDUSDT', 'ARUSDT', 'MAGICUSDT', 'DYDXUSDT', 'MASKUSDT', 'ICPUSDT', 'ZECUSDT', 'LDOUSDT', 'CFXUSDT', 'ADAUSDT', 'ETCUSDT', 'CELOUSDT', 'MINAUSDT', 'CKBUSDT', 'PENDLEUSDT', 'ARBUSDT', 'RSRUSDT', 'FLOWUSDT', 'NEOUSDT', 'PEOPLEUSDT']
long_trade_list = [] 
SYMBOLS = short_trade_list + long_trade_list 

# close_list = ['ARUSDT', 'ICPUSDT', 'PENDLEUSDT', 'MINAUSDT', 'MAGICUSDT', 'ADAUSDT', 'MASKUSDT','PEOPLEUSDT','CKBUSDT','MKRUSDT','NEOUSDT']
# close_list = ['ZECUSDT', 'EOSUSDT', 'IDUSDT', 'ROSEUSDT', 'MKRUSDT', 'ENJUSDT', 'DASHUSDT', 'YFIUSDT', 'COTIUSDT', 'VETUSDT', 'EGLDUSDT', 'ARUSDT', 'MAGICUSDT', 'DYDXUSDT', 'MASKUSDT', 'ICPUSDT', 'ZECUSDT', 'LDOUSDT', 'CFXUSDT', 'ADAUSDT', 'ETCUSDT', 'CELOUSDT', 'MINAUSDT', 'CKBUSDT', 'PENDLEUSDT', 'ARBUSDT', 'RSRUSDT', 'FLOWUSDT', 'NEOUSDT', 'PEOPLEUSDT']

apikey = "z9rb55Ix9vyBvzHjYVxZT0mRshd9Qyvvs40vKoCZPhb7FWwAbiUVVsdfQXv5AHHZ" 
apisecret = "yVYqipzjCe7S7Eydc4kAEMbEvi68Qmo2un91CAm0lOIXaY5vdoXf4W3YW5dNYkYw" 

class binance_rest_client(object):
    def __init__(self, apikey, secret, host='https://api.binance.com'):
        self.apikey = apikey
        self.secret = secret
        self.host = host 

    def __hashing(self, query_string):
        return hmac.new(self.secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    def __dispatch_request(self, http_method):
        session = requests.Session()
        session.headers.update({
            'Content-Type': 'application/json',
            'X-MBX-APIKEY': self.apikey
        })
        return {
            'GET': session.get,
            'DELETE': session.delete,
            'PUT': session.put,
            'POST': session.post,
        }.get(http_method, 'GET') 

    def http_request(self, http_method, url_path, payload={}, signed=True): 
        query_string = urlencode(payload, True) 
        if signed:
            timestamp = int(time.time() * 1000) 
            if query_string:
                query_string = f"{query_string}&timestamp={timestamp}"
            else:
                query_string = f'timestamp={timestamp}' 
            url = f"{self.host}{url_path}?{query_string}&signature={self.__hashing(query_string)}" 
        else: 
            url = f"{self.host}{url_path}" 

        params = {'url': url, 'params': {}, 'timeout': 5} 
        try:
            response = self.__dispatch_request(http_method)(**params) 
            if response.status_code == 200:
                return response.json() 
            else: 
                response.raise_for_status() 
        except Exception as e: 
            logger.info(f"Request error: {e}") 
            return None 

def fetch_funding_rate_history(symbol, start_time, end_time, limit=LIMIT):
    params = {
        'symbol': symbol,
        'startTime': int(start_time.timestamp() * 1000),
        'endTime': int(end_time.timestamp() * 1000),
        'limit': limit
    }
    response = requests.get(API_URL, params=params)
    
    if response.status_code == 200:
        return response.json() 
    else:
        logger.info(f"Error fetching data for {symbol}: {response.text}")
        return []
    
def get_all_premium_indices():
    """
    获取所有交易对的标记价格和资金费率信息
    """
    url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    params = {}
    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.exception("请求所有交易对资金费率失败: {}".format(e))
        return None 

def process_symbol_data(symbol, final_df, current_timestamp, loan_interest_dic, current_funding_data):
    recent_df = final_df[final_df['fundingTime'] == current_timestamp] 
    if not recent_df.empty:
        row = recent_df.iloc[0] 
        key = f"fr6=binance_swap=binance_spot={symbol.lower().split('usdt')[0] + '_usdt'}" 
        coin_loan_rate = loan_interest_dic[symbol] 
        
        if row['predict_funding_rate'] >= 0.00008: 
            signal = -1
        elif row['predict_funding_rate'] + coin_loan_rate <= -0.00008:
            signal = 1 
        else:
            signal = 0 
            
        if symbol in long_trade_list and current_funding_data[symbol] > 0.0005:
            signal = -2
        elif symbol in short_trade_list and current_funding_data[symbol] < -0.0005:
            signal = 2
            
        # # close settings
        # if symbol in close_list: 
        #     signal = 2 
        # else: 
        #     signal = 0 

            
        # value = f"{int(row['signal'])}_{int(row['fundingTime'])}" 
        # value = f"{int(1.0)}_{int(row['fundingTime'])}" 
        
        value = f"{signal}_{int(row['fundingTime'])}" 
        redis_client.set(key, value) 
        logger.info(f'{symbol}, {key}, {value}') 
        # print(f'{symbol}, {key}, {value}') 

def process_data(symbol, symbol_data):
    predict_interval = 6 
    predict_num = 1
    short_threshold = 0.00008 
    long_threshold = -0.00008 
    
    df = pd.DataFrame(symbol_data) 
    df['fundingTime'] = pd.to_numeric(df['fundingTime']) // 1000 
    df['fundingRate'] = df['fundingRate'].astype(float) 
    df = df.sort_values('fundingTime').reset_index(drop=True)[['fundingTime', 'fundingRate']] 

    min_ts, max_ts = df['fundingTime'].min(), df['fundingTime'].max() + (predict_num+1) * 28800 
    hour_df = pd.DataFrame({'fundingTime': range(min_ts, max_ts + 1, 28800)}) 
    
    df_merged = pd.merge(hour_df, df, on='fundingTime', how='left').fillna(0) 
    df_merged['predict_funding_rate'] = df_merged['fundingRate'].rolling(predict_interval).mean().shift(predict_num).fillna(0) 

    min_ts, max_ts = df_merged['fundingTime'].min(), df_merged['fundingTime'].max() 
    ts_df = pd.DataFrame({'fundingTime': range(min_ts, max_ts + 1)}) 
    final_df = pd.merge(ts_df, df_merged, on='fundingTime', how='left').ffill().tail(200000) 
    
    # long_short 
    final_df['signal'] = np.where(final_df['predict_funding_rate'] >= short_threshold, -1, 
                         np.where(final_df['predict_funding_rate'] <= long_threshold, 1, 0)) 
    
    return final_df 

def main(): 
    last_fetch_time = datetime.min 
    
    while True: 
        current_timestamp = int(time.time()) 
        current_time = datetime.now() 
        
        if current_time - last_fetch_time >= timedelta(hours=1): 
            binance_client = binance_rest_client(apikey, apisecret) 
            binance_loan_info = binance_client.http_request("GET", "/sapi/v1/margin/crossMarginData", {}) 
            loan_interest_dic = {item["coin"]+'USDT': float(item["dailyInterest"])/3 for item in binance_loan_info if item["coin"]+'USDT' in SYMBOLS} 
            # print(loan_interest_dic) 

            last_fetch_time = current_time 
            all_data = {} 
            for symbol in SYMBOLS: 
                symbol_data = [] 
                end_time = datetime.now() 
                start_time = end_time - timedelta(days=DAYS_HISTORY) 
                logger.info(f"Fetching data for {symbol} from {start_time} to {end_time}") 

                while start_time < end_time:
                    data = fetch_funding_rate_history(symbol, start_time, end_time)
                    if not data or len(data) == 1:
                        break
                    symbol_data.extend(data) 
                    start_time = datetime.fromtimestamp(int(data[-1]['fundingTime']) // 1000) 
                    logger.info(f"start_time:{start_time}, end_time:{end_time}")  

                final_df = process_data(symbol, symbol_data) 
                logger.info(f"dataframe_last_info: {final_df.tail(1).to_dict()}") 

                all_data[symbol] = final_df 

            global_data = all_data 
            
        if global_data: 
            
            all_data = get_all_premium_indices() 
            if not all_data:
                logger.info('all data is None, continue, wait 30s...')
                time.sleep(30)
                continue
            
            current_funding_data = {}
            for data in all_data:
                symbol = data.get('symbol')
                last_funding_rate = float(data.get('lastFundingRate', '0'))
                current_funding_data[symbol] = last_funding_rate
                
            for symbol in SYMBOLS: 
                symbol_df = process_symbol_data(symbol, global_data[symbol], current_timestamp,loan_interest_dic, current_funding_data) 

        logger.info("Sleeping for 30 seconds...") 
        time.sleep(30) 

if __name__ == '__main__': 
    main() 
    
    
    
    
    
    