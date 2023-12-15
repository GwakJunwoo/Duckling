from datetime import datetime
from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd
import time
from _util import _validate_dates
from io import BytesIO
import webio as wb

def _map_symbol(symbol):

    curr_list = ['KRW', 'EUR', 'CNY', 'JPY', 'CHF']
    coin_list = ['BTC', 'ETH', 'USDT', 'BNB', 'USDC', 'XRP', 'BUSD', 'ADA', 'SOL', 'DOGE']

    if symbol.startswith('USD/'):
        symbol = symbol.split('USD/')[1] + '=X'            
    elif any(map(symbol.startswith, [f'{curr}/' for curr in curr_list])):
        symbol = symbol.replace('/', '') + '=X'          
    elif any(map(symbol.startswith, [f'{coin}/' for coin in coin_list])):
        symbol = symbol.replace('/', '-')

    return symbol

@delayed
def process_data(content, symbol):
    df = pd.read_csv(BytesIO(content))
    df = df.drop('Close', axis=1)
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Ticker'] = symbol
    return df

def prepare_dask_df(df, start, end):
    ddf = dd.from_pandas(df, npartitions=1)
    ddf['Date'] = dd.to_datetime(ddf['Date'], format='%Y-%m-%d')
    #ddf['Change'] = (ddf['Close'] - ddf['Close'].shift(1)) / ddf['Close'].shift(1)
    ddf = ddf.fillna(0)
    return ddf[(ddf['Date'] >= start) & (ddf['Date'] <= end)]

async def data_reader(tickers, start, end, asyn):
    start_ts = int(time.mktime(start.timetuple()))
    end_ts = int(time.mktime(end.timetuple()))

    url_prefix = 'https://query1.finance.yahoo.com/v7/finance/download/'
    url_profix = f'?period1={start_ts}&period2={end_ts}&interval=1d&events=history&includeAdjustedClose=true'
    
    urls = [url_prefix + _map_symbol(ticker) + url_profix for ticker in tickers]

    if asyn: responses = await wb.asyn_get_csv(urls)
    else: responses = [wb.get_csv(url) for url in urls]

    processed_data = [process_data(content, symbol) for content, symbol in zip(responses, tickers) if content]
    ddfs = [prepare_dask_df(df, start, end) for df in compute(*processed_data)]
    return dd.concat(ddfs)

class YahooDailyReader:
    def __init__(self, tickers, start=None, end=None, asyn=True):
        self.start, self.end = _validate_dates(start, end)

        self.tickers = tickers
        self.asyn = asyn

    async def run(self):
        return await data_reader(self.tickers, self.start, self.end, self.asyn)