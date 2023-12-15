from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd
import re
from _util import _validate_dates
from io import StringIO
import webio as wb

@delayed
def process_data(content, symbol):
    data_list = re.findall('<item data=\"(.*?)\" />', content, re.DOTALL)
    data = '\n'.join(data_list)
    df = pd.read_csv(StringIO(data), delimiter='|', header=None, dtype={0: str})
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Ticker'] = symbol
    return df

def prepare_dask_df(df, start, end):
    ddf = dd.from_pandas(df, npartitions=1)
    ddf['Date'] = dd.to_datetime(ddf['Date'], format='%Y%m%d')
    #ddf['Change'] = (ddf['Close'] - ddf['Close'].shift(1)) / ddf['Close'].shift(1)
    ddf = ddf.fillna(0)
    return ddf[(ddf['Date'] >= start) & (ddf['Date'] <= end)]

async def data_reader(tickers, start, end, asyn):
    url = 'https://fchart.stock.naver.com/sise.nhn?timeframe=day&count=6000&requestType=0&symbol='
    urls = [url + ticker for ticker in tickers]

    if asyn:
        responses = await wb.asyn_get(urls)
    else:
        responses = [wb._get(url + ticker) for ticker in tickers]

    processed_data = [process_data(content, symbol) for content, symbol in zip(responses, tickers) if content]
    ddfs = [prepare_dask_df(df, start, end) for df in compute(*processed_data)]
    return dd.concat(ddfs)

class NaverDataReader:
    def __init__(self, tickers, start=None, end=None, asyn=True):
        self.tickers = tickers
        self.start, self.end = _validate_dates(start, end)
        self.asyn = asyn

    async def run(self):
        return await data_reader(self.tickers, self.start, self.end, self.asyn)
