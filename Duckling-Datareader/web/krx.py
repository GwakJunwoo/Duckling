from datetime import datetime
from dask import delayed, compute
import dask.dataframe as dd
import pandas as pd
import time
from _util import _validate_dates
from io import BytesIO
import webio as wb

# 필요한 추가 함수 및 로직 (예: KRX 데이터 다운로드 및 처리)

def _krx_fullcode(symbol):
    # KRX 심볼 매핑 로직
    # 예: '005930' -> 'KR7005930003'
    return symbol

@delayed
def process_krx_data(content, symbol):
    # KRX 데이터 처리 로직
    # 예: CSV 변환 및 필요한 데이터 선택
    df = pd.read_csv(BytesIO(content))
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Ticker'] = symbol
    return df

def prepare_dask_df(df, start, end):
    ddf = dd.from_pandas(df, npartitions=1)
    ddf['Date'] = dd.to_datetime(ddf['Date'], format='%Y-%m-%d')
    ddf = ddf.fillna(0)
    return ddf[(ddf['Date'] >= start) & (ddf['Date'] <= end)]

async def krx_data_reader(tickers, start, end, asyn):
    start_ts = int(time.mktime(start.timetuple()))
    end_ts = int(time.mktime(end.timetuple()))

    # KRX 데이터 다운로드 URL 구성 로직
    # 예: KRX 데이터 다운로드를 위한 URL 생성
    urls = ['KRX_DOWNLOAD_URL' + _krx_fullcode(ticker) for ticker in tickers]

    if asyn: 
        responses = await wb.asyn_get_csv(urls)
    else: 
        responses = [wb.get_csv(url) for url in urls]

    processed_data = [process_krx_data(content, symbol) for content, symbol in zip(responses, tickers) if content]
    ddfs = [prepare_dask_df(df, start, end) for df in compute(*processed_data)]
    return dd.concat(ddfs)

class KrxDailyReader:
    def __init__(self, tickers, start=None, end=None, asyn=True):
        self.start, self.end = _validate_dates(start, end)
        self.tickers = tickers
        self.asyn = asyn

    async def run(self):
        return await krx_data_reader(self.tickers, self.start, self.end, self.asyn)
