from dask import delayed, compute
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import json
from _util import _validate_dates
from webio import POST

class KrxDailyReader:
    def __init__(self, tickers, start=None, end=None, asyn=True):
        self.post = POST(headers={'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36'})
        self._KRX_CODES = pd.read_csv('./info/stock.csv', encoding='euc-kr')
        self.tickers = tickers
        self.start, self.end = _validate_dates(start, end)
        self.asyn = asyn

    async def run(self):
        return await self.data_reader(self.tickers, self.start, self.end, self.asyn)
    
    def _map_symbol(self, symbol):
        return self._KRX_CODES.loc[self._KRX_CODES['ISU_SRT_CD'] == symbol]['ISU_CD']

    @delayed
    def process_data(self, content, symbol):
        content = json.loads(content)['output']
        df = pd.DataFrame(content)
        df = df[['TRD_DD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'ACC_TRDVOL', 'MKTCAP']]
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'MarketCap']
        df['Ticker'] = symbol
        return df

    def prepare_dask_df(self, df, start, end):
        ddf = dd.from_pandas(df, npartitions=1)
        ddf['Date'] = dd.to_datetime(ddf['Date'], format='%Y/%m/%d')
        for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'MarketCap']:
            ddf[col] = ddf[col].str.replace(',', '').astype(float)
        ddf = ddf.fillna(0)
        return ddf[(ddf['Date'] >= start) & (ddf['Date'] <= end)]

    async def data_reader(self, tickers, start, end, asyn):
        start_ts = start.strftime('%Y%m%d')
        end_ts = end.strftime('%Y%m%d')
        
        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
        'locale': 'ko_KR',
        'isuCd': '',
        'isuCd2': '',
        'strtDd': start_ts,
        'endDd': end_ts,
        'adjStkPrc_check': 'Y',
        'adjStkPrc': 2,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        }
        datas = [{**data, 'isuCd': self._map_symbol(ticker)} for ticker in tickers]

        if asyn: responses = await self.post.async_read(url, datas=datas)
        else: responses = self.post._sync_read(url, datas=datas)
        processed_data = [self.process_data(content, symbol) for content, symbol in zip(responses, tickers) if content]
        ddfs = [self.prepare_dask_df(df, start, end) for df in compute(*processed_data)]
        return dd.concat(ddfs)