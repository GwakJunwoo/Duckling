import requests
import aiohttp
import asyncio
import time

class GET:
    def __init__(self, headers=None, request_timeout=10, delay_between_requests=0):
        
        self.request_timeout = request_timeout
        self.delay_between_requests = delay_between_requests

        self.headers = headers

    async def _async_read(self, session, url, headers, return_type='String', **kwargs):
        assert return_type.lower() == 'string' or return_type.lower() == 'byte'
        async with session.get(url, headers=headers, **kwargs) as resp:
            assert resp.status == 200
            await asyncio.sleep(self.delay_between_requests)
            if return_type.lower() == 'string': return await resp.text()
            elif return_type.lower() == 'byte': return await resp.content.read()

    async def async_read(self, urls, return_type='String', **kwargs):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.request_timeout), **kwargs) as session:
            resp = await asyncio.gather(
                *[self._async_read(session, url, headers=self.headers, return_type=return_type, **kwargs) for url in urls]
            )
            return resp

    def _sync_read(self, url, headers, return_type='String', **kwargs):
        assert return_type.lower() == 'string' or return_type.lower() == 'byte'
        resp = requests.get(url, headers=headers, timeout=self.request_timeout, *kwargs)
        assert resp.status_code == 200
        time.sleep(self.delay_between_requests)
        if return_type.lower() == 'string': return resp.text
        elif return_type.lower() == 'byte': return resp.content
    
    def sync_read(self, urls, return_type='String', **kwargs):
        resp = [self._sync_read(url, headers=self.headers, return_type=return_type, **kwargs) for url in urls]
        return resp

class POST:
    def __init__(self, headers=None, request_timeout=10, delay_between_requests=0):
        
        self.request_timeout = request_timeout
        self.delay_between_requests = delay_between_requests

        self.headers = headers

    async def _async_read(self, session, url, data=None, headers=None, **kwargs):
        async with session.post(url, data=data, headers=headers, **kwargs) as resp:
            assert resp.status == 200
            await asyncio.sleep(self.delay_between_requests)
            return await resp.text()
        
    async def async_read(self, url, datas=None, **kwargs):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.request_timeout), **kwargs) as session:
            resp = await asyncio.gather(
                *[self._async_read(session, url, data=data, headers=self.headers, **kwargs) for data in datas]
            )
            return resp
    
    def _sync_read(self, url, data=None, headers=None, **kwargs):
        resp = requests.post(url, data=data, headers=headers, timeout=self.request_timeout, **kwargs)
        assert resp.status_code == 200
        time.sleep(self.delay_between_requests)
        return resp.text
    
    def sync_read(self, url, datas=None, headers=None, **kwargs):
        resp = [self._sync_read(url, data=data, headers=headers, **kwargs) for data in datas]
        return resp
