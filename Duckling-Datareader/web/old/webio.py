import requests
import aiohttp
import asyncio

async def _asyn_get(session, url, headers=None, **kwargs):
    async with session.get(url, headers=headers, **kwargs) as resp:
        assert resp.status == 200
        return await resp.text()

async def _asyn_post(session, url, data=None, headers=None, **kwargs):
    async with session.post(url, data=data, headers=headers, **kwargs) as resp:
        assert resp.status == 200
        return await resp.text()

async def asyn_get(urls, headers=None, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_get(session, url, headers=headers, **kwargs) for url in urls]
        )
        return resp

async def asyn_post(urls, data=None, headers=None, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_post(session, url, data=data, headers=headers, **kwargs) for url in urls]
        )
        return resp
    
async def _asyn_get_csv(session, url, headers=None, **kwargs):
    async with session.get(url, headers=headers, **kwargs) as resp:
        assert resp.status == 200
        return await resp.content.read()

async def asyn_get_csv(urls, headers=None, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_get_csv(session, url, headers=headers, **kwargs) for url in urls]
        )
        return resp
    
def _get(url, headers=None, **kwargs):
    resp = requests.get(url, headers=headers, **kwargs)
    assert resp.status_code == 200
    return resp.text

def _post(url, data=None, headers=None, **kwargs):
    resp = requests.post(url, data=data, headers=headers, **kwargs)
    assert resp.status_code == 200
    return resp.text

def get(urls, headers=None, **kwargs):
    resp = [_get(url, headers=headers, **kwargs) for url in urls]
    return resp

def post(urls, data=None, headers=None, **kwargs):
    resp = [_post(url, data=data, headers=headers, **kwargs) for url in urls]
    return resp

def _get_csv(url, headers=None, **kwargs):
    resp = requests.get(url, headers=headers, **kwargs)
    assert resp.status_code == 200
    return resp.content

def get_csv(urls, headers=None, **kwargs):
    responses = [_get_csv(url, headers=headers, **kwargs) for url in urls]
    return responses