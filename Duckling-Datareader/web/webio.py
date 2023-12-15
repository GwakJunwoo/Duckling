import requests
import aiohttp
import asyncio

async def _asyn_get(session, url, **kwargs):
    async with session.get(url, **kwargs) as resp:
        assert resp.status == 200
        return await resp.text()

async def _asyn_post(session, url, **kwargs):
    async with session.post(url, **kwargs) as resp:
        assert resp.status == 200
        return await resp.text()

async def asyn_get(urls, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_get(session, url, **kwargs) for url in urls]
        )
        return resp

async def asyn_post(urls, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_post(session, url, **kwargs) for url in urls]
        )
        return resp
    
async def _asyn_get_csv(session, url, **kwargs):
    async with session.get(url, **kwargs) as resp:
        assert resp.status == 200
        return await resp.content.read()

async def asyn_get_csv(urls, **kwargs):
    async with aiohttp.ClientSession(**kwargs) as session:
        resp = await asyncio.gather(
            *[_asyn_get_csv(session, url, **kwargs) for url in urls]
        )
        return resp
    
def _get(url, **kwargs):
    resp = requests.get(url, **kwargs)
    assert resp.status_code == 200
    return resp.text

def _post(url, **kwargs):
    resp = requests.post(url, **kwargs)
    assert resp.status_code == 200
    return resp.text

def get(urls, **kwargs):
    resp = [_get(url, **kwargs) for url in urls]
    return resp

def post(urls, **kwargs):
    resp = [_post(url, **kwargs) for url in urls]
    return resp

def _get_csv(url, **kwargs):
    resp = requests.get(url, **kwargs)
    assert resp.status_code == 200
    return resp.content

def get_csv(urls, **kwargs):
    responses = [_get_csv(url, **kwargs) for url in urls]
    return responses