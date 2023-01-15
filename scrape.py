import asyncio
import contextlib
import math
import json
import queue
import sys
import time
import urllib.request
from threading import Thread

import requests
import websockets

# GLOBAL VARIABLE TO CHANGE
threshold = 0.145
updateRebalanceInterval = 5  # minutes


def request_rebalances(addresses, no_workers):
    class Worker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue
            self.results = {}

        def run(self):
            while True:
                content = self.queue.get()
                if content == "":
                    break
                try:
                    request = urllib.request.Request(
                        f"https://www.mexc.co/api/platform/spot/market/etf/rebalance/list?coinName={content}&pageNum=1&pageSize=2")
                    response = urllib.request.urlopen(request)
                    self.results[content] = (json.load(response))
                except Exception:
                    self.results[content] = {'code': 400}
                self.queue.task_done()

    # Create queue and add addresses
    q = queue.Queue()
    for url in addresses:
        q.put(url)

    # Workers keep working till they receive an empty string
    for _ in range(no_workers):
        q.put("")

    # Create workers and add tot the queue
    workers = []
    for _ in range(no_workers):
        worker = Worker(q)
        worker.start()
        workers.append(worker)
    # Join workers to wait till they finished
    for worker in workers:
        worker.join()

    # Combine results from all workers
    r = {}
    for worker in workers:
        r |= worker.results
    return r


def request_prices(addresses, no_workers, latest):
    class Worker(Thread):
        def __init__(self, request_queue, latest):
            Thread.__init__(self)
            self.queue = request_queue
            self.latest = latest
            self.results = {}

        def run(self):
            while True:
                curr = self.queue.get()
                if curr == "":
                    break
                try:
                    request = urllib.request.Request(
                        f"https://futures.mexc.com/api/v1/contract/kline/{curr[:-2]}_USDT?end={self.latest[curr]//1000+30}&interval=Min1&start={self.latest[curr]//1000-30}")
                    self.results[curr] = json.load(
                        urllib.request.urlopen(request))
                except Exception:
                    self.results[curr] = {'success': False}
                self.queue.task_done()

    # Create queue and add addresses
    q = queue.Queue()
    for url in addresses:
        q.put(url)

    # Workers keep working till they receive an empty string
    for _ in range(no_workers):
        q.put("")

    # Create workers and add tot the queue
    workers = []
    for _ in range(no_workers):
        worker = Worker(q, latest)
        worker.start()
        workers.append(worker)
    # Join workers to wait till they finished
    for worker in workers:
        worker.join()

    # Combine results from all workers
    r = {}
    for worker in workers:
        r |= worker.results
    return r


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def update_etfs():
    # Get all ETFs
    r = requests.get(
        "https://www.mexc.co/_next/data/Y-L5HSHVBa36HvD-zpKWF/en-US/leveraged-ETF.json")
    j = r.json()

    l = [
        curr['currency']
        for curr in j['pageProps']['spotMarkets']['USDT']
        if '3S' in curr['currency'] and curr['currency'] != 'H3RO3S'
    ]
    print(len(l))

    # Filter out non-tradingview
    keep = []
    for curr in l:
        r = requests.get(
            f"https://symbol-search.tradingview.com/s/?text=+{curr[:-2]}USDT.P&hl=1&exchange=MEXC&lang=en&type=&domain=production")
        if f"<em>{curr[:-2]}USDT.P</em>" == r.json()['symbols'][0]['symbol']:
            keep.append(curr)

    print(f"Retrieved {len(keep)} ETFs")
    return keep


def update_rebalances(l):
    start = time.time()
    latest = {}
    baskets = {}
    to_retry = l
    print("Starting update rebalances...")
    print("")
    while True:
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        print(f"ETFs left: {len(to_retry)}")
        results = request_rebalances(to_retry, 20)
        to_retry = []
        for item in results:
            if results[item]['code'] != 200:
                to_retry.append(item)
            else:
                latest[item] = results[item]['data']['resultList'][0][
                    'rebalanceTime']
                baskets[item] = results[item]['data']['resultList'][0][
                    'basketAfter']
        if not to_retry:
            break
    sys.stdout.write("\033[1A")
    sys.stdout.write("\033[K")
    sys.stdout.write("\033[1A")
    sys.stdout.write("\033[K")
    print(f"Updated rebalances in {time.time() - start}s")
    return latest, baskets


def update_prices(latest):
    start = time.time()
    to_retry = latest.keys()
    results = {}

    print("Starting update prices...")
    print("")
    while True:
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        print(f"ETFs left: {len(to_retry)}")
        results |= request_prices(to_retry, 20, latest)
        to_retry = [item for item in results if not results[item]['success']]
        if not to_retry:
            break
    ret = {
        curr: {
            'low': results[f'{curr}3L']['data']['low'][0],
            'high': results[f'{curr}3S']['data']['high'][0],
        }
        for curr in {x[:-2] for x in latest.keys()}
    }
    sys.stdout.write("\033[1A")
    sys.stdout.write("\033[K")
    sys.stdout.write("\033[1A")
    sys.stdout.write("\033[K")
    print(f"Updated prices in {time.time() - start}s")
    return ret


async def forever():
    while True:
        await get_updated_prices()


async def ping(websocket):
    while True:
        try:
            await websocket.send('{"method":"ping"}')
            await asyncio.sleep(15)
        except Exception:
            await websocket.close()
            return


async def get_updated_prices():

    while True:
        with contextlib.suppress(Exception):
            async with websockets.connect("wss://futures.mexc.com/ws") as websocket:
                async def send(message):
                    await websocket.send(json.dumps(message))

                await send({"method": "sub.contract", "param": {}})
                await send({"method": "sub.personal.user.preference", "param": {}})
                for curr in perp:
                    await send({"method": "sub.deal", "param": {"symbol": f"{curr}_USDT", "compress": True}})
                    await send({"method": "unsub.depth.step", "param": {"symbol": f"{curr}_USDT"}})
                    await send({"method": "unsub.depth.full", "param": {"symbol": f"{curr}_USDT", "limit": 20}})

                print("Connected to websocket")
                print("TIME        DIR.  PERC.  SYMB. BASKET  PLACE")
                print("--------------------------------------------")
                task = asyncio.create_task(ping(websocket))
                multiplier = threshold
                mx = {}
                mn = {}

                async for m in websocket:
                    message = json.loads(m)
                    if message["channel"] == "push.deal":
                        symbol = message["symbol"].split("_")[0]
                        curr_prices = [d['p'] for d in message["data"]]
                        if max(curr_prices) >= prices[symbol]['high'] * (
                                1 + multiplier) and max(curr_prices) > mx.get(
                                symbol, 0):
                            mx[symbol] = max(curr_prices)
                            print(
                                f'{math.trunc(time.time()): <12}UP   {(max(curr_prices) - prices[symbol]["high"]) *100 / prices[symbol]["high"] : .2f}%  {symbol: <6}{math.trunc(baskets[f"{symbol}3S"]): <8}{list(baskets.keys()).index(f"{symbol}3S"): <4}')
                        elif min(curr_prices) <= prices[symbol]['low']*(1-multiplier) and min(curr_prices) < mn.get(symbol, float('inf')):
                            mn[symbol] = min(curr_prices)
                            print(
                                f'{math.trunc(time.time()): <12}DOWN {((prices[symbol]["low"] - min(curr_prices))*100/prices[symbol]["low"]): .2f}%  {symbol: <6}{math.trunc(baskets[f"{symbol}3L"]): <8}{list(baskets.keys()).index(f"{symbol}3L"): <4}')


async def schedule_update():
    global prices, latest, baskets
    while True:
        await asyncio.sleep(60 * updateRebalanceInterval)
        print("Updating rebalances and prices...")
        latest_updates, baskets = update_rebalances(etfs)
        baskets = dict(
            sorted(
                baskets.items(),
                key=lambda item: item[1],
                reverse=True))
        recheck = {etf: latest_updates[etf]
                   for etf in etfs if latest[etf] != latest_updates[etf]}
        latest = latest_updates
        prices |= update_prices(recheck)


async def multi_thread_this():
    tasks = await asyncio.gather(schedule_update(), get_updated_prices())

if __name__ == "__main__":
    short_etfs = [
        'NEAR3S', 'YFII3S', 'LOOKS3S', 'IOTX3S', 'CELO3S', 'QTUM3S', 'AR3S',
        'APE3S', 'ZEC3S', 'DOGE3S', 'EGLD3S', 'STORJ3S', 'MATIC3S', 'BAL3S',
        'XLM3S', 'VINU3S', 'CHR3S', 'HNT3S', 'NKN3S', 'ONT3S', 'FILECOIN3S',
        'RSR3S', 'CRV3S', 'MKR3S', 'XTZ3S', 'ENJ3S', 'OGN3S', 'DENT3S',
        'LDO3S', 'ZIL3S', 'GALA3S', 'GRT3S', 'WOO3S', 'CVC3S', 'BSV3S',
        'AVAX3S', 'BNX3S', 'BEL3S', 'GTC3S', 'OMG3S', 'JASMY3S', 'ROSE3S',
        'HT3S', 'SHIB3S', 'GAL3S', 'AXS3S', 'LRC3S', 'TRX3S', 'YFI3S', 'CTK3S',
        'CELR3S', 'ATOM3S', 'AAVE3S', 'GLMR3S', 'KAVA3S', 'LUNC3S', 'REEF3S',
        'RUNE3S', 'DASH3S', 'DAR3S', 'OCEAN3S', 'OP3S', 'BAT3S', 'SOL3S',
        'ZRX3S', '1INCH3S', 'VET3S', 'ENS3S', 'COTI3S', 'DOT3S', 'RAY3S',
        'FITFI3S', 'WAVES3S', 'ONE3S', 'RLC3S', 'ARPA3S', 'ETHW3S', 'REN3S',
        'ANT3S', 'PSG3S', 'SUSHI3S', 'ICP3S', 'LINA3S', 'FLM3S', 'DUSK3S',
        'FLOW3S', 'SAND3S', 'FTM3S', 'UNI3S', 'BONE3S', 'LTC3S', 'IOST3S',
        'LIT3S', 'PEOPLE3S', 'SRM3S', 'SNX3S', 'SXP3S', 'BLZ3S', 'TRB3S',
        'NEO3S', 'ALICE3S', 'STG3S', 'AUDIO3S', 'UNFI3S', 'CHZ3S', 'XMR3S',
        'LINK3S', 'KLAY3S', 'KSM3S', 'MANA3S', 'ALGO3S', 'THETA3S', 'BNB3S',
        'CEL3S', 'FOOTBALL3S', 'USTC3S', 'CEEK3S', 'SNFT3S', 'BIT3S',
        'SANTOS3S', 'ETC3S', 'ANC3S', 'KNC3S', 'IOTA3S', 'BTC3S', 'RVN3S',
        'SFP3S', 'API33S', 'EOS3S', 'BAKE3S', 'IMX3S', 'ADA3S', 'MTL3S',
        'DYDX3S', 'C983S', 'BAND3S', 'COMP3S', 'XRP3S', 'SWEAT3S', 'MASK3S',
        'DC3S', 'ETH3S', 'ANKR3S', 'STMX3S', 'BCH3S']

    perp = [curr[:-2] for curr in short_etfs]
    long_etf = [f'{curr}3L' for curr in perp]
    etfs = long_etf + short_etfs
    print(f"Initialising {len(etfs)} ETFs...")
    latest, baskets = update_rebalances(etfs)
    baskets = dict(
        sorted(
            baskets.items(),
            key=lambda item: item[1],
            reverse=True))
    prices = update_prices(latest)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(multi_thread_this())
