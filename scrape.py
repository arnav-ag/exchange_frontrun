import asyncio
import contextlib
import json
import math
import queue
import sys
import time
import urllib.request
import sqlite3
from datetime import datetime
from threading import Thread

import requests
import websockets
from loguru import logger

# GLOBAL VARIABLE TO CHANGE
threshold = 0.01
update_rebalance_interval = 5  # minutes

# Logger initialize
logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:DD-MM-YY HH:mm:ss}</green> | <yellow>{message}</yellow>",
    level="INFO")

def create_database(list_of_tokens):
    conn = sqlite3.connect('Price_table.db')
    cur = conn.cursor()
    #for name in list_of_tokens:
        #cur.execute('''CREATE TABLE {} ('TIME', 'DIR', 'PERC', 'SYMBOL', 'BASKET', 'CURRENT', 'PREVIOUS', 'TARGET')'''.format(name))
    return cur, conn

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
                        f"https://futures.mexc.com/api/v1/contract/kline/{curr[:-2]}_USDT?end={self.latest[curr] // 1000 + 30}&interval=Min1&start={self.latest[curr] // 1000 - 30}"
                    )
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


def update_etfs(num=30):
    # Get all ETFs
    '''
    :param num: the number of unique etfs coins to return, default = 30
    :return: list of unique etfs coins names
    '''
    r = requests.get(
        "https://api.mexc.com/api/v3/etf/info"
    )
    #getting list of unique token values
    keep = list(set([r.json()[i]['symbol'][:-6] for i in range(len(r.json()))]))[:num]
    logger.info(f"Retrieved {len(keep)} ETFs from MEXC")
    #adding 3l and 3s to make a final list
    l_etfs = [f'{x}3L' for x in keep]
    s_etfs = [f'{x}3S' for x in keep]
    l = l_etfs + s_etfs
    logger.info(f"Retrieved {len(keep)} ETFs from MEXC")
    logger.debug(len(l))

    # Filter out non-tradingview
    keep = []
    for curr in l:
        r = requests.get(
            f"https://symbol-search.tradingview.com/s/?text=+{curr[:-2]}USDT.P&hl=1&exchange=MEXC&lang=en&type=&domain=production"
        )
        if f"<em>{curr[:-2]}USDT.P</em>" == r.json()['symbols'][0]['symbol']:
            keep.append(curr)

    logger.info(f"Retrieved {len(keep)} perpetuals from TradingView")
    return keep


def update_rebalances(l, debug=True):
    start = time.time()
    latest = {}
    baskets = {}
    to_retry = l
    if debug:
        logger.info("Starting update rebalances...")
        logger.info("")
    while True:
        if debug:
            sys.stdout.write("\033[1A")
            sys.stdout.write("\033[K")
            logger.info(f"Rebalances left: {len(to_retry)}")
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
    if debug:
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        logger.info(f"Updated rebalances in {time.time() - start}s")
    return latest, baskets


def update_prices(latest, debug=True, prices=None):
    if prices is None:
        prices = {}
    start = time.time()
    to_retry = latest.keys()
    results = {}
    if debug:
        logger.info("Starting update prices...")
        logger.info("")
    while True:
        if debug:
            sys.stdout.write("\033[1A")
            sys.stdout.write("\033[K")
            logger.info(f"Prices left: {len(to_retry)}")
        results |= request_prices(to_retry, 20, latest)
        to_retry = [item for item in results if not results[item]['success']]
        if not to_retry:
            break
    ret = {
        curr:
            {'low': results[f'{curr}3L']['data']['low'][0]
            if f'{curr}3L' in results else prices[curr]['low'],
             'high': results[f'{curr}3S']['data']['high'][0]
             if f'{curr}3S' in results else prices[curr]['high'], }
        for curr in {x[: -2] for x in latest.keys()}}
    if debug:
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")
        logger.info(f"Updated prices in {time.time() - start}s")
    return ret


async def forever():
    while True:
        await get_updated_prices(cur, conn)


async def ping(websocket):
    while True:
        try:
            await websocket.send('{"method":"ping"}')
            await asyncio.sleep(15)
        except Exception:
            await websocket.close()
            return


async def get_updated_prices(cur, conn):
    global to_print, mx, mn
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

                logger.info("Connected to websocket")
                to_print = f"|{'TIME'.center(18, ' ')}|{'DIR.'.center(8, ' ')}|{'PERC'.center(10, ' ')}|{'SYMBOL'.center(12, ' ')}|{'BASKET'.center(12, ' ')}|{'CURRENT'.center(11, ' ')}|{'PREVIOUS'.center(11, ' ')}|{'TARGET'.center(11, ' ')}|"
                print("-" * len(to_print))
                print(to_print)
                print("-" * len(to_print))
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
                            TIME = f'{str(datetime.now().strftime("%d/%m %H:%M:%S"))}'
                            DIR = f'{"UP"}'
                            PERC = f'{"{:.2f}%".format((max(curr_prices) - prices[symbol]["high"]) * 100 / prices[symbol]["high"])}'
                            SYMBOL = f'{symbol.center(12, " ")}'
                            BASKET = f'{str.format("{:,},", math.trunc(baskets[f"{symbol}3S"]))}'
                            CURRENT = f'|{str(round(max(curr_prices), 5))}'
                            PREVIOUS = f'{str(round(prices[symbol]["high"], 5))}'
                            TARGET = f'{str(round(prices[symbol]["high"] * 1.15, 5))}'

                            print(
                                f'|{str(datetime.now().strftime("%d/%m %H:%M:%S")).center(18, " ")}' +
                                f'|{"UP".center(8, " ")}' +
                                f'|{"{:.2f}%".format((max(curr_prices) - prices[symbol]["high"]) * 100 / prices[symbol]["high"]).center(10, " ")}' +
                                f'|{symbol.center(12, " ")}' +
                                f'|{str.format("{:,},", math.trunc(baskets[f"{symbol}3S"])).center(12, " ")}' +
                                f'|{str(round(max(curr_prices), 5)).center(11, " ")}' +
                                f'|{str(round(prices[symbol]["high"], 5)).center(11, " ")}' +
                                f'|{str(round(prices[symbol]["high"] * 1.15, 5)).center(11, " ")}|')
                            row = (TIME, DIR, PERC, SYMBOL, BASKET, CURRENT, PREVIOUS, TARGET)
                            cur.execute(f"INSERT INTO {symbol} VALUES {row}")
                            conn.commit()
                        elif min(curr_prices) <= prices[symbol]['low'] * (1 - multiplier) and min(curr_prices) < mn.get(
                                symbol, float('inf')):
                            mn[symbol] = min(curr_prices)
                            TIME = f'{str(datetime.now().strftime("%d/%m %H:%M:%S"))}'
                            DIR = f'{"DOWN".center(8, " ")}'
                            PERC = f'{"{:.2f}%".format((max(curr_prices) - prices[symbol]["high"]) * 100 / prices[symbol]["low"])}'
                            SYMBOL = f'{symbol}'
                            BASKET = f'{str.format("{:,}", math.trunc(baskets[f"{symbol}3L"]))}'
                            CURRENT = f'|{str(round(min(curr_prices), 5))}'
                            PREVIOUS = f'{str(round(prices[symbol]["low"], 5))}'
                            TARGET = f'{str(round(prices[symbol]["low"] * 0.85, 5))}'
                            row = (TIME, DIR, PERC, SYMBOL, BASKET, CURRENT, PREVIOUS, TARGET)
                            print(
                                f'|{str(datetime.now().strftime("%d/%m %H:%M:%S")).center(18, " ")}' +
                                f'|{"DOWN".center(8, " ")}' +
                                f'|{"{:.2f}%".format((prices[symbol]["low"] - min(curr_prices)) * 100 / prices[symbol]["low"]).center(10, " ")}' +
                                f'|{symbol.center(12, " ")}' +
                                f'|{str.format("{:,}", math.trunc(baskets[f"{symbol}3L"])).center(12, " ")}' +
                                f'|{str(round(min(curr_prices), 5)).center(11, " ")}' +
                                f'|{str(round(prices[symbol]["low"], 5)).center(11, " ")}' +
                                f'|{str(round(prices[symbol]["low"] * 0.85, 5)).center(11, " ")}')
                            cur.execute(f"INSERT INTO {symbol} VALUES {row}")
                            conn.commit()


def save_files(rebalances, prices):
    with open('rebalances.json', 'w') as fp:
        json.dump(rebalances, fp, sort_keys=True, indent=4)
    with open('prices.json', 'w') as fp:
        json.dump(prices, fp, sort_keys=True, indent=4)
    return


def read_files() -> tuple[dict[str, int], dict[str, float]]:
    try:
        with open('rebalances.json', 'r') as fp:
            rebalances = json.load(fp)
        with open('prices.json', 'r') as fp:
            prices = json.load(fp)
        return rebalances, prices
    except Exception:
        return {}, {}


async def schedule_update():
    global prices, latest, baskets, mx, mn
    while True:
        await asyncio.sleep(60 * update_rebalance_interval)

        latest_updates, rebalanced_baskets = update_rebalances(
            etfs, debug=False
        )
        rebalanced_baskets = dict(
            sorted(
                baskets.items(),
                key=lambda item: item[1],
                reverse=True))
        if new_rebalances := {
            etf: latest_updates[etf]
            for etf in etfs
            if latest[etf] != latest_updates[etf]
        }:
            mx = {}
            mn = {}
            latest = latest_updates
            prices |= update_prices(new_rebalances, debug=False, prices=prices)
            new_baskets = {etf: (baskets[etf],
                                 rebalanced_baskets[etf])
                           for etf in new_rebalances}
            print("-" * len(to_print))
            logger.info(f"Updated rebalances and prices: {new_baskets}")
            print("-" * len(to_print))
            print(to_print)
            print("-" * len(to_print))

            save_files(latest, prices)


async def multi_thread_this():
    tasks = await asyncio.gather(schedule_update(), get_updated_prices(cur, conn))


if __name__ == "__main__":
    etfs = update_etfs()
    logger.info(f"Initialising {len(etfs)} ETFs...")
    perp = [curr[:-2] for curr in etfs]
    latest, baskets = update_rebalances(etfs)
    file_rebalances, prices = read_files()
    new_rebalances = {
        etf: latest[etf] for etf in etfs
        if file_rebalances.get(etf, 0) != latest[etf]
    }

    baskets = dict(
        sorted(
            baskets.items(),
            key=lambda item: item[1],
            reverse=True))

    prices |= update_prices(new_rebalances)

    save_files(latest, prices)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(multi_thread_this())