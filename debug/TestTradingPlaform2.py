# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
from copy import deepcopy
from multiprocessing import Process, Queue

from common.Exchange.Command import MarketDataCommand, MarketOrderCommand, LimitOrderCommand, CancelOrderCommand
from common.Exchange.OrderBookManager import OrderBookManager
from common.SingleStockExecution import SingleStockExecution
from common.SingleStockOrder import SingleStockOrder
from marketDataService import MarketDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform, DummyTradingPlatform
import pandas as pd
from os.path import join
from pathlib import Path
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
import time
from datetime import datetime
from uuid import uuid1
marketData_2_exchSim_q = Queue()
marketData_2_platform_q = Queue()

futureData_2_exchSim_q = Queue()
futureData_2_platform_q = Queue()

platform_2_exchSim_order_q = Queue()
exchSim_2_platform_execution_q = Queue()

platform_2_futuresExchSim_order_q = Queue()

platform_2_strategy_md_q = Queue()
strategy_2_platform_order_q = Queue()
platform_2_strategy_execution_q = Queue()
analysis_q = Queue()


def dfToSnapshotStocks(cData:pd.DataFrame):
    snapshots =[]
    for index, row in cData.iterrows():
        now = datetime.now()
        quoteSnapshot = OrderBookSnapshot_FiveLevels(row.ticker, now.date(), now.time(),
                                                     bidPrice=row["BP1":"BP5"].tolist(),
                                                     askPrice=row["SP1":"SP5"].tolist(),
                                                     bidSize=row["BV1":"BV5"].tolist(),
                                                     askSize=row["SV1":"SV5"].tolist())
        quoteSnapshot.type = "both"
        quoteSnapshot.midQ = row.get("midQ")
        quoteSnapshot.symbol = row.get("symbol")
        quoteSnapshot.totalMatchSize = row.get("totalMatchSize")
        quoteSnapshot.totalMatchValue = row.get("totalMatchValue")
        quoteSnapshot.avgMatchPx = row.get("avgMatchPx")
        quoteSnapshot.size = row.get("size")
        quoteSnapshot.volume = row.get("volume")
        quoteSnapshot.lastPx = row.get("lastPx")
        snapshots.append(quoteSnapshot)
    return snapshots

def dfToSnapshotFutures(tqcData:pd.DataFrame):
    snapshots =[]
    for index, row in tqcData.iterrows():
        now = datetime.now()
        quoteSnapshot = OrderBookSnapshot_FiveLevels(row.ticker, now.date(), now.time(),
                                                     bidPrice=row["bidPrice1":"bidPrice5"].tolist(),
                                                     askPrice=row["askPrice1":"askPrice5"].tolist(),
                                                     bidSize=row["bidSize1":"bidSize5"].tolist(),
                                                     askSize=row["askSize1":"askSize5"].tolist())
        quoteSnapshot.type = row.get("type")
        quoteSnapshot.midQ = row.get("midQ")
        quoteSnapshot.symbol = row.get("symbol")
        quoteSnapshot.totalMatchSize = row.get("totalMatchSize")
        quoteSnapshot.totalMatchValue = row.get("totalMatchValue")
        quoteSnapshot.avgMatchPx = row.get("avgMatchPx")
        quoteSnapshot.size = row.get("size")
        quoteSnapshot.volume = row.get("volume")
        quoteSnapshot.lastPx = row.get("lastPx")
        snapshots.append(quoteSnapshot)
    return snapshots


def setupTradingPlatform():

    #######create mgr for 2610
    # tickers = ["2610"]
    # mgrs = [OrderBookManager(ticker=ticker) for ticker in tickers]
    p = Process(name='sim', target=TradingPlatform,
                args=(marketData_2_platform_q,
                      platform_2_exchSim_order_q,
                      platform_2_futuresExchSim_order_q,
                      exchSim_2_platform_execution_q,
                      None,
                      True,))
    p.start()

    return p





if __name__ == '__main__':
    ###########################################################################
    # Define all components
    ###########################################################################

    ###########read some snapshot data
    parent_dir = Path(__file__).parent
    dfStocks = pd.read_pickle('./stocksData.1000.pkl').reset_index(drop=True)
    dfFutures = pd.read_pickle('./futuresData.1000.pkl').reset_index(drop=True)
    snapshots1 = dfToSnapshotStocks(dfStocks)
    snapshots2 = dfToSnapshotFutures(dfFutures)

    startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest,resampleFreq = ('2024-06-28',
                                                                                                             '2024-06-28',
                                                                                                             90000000,
                                                                                                            #  122015869,
                                                                                                            # 132315869,
                                                                                                             ['2610'],
                                                                                                             ['NEF1'],
                                                                                                             10000000,
                                                                                                             1000000,
                                                                                                             False,
                                                                                                             True,
                                                                                                             '1s')
    # exchSimProcess = setupTradingPlatform()
    platform = DummyTradingPlatform(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, None, debug)


    ###########
    marketData_2_platform_q.put(snapshots1[103])
    time.sleep(1)


    futures_quotes1 = snapshots2[0]
    futures_trades1 = snapshots2[11]
    marketData_2_platform_q.put(futures_quotes1)
    time.sleep(1)

    ##################MO Flow
    #########send back order with exOrderID
    order:SingleStockOrder = platform_2_exchSim_order_q.get()
    order.exOrderID = str(uuid1())
    exchSim_2_platform_execution_q.put(order)

    #########send back execution
    time.sleep(0.5)
    now = datetime.now()
    ex = SingleStockExecution(
            ticker=order.ticker,
            date=now.date(),
            timeStamp=now.timestamp()
    )
    ex.execID = str(uuid1())
    ex.orderID = order.orderID
    ex.exOrderID = order.exOrderID
    ex.direction = order.direction
    ex.price = 999
    ex.size = 1
    exchSim_2_platform_execution_q.put(ex)

    #################LO Flow
    #########send back order with exOrderID
    order:SingleStockOrder = platform_2_futuresExchSim_order_q.get()
    order.exOrderID = str(uuid1())
    exchSim_2_platform_execution_q.put(order)

    #########send back execution
    time.sleep(0.5)
    now = datetime.now()
    ex = SingleStockExecution(
            ticker=order.ticker,
            date=now.date(),
            timeStamp=now.timestamp()
    )
    ex.execID = str(uuid1())
    ex.orderID = order.orderID
    ex.exOrderID = order.exOrderID
    ex.direction = order.direction
    ex.price = order.price
    ex.size = 1
    exchSim_2_platform_execution_q.put(ex)


    ########Cancel Order Flow
    order:SingleStockOrder = platform_2_futuresExchSim_order_q.get()
    exchSim_2_platform_execution_q.put(order)
    now = datetime.now()
    ex = SingleStockExecution(
        ticker=order.ticker,
        date=now.date(),
        timeStamp=now.timestamp()
    )
    ex.execID = str(uuid1())
    ex.orderID = order.orderID
    ex.exOrderID = order.exOrderID
    ex.direction = order.direction
    ex.price = None
    ex.size = None
    exchSim_2_platform_execution_q.put(ex)
    print()
