# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
from copy import deepcopy
from multiprocessing import Process, Queue

from common.Exchange.Command import MarketOrderCommand, LimitOrderCommand, CancelOrderCommand
from common.Exchange.OrderBookManager import OrderBookManager
from common.SingleStockOrder import SingleStockOrder
from marketDataService import MarketDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import pandas as pd
from os.path import join
from pathlib import Path
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from marketDataServiceConfig import MarketDataServiceConfig

import time
from datetime import datetime
from uuid import uuid1
marketData_2_exchSim_q = Queue()
marketData_2_platform_q = Queue()

platform_2_exchSim_order_q = Queue()
exchSim_2_platform_execution_q = Queue()

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

def setupExchSim():

    #######create mgr for 2610
    tickers = ["2610"]
    mgrs = [OrderBookManager(ticker=ticker) for ticker in tickers]

    p = Process(name='sim', target=ExchangeSimulator,
                args=(marketData_2_exchSim_q,
                      platform_2_exchSim_order_q,
                      exchSim_2_platform_execution_q,
                      ))
    p.start()

    return p







if __name__ == '__main__':
    ###########################################################################
    # Define all components
    ###########################################################################

    ###########read some snapshot data
    parent_dir = Path(__file__).parent.parent
    dfStocks = pd.read_pickle('./stocksData.1000.pkl').reset_index(drop=True)
    snapshots1 = dfToSnapshotStocks(dfStocks)
    stockCodes = MarketDataServiceConfig.stockCodes
    isReady = None
    startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest,resampleFreq = ('2024-06-28',
                                                                                                             '2024-06-28',
                                                                                                             90000000,
                                                                                                            #  122015869,
                                                                                                            # 132315869,
                                                                                                             stockCodes,
                                                                                                             ['HSF1'],
                                                                                                             10000000,
                                                                                                             1000000,
                                                                                                             False,
                                                                                                             True,
                                                                                                             '1s')

    exch = ExchangeSimulator(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)
    # Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)).start()

    #########initialize with snapshot
    marketData_2_exchSim_q.put(snapshots1[103])
    time.sleep(1)


    ########
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker="2610",
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.size = 150
    buyOrder.direction = 1
    buyOrder.type = "MO"
    platform_2_exchSim_order_q.put(buyOrder)


    ########
    now = datetime.now()
    sellOrder = SingleStockOrder(
        ticker="2610",
        date=now.date(),
        submissionTime=now.time(),
    )
    sellOrder.orderID = str(uuid1())
    sellOrder.size = 50
    sellOrder.price = 2415
    sellOrder.direction = -1
    sellOrder.type = "LO"
    platform_2_exchSim_order_q.put(sellOrder)
    while True:
        obj = exchSim_2_platform_execution_q.get()
        if isinstance(obj,SingleStockOrder) and obj.type == "LO":
            lo = obj
            break


    ########
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker="2610",
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.size = 50
    buyOrder.direction = 1
    buyOrder.type = "MO"
    platform_2_exchSim_order_q.put(buyOrder)


    ########
    now = datetime.now()
    cancelOrder = SingleStockOrder(
        ticker="2610",
        date=now.date(),
        submissionTime=now.time(),
    )
    cancelOrder.orderID = lo.orderID
    cancelOrder.exOrderID = lo.exOrderID
    cancelOrder.type = "CANCEL"
    platform_2_exchSim_order_q.put(cancelOrder)

    print()
