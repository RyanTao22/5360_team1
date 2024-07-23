# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
from copy import deepcopy
from multiprocessing import Process, Queue

from common.Exchange.Command import OverwriteOrderBookCommand, MarketOrderCommand, LimitOrderCommand
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
import time
from datetime import datetime
from uuid import uuid1
def dfToSnapshot(df:pd.DataFrame,ticker):
    df = df.sort_index(axis=1)
    return [
        OrderBookSnapshot_FiveLevels(ticker, r1["date"], r1["time"],
                                             bidPrice=r1["BP1":"BP5"], askPrice=r1["SP1":"SP5"],
                                             bidSize=r1["BV1":"BV5"], askSize=r1["SV1":"SV5"])
        for _,r1 in df.iterrows()
    ]

if __name__ == '__main__':
    ###########################################################################
    # Define all components
    ###########################################################################
    marketData_2_exchSim_q = Queue()
    marketData_2_platform_q = Queue()

    platform_2_exchSim_order_q = Queue()
    exchSim_2_platform_execution_q = Queue()

    platform_2_strategy_md_q = Queue()
    strategy_2_platform_order_q = Queue()
    platform_2_strategy_execution_q = Queue()

    ticker = "2610"


    ###########read some snapshot data
    parent_dir = Path(__file__).parent
    df = pd.read_csv(join(parent_dir,'processedData_2024','stocks','2610_md_202404_202404.csv.gz'),nrows=20)
    snapshots = dfToSnapshot(df,ticker)


    #######create mgr for 2610
    mgr = OrderBookManager(
        ticker=ticker
    )
    mgr.mgr_2_exchSim_q = Queue()

    #######Case1: limit buy order that immediately produce executions
    print("Case1[START]: limit buy order that immediately produce executions")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.price = 1961
    buyOrder.size = 150
    buyOrder.direction = 1
    buyOrder.type = "LO"
    cmd = LimitOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case1[END]: limit buy order that immediately produce executions")

    #######Case2: limit buy order same as Bid1 followed by market sell order
    print("Case2[START]: limit buy order same as Bid1 followed by market sell order")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.price = 1950
    buyOrder.size = 20
    buyOrder.direction = 1
    buyOrder.type = "LO"
    cmd = LimitOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    #######followed by market order which
    now = datetime.now()
    sellOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    sellOrder.orderID = str(uuid1())
    sellOrder.size = 34
    sellOrder.direction = -1
    sellOrder.type = "MO"
    cmd = MarketOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()


    sellOrder2 = deepcopy(sellOrder)
    sellOrder2.orderID = str(uuid1())
    sellOrder2.size = 120
    cmd = MarketOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()


    print()
    print("Case2[END]: limit buy order same as Bid1 followed by market sell order")




    #######Case3: limit sell order that immediately produce executions
    print("Case3[START]: limit sell order that immediately produce executions")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.price = 1944
    buyOrder.size = 278
    buyOrder.direction = -1
    buyOrder.type = "LO"
    cmd = LimitOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case3[END]: limit sell order that immediately produce executions")

    #######Case4: limit sell order same as Bid1 followed by market buy order
    print("Case4[START]: limit sell order same as Bid1 followed by market buy order")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.price = 1955
    buyOrder.size = 30
    buyOrder.direction = -1
    buyOrder.type = "LO"
    cmd = LimitOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    #######followed by market order which
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.size = 10
    buyOrder.direction = 1
    buyOrder.type = "MO"
    cmd = MarketOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()


    buyOrder2 = deepcopy(buyOrder)
    buyOrder2.orderID = str(uuid1())
    buyOrder2.size = 20
    cmd = MarketOrderCommand(buyOrder2)
    cmd.execute(mgr)

    mgr.displayOrderBook()


    print()
    print("Case4[END]: limit sell order same as Bid1 followed by market buy order")

