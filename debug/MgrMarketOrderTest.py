# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

from multiprocessing import Process, Queue

from common.Exchange.Command import OverwriteOrderBookCommand, MarketOrderCommand
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

    #######Case1: market buy order that produce multiple executions
    print("Case1[START]: market buy order that produce multiple executions")
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
    buyOrder.size = 150
    buyOrder.direction = 1
    buyOrder.type = "MO"
    cmd = MarketOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case1[END]: market buy order that produce multiple executions")


    #######Case2: market buy order that produce multiple executions
    print("Case2[START]: market buy order that produce single executions")
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
    buyOrder.size = 22
    buyOrder.direction = 1
    buyOrder.type = "MO"
    cmd = MarketOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case2[END]: market buy order that produce single executions")




    #######Case3: market sell order that produce multiple executions
    print("Case3[START]: market sell order that produce multiple executions")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    sellOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    sellOrder.orderID = str(uuid1())
    sellOrder.size = 148
    sellOrder.direction = -1
    sellOrder.type = "MO"
    cmd = MarketOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case3[END]: market sell order that produce multiple executions")


    #######Case4: market sell order that produce multiple executions
    print("Case4[START]: market sell order that produce single executions")
    cmd = OverwriteOrderBookCommand(snapshots[8])
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()

    #####
    now = datetime.now()
    sellOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    sellOrder.orderID = str(uuid1())
    sellOrder.size = 144
    sellOrder.direction = -1
    sellOrder.type = "MO"
    cmd = MarketOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    print()
    print("Case4[END]: market sell order that produce single executions")
