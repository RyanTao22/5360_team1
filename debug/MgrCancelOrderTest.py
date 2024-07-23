# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
from copy import deepcopy
from multiprocessing import Process, Queue

from common.Exchange.Command import OverwriteOrderBookCommand, MarketOrderCommand, LimitOrderCommand, CancelOrderCommand
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

    #######Case1: limit buy order at bid2 + cancel order + market order
    print("Case1[START]: limit buy order at bid2 + cancel order + market order")
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
    buyOrder.price = 1945
    buyOrder.size = 150
    buyOrder.direction = 1
    buyOrder.type = "LO"
    cmd = LimitOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    ###########cancel order
    cancelOrder = deepcopy(buyOrder)
    buyOrder.type = "CANCEL"
    cmd = CancelOrderCommand(cancelOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    ##############market sell order
    now = datetime.now()
    sellOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    sellOrder.orderID = str(uuid1())
    sellOrder.size = 268
    sellOrder.direction = -1
    sellOrder.type = "MO"
    cmd = MarketOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    print()
    print("Case1[END]: limit buy order at bid2 + cancel order + market order")


    #######Case2: limit sell order at ask2 + cancel order + market order
    print("Case2[START]: limit sell order at ask2 + cancel order + market order")
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
    sellOrder.price = 1960
    sellOrder.size = 20
    sellOrder.direction = -1
    sellOrder.type = "LO"
    cmd = LimitOrderCommand(sellOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()
    ###########cancel order
    cancelOrder = deepcopy(sellOrder)
    sellOrder.type = "CANCEL"
    cmd = CancelOrderCommand(cancelOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    ##############market buy order
    now = datetime.now()
    buyOrder = SingleStockOrder(
        ticker=ticker,
        date=now.date(),
        submissionTime=now.time(),
    )
    buyOrder.orderID = str(uuid1())
    buyOrder.size = 144#154
    buyOrder.direction = 1
    buyOrder.type = "MO"
    cmd = MarketOrderCommand(buyOrder)
    cmd.execute(mgr)

    mgr.displayOrderBook()

    print()
    print("Case2[END]: limit sell order at ask2 + cancel order + market order")
