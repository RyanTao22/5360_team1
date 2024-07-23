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
marketData_2_exchSim_q = Queue()
marketData_2_platform_q = Queue()

platform_2_exchSim_order_q = Queue()
exchSim_2_platform_execution_q = Queue()

platform_2_strategy_md_q = Queue()
strategy_2_platform_order_q = Queue()
platform_2_strategy_execution_q = Queue()


def dfToSnapshot(df:pd.DataFrame,ticker):
    df = df.sort_index(axis=1)
    return [
        OrderBookSnapshot_FiveLevels(ticker, r1["date"], r1["time"],
                                             bidPrice=r1["BP1":"BP5"], askPrice=r1["SP1":"SP5"],
                                             bidSize=r1["BV1":"BV5"], askSize=r1["SV1":"SV5"])
        for _,r1 in df.iterrows()
    ]

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
    parent_dir = Path(__file__).parent
    df = pd.read_csv(join(parent_dir,'processedData_2024','stocks','2610_md_202404_202404.csv.gz'),nrows=20)
    snapshots = dfToSnapshot(df,"2610")


    exchSimProcess = setupExchSim()
    time.sleep(5)

    #########initialize with snapshot
    marketData_2_exchSim_q.put(snapshots[8])
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


    print()
