# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:15:48 2020

@author: hongsong chou
"""

import threading
import os
import time
from collections import defaultdict
from multiprocessing import Queue

import pandas as pd

from QuantStrategy import QuantStrategy, SampleDummyStrategy
from uuid import uuid1
from datetime import datetime

from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Platform.OrderManager import OrderManager
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution
from typing import Mapping
from marketDataServiceConfig import MarketDataServiceConfig

class TradingPlatform:

    def __init__(self, marketData_2_platform_q, platform_2_exchSim_order_q,platform_2_futuresExchSim_order_q,
                 exchSim_2_platform_execution_q,isReady=None,debug=True):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))
        self.isReady = isReady
        self.debug = debug
        stockCodes = MarketDataServiceConfig.stockCodes
        futuresCodes = MarketDataServiceConfig.futureCodes
        self.qMapping:Mapping[str,Queue] = {stock:platform_2_exchSim_order_q for stock in stockCodes}
        self.qMapping.update({futures:platform_2_futuresExchSim_order_q for futures in futuresCodes})

        #Instantiate individual strategies
        # (1)
        #self.quantStrat = dict(str,QuantStrategy())
        self.orderManager = OrderManager(debug=debug)
        self.tickers2Snapshots: Mapping[str,list[pd.DataFrame]] = defaultdict(lambda: [])

        ######init strat
        strat = SampleDummyStrategy(
            stratID="dummy1",stratName="dummy1",stratAuthor="hongsongchou",day="20230706",
            ticker=["2610","3374"],tickers2Snapshots=self.tickers2Snapshots,
            orderManager=self.orderManager
        )

        self.quantStrats:Mapping[str,QuantStrategy] = {
            strat.getStratID():strat
        }

        #######maintain a mapping from ticker to strats for sending market data
        self.tickers2Strats:Mapping[str,list[QuantStrategy]] = defaultdict(lambda : [])
        for _,strat in self.quantStrats.items():
            if isinstance(strat.ticker,list):
                for t in strat.ticker:
                    self.tickers2Strats[t] += [strat]
            elif isinstance(strat.ticker,str):
                self.tickers2Strats[strat.ticker] += [strat]

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=( marketData_2_platform_q,))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q, ))
        t_exec.start()

    def loopUntilReady(self):
        if self.isReady is None: return
        while self.isReady.value==0:
            print("sleep for 3 secs")
            time.sleep(3)

    def consume_marketData(self, marketData_2_platform_q):
        print('[%d]Platform.consume_marketData' % (os.getpid(),))
        self.loopUntilReady()
        while True:
            res:OrderBookSnapshot_FiveLevels = marketData_2_platform_q.get()
            print('[%d] Platform.on_md' % (os.getpid()))
            print(res.outputAsDataFrame())

            ######updating tickers2Snapshots
            self.tickers2Snapshots[res.ticker].append(res.outputAsDataFrame())

            for strat in self.tickers2Strats[res.ticker]:
                stratOut = strat.run(None)
                if stratOut is None: continue ####
                if isinstance(stratOut,SingleStockOrder):
                    stratOut:list[SingleStockOrder] = [stratOut]
                for order in stratOut:
                    order.stratID = strat.getStratID()
                    if order.type == "CANCEL":
                        order.currStatus = "Cancelled"
                    self.orderManager.trackOrder(order)
                    self.qMapping.get(order.ticker).put(order)
            if self.debug: self.orderManager.displayOrders()
    
    def handle_execution(self, exchSim_2_platform_execution_q):
        print('[%d]Platform.handle_execution' % (os.getpid(),))
        self.loopUntilReady()
        while True:
            ##### BSJ ######
            obj = exchSim_2_platform_execution_q.get()
            if isinstance(obj,SingleStockOrder):
                # need a if-else to determine whether the obj.exchID is None
                print(f'Order {obj.orderID} has been validated by the exchange, the exchangeID is {obj.exOrderID}')
                self.orderManager.trackOrder(obj)
                if self.debug: self.orderManager.displayOrders()
            elif isinstance(obj,SingleStockExecution):
                execution: SingleStockExecution = obj

                ########update order status
                order = self.orderManager.lookupOrderID(execution.orderID)
                if order is None: continue
                if order.type in ["MO","LO"]:
                    order.price = execution.price
                order.filledSize += execution.size
                if order.filledSize < order.size:
                    order.currStatus = "PartiallyFilled"
                else:
                    order.currStatus = "Filled"
                self.orderManager.trackOrder(order)
                execution.order = order

                #########
                strat = self.quantStrats[order.stratID]
                stratOut = strat.run(execution)
                if stratOut is None: continue ####
                if isinstance(stratOut,SingleStockOrder):
                    stratOut:list[SingleStockOrder] = [stratOut]
                for order in stratOut:
                    order.stratID = strat.getStratID()
                    if order.type == "CANCEL":
                        order.currStatus = "Cancelled"
                    self.orderManager.trackOrder(order)
                    self.qMapping.get(order.ticker).put(order)
                #pass
            else:
                print('Wrong Message')
            ##### BSJ ######
            if self.debug: self.orderManager.displayOrders()
