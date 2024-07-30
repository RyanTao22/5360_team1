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

#from QuantStrategy import QuantStrategy, InDevelopingStrategy
from QuantStrategy import QuantStrategy, SampleDummyStrategy, InDevelopingStrategy
from uuid import uuid1
from datetime import datetime

from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Platform.OrderManager import OrderManager
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution
from typing import Mapping
from marketDataServiceConfig import MarketDataServiceConfig

import logging
from pathlib import Path
from os.path import join

__today = datetime.today()
__today_str = __today.strftime("%Y-%m-%d")
parent_dir = Path(__file__).parent

#######setup logger
os.makedirs(join(parent_dir,"log"),exist_ok=True)
logfilename = join(parent_dir,"log",f"{Path(__file__).name}.{__today_str}.log")
logger = logging.getLogger(__file__)
formatter = logging.Formatter('%(processName)s::%(threadName)s::%(asctime)s::%(levelname)s::%(message)s')
handler = logging.FileHandler(logfilename)
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

class TradingPlatform:

    def __init__(self, marketData_2_platform_q, platform_2_exchSim_order_q,platform_2_futuresExchSim_order_q,
                 exchSim_2_platform_execution_q,stockCodes,futuresCodes,initial_cash,analysis_q,isReady=None,debug=True):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))
        self.recentUpeateTimestamp = None
        self.isReady = isReady
        self.debug = debug
        self.stockCodes_full = MarketDataServiceConfig.stockCodes
        self.futuresCodes_full = MarketDataServiceConfig.futuresCodes
        self.qMapping:Mapping[str,Queue] = {stock:platform_2_exchSim_order_q for stock in self.stockCodes_full}
        self.qMapping.update({futures:platform_2_futuresExchSim_order_q for futures in self.futuresCodes_full})

        #Instantiate individual strategies
        # (1)
        #self.quantStrat = dict(str,QuantStrategy())
        self.orderManager = OrderManager(debug=debug)
        self.tickers2Snapshots: Mapping[str,list[pd.DataFrame]] = {
            'stocks':defaultdict(lambda :[]),
            'futures_quotes':defaultdict(lambda: []),
            'futures_trades':defaultdict(lambda: []),
        }

        self.stockCodes = stockCodes
        self.futuresCodes = futuresCodes
        #####init strat
        strat = InDevelopingStrategy(
            stratID="coupula1",stratName="couplua1",stratAuthor="proj4",day="20260628",
            ticker=[self.stockCodes[0],self.futuresCodes[0]],tickers2Snapshots=self.tickers2Snapshots,
            orderManager=self.orderManager,
            initial_cash=initial_cash,analysis_queue=analysis_q            
        )
        # strat = SampleDummyStrategy(
        #     stratID="dummy2",stratName="dummy2",stratAuthor="hongsongchou",day="20230706",
        #     ticker=[self.stockCodes[0],self.futuresCodes[0]],tickers2Snapshots=self.tickers2Snapshots,
        #     orderManager=self.orderManager,
        #     initial_cash=initial_cash,analysis_queue=analysis_q
        # )

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

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=( marketData_2_platform_q,analysis_q,))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q, ))
        t_exec.start()

    def loopUntilReady(self):
        if self.isReady is None: return
        while self.isReady.value==0:
            print("sleep for 3 secs")
            time.sleep(3)

    def isStock(self,ticker ): return ticker in self.stockCodes_full
    def isFutures(self,ticker ): return ticker in self.futuresCodes_full
    def updateTickers2Snapshots(self,snap: OrderBookSnapshot_FiveLevels):
        if self.isStock(snap.ticker):
            self.tickers2Snapshots['stocks'][snap.ticker].append(snap.outputAsDataFrame())
        elif self.isFutures(snap.ticker) and snap.type == "quotes" :
            self.tickers2Snapshots['futures_quotes'][snap.ticker].append(snap.outputAsDataFrame())
        elif self.isFutures(snap.ticker) and snap.type == "trades" :
            self.tickers2Snapshots['futures_trades'][snap.ticker].append(snap.outputAsDataFrame())
        self.checkTickers2Snapshots()

    def checkTickers2Snapshots(self):
        logMsg = """
        """
        for ticker in self.stockCodes:
            size = len(self.tickers2Snapshots['stocks'][ticker])
            logMsg += f"""
            stock:{ticker}:{size} images"""
        for ticker in self.futuresCodes:
            size = len(self.tickers2Snapshots['futures_quotes'][ticker])
            logMsg += f"""
            futures_quotes:{ticker}:{size} images"""
            size = len(self.tickers2Snapshots['futures_trades'][ticker])
            logMsg += f"""
            futures_trades:{ticker}:{size} images"""
        logger.info(f"tickers2Snapshots: {logMsg}")


    def consume_marketData(self, marketData_2_platform_q,analysis_q):
        # print('[%d]Platform.consume_marketData' % (os.getpid(),))
        self.loopUntilReady()
        logger.info("Listen to market data")
        while True:
            res:OrderBookSnapshot_FiveLevels = marketData_2_platform_q.get()
            # print('[%d] Platform.on_md' % (os.getpid()))
            #print(res.outputAsDataFrame())

            
            '''判断数据是否都更新完成'''
            if res.ticker.endswith('_EndOfData'): 
                analysis_q.put({'signal':'EndOfData'})
                print('signal EndOfData----------------------------------------------------------------------------------------------------')

            ######updating tickers2Snapshots
            self.updateTickers2Snapshots(res)
            logger.info(f"Received Market Data {res.outputAsDataFrame()}")

            for strat in self.tickers2Strats[res.ticker]:
                stratOut = strat.run(None)
                if stratOut is None: 
                    print('get order')
                    continue ####
                if isinstance(stratOut,SingleStockOrder):
                    stratOut:list[SingleStockOrder] = [stratOut]
                for order in stratOut:
                    order.stratID = strat.getStratID()
                    # if order.type == "CANCEL":
                    #     order.currStatus = "Cancelled"
                    self.orderManager.trackOrder(order)
                    logger.info(f"""Sending Order: Market Data Update ---> 
                                {self.orderManager.lookupOrderID(order.orderID)}""")
                    self.qMapping.get(order.ticker).put(order)
            if self.debug:
                logger.info(f"{self.orderManager}")
                self.orderManager.displayOrders()
    
    def handle_execution(self, exchSim_2_platform_execution_q):
        # print('[%d]Platform.handle_execution' % (os.getpid(),))
        self.loopUntilReady()
        logger.info("Listen to execution")
        while True:
            obj = exchSim_2_platform_execution_q.get()
            if isinstance(obj,SingleStockOrder):
                print('get order')
                order = self.orderManager.lookupOrderID(obj.orderID)
                if obj.currStatus == "Rejected":
                    order.currStatus = "Rejected"
                    order.currStatusTime = datetime.now().time()
                    logger.info(f"Exchange rejected orderID={order.orderID}")
                if order.type in ("MO","LO"):
                    order.exOrderID = obj.exOrderID
                    logger.info(f"Exchange accepted orderID={order.orderID} with exOrderID={order.exOrderID}")
                    print(f'Order {order.orderID} has been validated by the exchange, the exchangeID is {obj.exOrderID}')
                self.orderManager.trackOrder(order)
                logger.info(f"Updated Order {self.orderManager.lookupOrderID(order.orderID)}")
                if self.debug:
                    self.orderManager.displayOrders()
                    logger.info(f"Current Order Manager Inventory {self.orderManager}")
            elif isinstance(obj,SingleStockExecution):
                execution: SingleStockExecution = obj

                ########update order status
                order = self.orderManager.lookupOrderID(execution.orderID)
                if order is None: continue
                logger.info(f"Received Execution {execution}")
                if execution.price is None and execution.size is None: ###successful Cancel Order
                    order = self.orderManager.lookupExOrderID(execution.exOrderID)
                    if order is not None:order.currStatus = "Cancelled"
                else:
                    if order.type in ("MO","LO"):
                        order.price = execution.price
                    order.filledSize += execution.size
                    if order.filledSize < order.size:
                        order.currStatus = "PartiallyFilled"
                    else:
                        order.currStatus = "Filled"
                order.currStatusTime = datetime.now().time()
                self.orderManager.trackOrder(order)
                logger.info(f"Updated Order {self.orderManager.lookupOrderID(order.orderID)}")
                execution.order = order

                #########sending strategy order to respective q
                strat = self.quantStrats[order.stratID]
                logger.info(f"Passing execID={execution.execID} to stratID={strat.getStratID()}")
                stratOut = strat.run(execution)
                if stratOut is None: continue ####
                if isinstance(stratOut,SingleStockOrder):
                    stratOut:list[SingleStockOrder] = [stratOut]
                for order in stratOut:
                    order.stratID = strat.getStratID()
                    # if order.type == "CANCEL":
                    #     order.currStatus = "Cancelled"
                    self.orderManager.trackOrder(order)
                    logger.info(f"Current Order Manager Inventory {self.orderManager}")
                    logger.info(f"""Sending Order: {execution}--->
                                {self.orderManager.lookupOrderID(order.orderID)}""")
                    self.qMapping.get(order.ticker).put(order)
                #pass
            else:
                print('Wrong Message')
            ##### BSJ ######
            if self.debug: self.orderManager.displayOrders()



class DummyTradingPlatform(TradingPlatform):
    def __init__(self, marketData_2_platform_q, platform_2_exchSim_order_q,platform_2_futuresExchSim_order_q,
                 exchSim_2_platform_execution_q,stockCodes,futuresCodes,initial_cash,analysis_q,isReady=None,debug=True):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))
        self.recentUpeateTimestamp = None
        self.isReady = isReady
        self.debug = debug
        self.stockCodes_full = MarketDataServiceConfig.stockCodes
        self.futuresCodes_full = MarketDataServiceConfig.futuresCodes
        self.qMapping:Mapping[str,Queue] = {stock:platform_2_exchSim_order_q for stock in self.stockCodes_full}
        self.qMapping.update({futures:platform_2_futuresExchSim_order_q for futures in self.futuresCodes_full})

        #Instantiate individual strategies
        # (1)
        #self.quantStrat = dict(str,QuantStrategy())
        self.orderManager = OrderManager(debug=debug)
        self.tickers2Snapshots: Mapping[str,list[pd.DataFrame]] = {
            'stocks':defaultdict(lambda :[]),
            'futures_quotes':defaultdict(lambda: []),
            'futures_trades':defaultdict(lambda: []),
        }

        self.stockCodes = stockCodes
        self.futuresCodes = futuresCodes
        #####init strat
        # strat = InDevelopingStrategy(
        #     stratID="dummy1",stratName="dummy1",stratAuthor="hongsongchou",day="20230706",
        #     ticker=[self.stockCodes[0],self.futuresCodes[0]],tickers2Snapshots=self.tickers2Snapshots,
        #     orderManager=self.orderManager,
        #     initial_cash=initial_cash,analysis_queue=analysis_q
        # )
        strat = SampleDummyStrategy(
            stratID="dummy2",stratName="dummy2",stratAuthor="hongsongchou",day="20230706",
            ticker=[self.stockCodes[0],self.futuresCodes[0]],tickers2Snapshots=self.tickers2Snapshots,
            orderManager=self.orderManager,
            initial_cash=initial_cash,analysis_queue=analysis_q
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

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData, args=( marketData_2_platform_q,analysis_q,))
        t_md.start()

        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution, args=(exchSim_2_platform_execution_q, ))
        t_exec.start()