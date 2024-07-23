#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
import datetime
import os
import time
from typing import Mapping
from uuid import uuid1

import pandas as pd

from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Platform.OrderManager import OrderManager
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution
from datetime import datetime
class QuantStrategy(Strategy):
    
    def __init__(self, stratID, stratName, stratAuthor, ticker, day):
        super(QuantStrategy, self).__init__(stratID, stratName, stratAuthor) #call constructor of parent
        self.ticker = ticker #public field
        self.day = day #public field
        
    def getStratDay(self):
        return self.day
    
    def run(self, marketData, execution):
        if (marketData is None) and (execution is None):
            return None
        elif (marketData is None) and ((execution is not None) and (isinstance(execution, SingleStockExecution))):
            #handle executions
            print('[%d] Strategy.handle_execution' % (os.getpid()))
            print(execution.outputAsArray())
            return None
        elif ((marketData is not None) and (isinstance(marketData, OrderBookSnapshot_FiveLevels))) and (execution is None):
            #handle new market data, then create a new order and send it via quantTradingPlatform.
            return SingleStockOrder('testTicker','2019-07-05',time.asctime(time.localtime(time.time())))
        else:
            return None
                

class SampleDummyStrategy(QuantStrategy):
    def __init__(self, stratID, stratName, stratAuthor, ticker, day,
                 tickers2Snapshots: Mapping[str,OrderBookSnapshot_FiveLevels],
                 orderManager:OrderManager):
        super().__init__(stratID,stratName,stratAuthor,ticker,day)
        self.tickers2Snapshots = tickers2Snapshots
        self.orderManager = orderManager

    def run(self,execution:SingleStockExecution)->list[SingleStockOrder]:
        if execution is None:#######on receive market data
            ####get most recent market data for ticker
            ticker1 = "2610"
            ticker2 = "3374"
            ticker1MarketData:list[pd.DataFrame] = self.tickers2Snapshots[ticker1]
            ticker2MarketData:list[pd.DataFrame] = self.tickers2Snapshots[ticker2]
            ticker1RecentMarketData = None
            ticker2RecentMarketData = None

            if len(ticker1MarketData) > 0:
                ticker1RecentMarketData = ticker1MarketData[-1]

            if len(ticker2MarketData) > 0:
                ticker2RecentMarketData = ticker2MarketData[-1]

            if ticker1RecentMarketData is not None and ticker2RecentMarketData is not None:
                #########do some calculation with the recent market data
                #.....
                from datetime import datetime
                now = datetime.now()
                sampleOrder1 = SingleStockOrder(
                    ticker="2610",
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder1.orderID = f"{self.getStratID()}-2610-{str(uuid1())}"
                sampleOrder1.type = "MO"
                sampleOrder1.currStatus = "New"
                sampleOrder1.currStatusTime = now.time()
                sampleOrder1.direction = 1
                sampleOrder1.size = 1
                sampleOrder1.stratID = self.getStratID()

                sampleOrder2 = SingleStockOrder(
                    ticker="3374",
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder2.orderID = f"{self.getStratID()}-3374-{str(uuid1())}"
                sampleOrder2.type = "LO"
                sampleOrder2.currStatus = "New"
                sampleOrder2.currStatusTime = now.time()
                sampleOrder2.direction = 1###1 = buy; -1 = sell
                sampleOrder2.size = 2
                sampleOrder2.price = ticker2RecentMarketData['bidPrice5'].item()
                sampleOrder2.stratID = self.getStratID()


                ######return a list
                return [sampleOrder1,sampleOrder2]
        else:
            #######on receive execution
            order = self.orderManager.lookupOrderID(execution.orderID)

            ######do something

            ####e.g. issue cancel order
            # if order.type == "LO":
            #     order.type = "CANCEL"
            return []



        return []

