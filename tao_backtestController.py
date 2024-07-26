# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

from multiprocessing import Process, Queue,Value
from marketDataService import MarketDataService
from futureDataService import FutureDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import time
from marketDataServiceConfig import MarketDataServiceConfig

import os
import random
import pandas as pd
from datetime import datetime, timedelta


def back_test(self, startDate, endDate, startTime,stockCodes,futuresCodes,playSpeed):
    
    stockCodes = MarketDataServiceConfig.stockCodes
    futuresCodes = MarketDataServiceConfig.futureCodes

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

    isReady = None#Value('i',0)

    Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q,startDate, endDate, startTime,stockCodes,playSpeed,isReady, )).start()
    Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, futureData_2_platform_q,isReady,startDate, endDate, startTime,futuresCodes,playSpeed,isReady,)).start()

    Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q,exchSim_2_platform_execution_q,stockCodes,isReady,True,)).start()
    Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q,exchSim_2_platform_execution_q,futuresCodes,isReady,True,)).start()


    Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q,platform_2_futuresExchSim_order_q,exchSim_2_platform_execution_q,isReady,True,)).start()
            

    return self.networth, self.timestamp
    


if __name__ == '__main__':
    worth = [1000000]
    timestamp = []

    

    




    
    
    ###########################################################################
    # Define all components
    ###########################################################################
    

    # FutureDataService(futureData_2_exchSim_q, futureData_2_platform_q)
    # isReady.value = int(input())
    # Process(name='sim', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, futureData_2_exchSim_q)).start()
    # Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, futureData_2_platform_q)).start()
