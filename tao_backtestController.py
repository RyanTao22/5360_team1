# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

from multiprocessing import Process, Queue, Manager
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


# def back_test(startDate, endDate, startTime,stockCodes,futuresCodes,playSpeed):
def back_test():
    
    stockCodes = ['0050', '2392', '2498', '2610', '2618', '3035', '3264', '3374', '5347', '6443']
    futuresCodes = ['DBF1', 'GLF1', 'HCF1', 'HSF1', 'IPF1', 'NEF1', 'NLF1', 'NYF1', 'QLF1', 'RLF1']
    endDate = '2024-06-28'
    startDate = '2024-06-28'
    startTime = 90515869
    playSpeed = 0.2
    initial_cash = 1000000.0
    debug = False

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
    '''
    self.analysis_q.put({
                'timestamp': ticker1RecentMarketData['time'],
                'cash': self.cash[-1],
                'networth': netWrorth,
                'positions_'+ticker1: self.positions[ticker1][-1],
                'positions_'+ticker2: self.positions[ticker2][-1],
                'midPrice_'+ticker1: self.midPrices[ticker1][-1],
                'midPrice_'+ticker2: self.midPrices[ticker2][-1]
    })'''

    isReady = None#Value('i',0)

    Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime,stockCodes,playSpeed,isReady, )).start()
    Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, futureData_2_platform_q, startDate, endDate, startTime,futuresCodes,playSpeed, isReady,)).start()

    Process(name='stockExchange', target=ExchangeSimulator,  args=(marketData_2_exchSim_q, platform_2_exchSim_order_q,       exchSim_2_platform_execution_q,stockCodes,  isReady,debug,)).start()
    Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q,exchSim_2_platform_execution_q,futuresCodes,isReady,debug,)).start()
                                                                   

    #Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, isReady,True,)).start()
    Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, isReady, debug,)).start()       

    
    


if __name__ == '__main__':
    worth = [1000000]
    timestamp = []
    
    
    back_test()
    # while shared_analysis_list != []:
    #     print(shared_analysis_list[-1])
    #     time.sleep(1)