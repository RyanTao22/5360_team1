# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

from multiprocessing import Process, Queue,Value

from UnifiedDataService import UnifiedDataService
from marketDataService import MarketDataService
from futureDataService import FutureDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import time
from marketDataServiceConfig import MarketDataServiceConfig

if __name__ == '__main__':
    ###########################################################################
    # Define all components
    ###########################################################################
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

    analysis_q = Queue()


    isReady = None#Value('i',0)

    ##########v1
    # Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q,isReady, )).start()
    # Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, marketData_2_platform_q,isReady,)).start()
    #
    # Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q,exchSim_2_platform_execution_q,stockCodes,isReady,True,)).start()
    # Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q,exchSim_2_platform_execution_q,futuresCodes,isReady,True,)).start()

    # Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q,platform_2_futuresExchSim_order_q,exchSim_2_platform_execution_q,isReady,True,)).start()


    ##########v2
    # startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest = ('2024-06-28',
    #                                                                                                          '2024-06-28',
    #                                                                                                          #132315869,
    #                                                                                                          ['2610'],
    #                                                                                                          ['NEF1'],
    #                                                                                                          10000000,
    #                                                                                                          1000000,
    #                                                                                                          False,
    #                                                                                                          True)

    # Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, isReady)).start()
    # Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)).start()
    # Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)).start()
    # Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, futuresCodes, isReady, debug)).start()
    #
    # Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, isReady, debug)).start()

    #########v3
    startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest = ('2024-06-28',
                                                                                                             '2024-06-28',
                                                                                                             122015869,
                                                                                                            # 132315869,
                                                                                                             ['2610'],
                                                                                                             ['NEF1'],
                                                                                                             10000000,
                                                                                                             1000000,
                                                                                                             False,
                                                                                                             True)
    fds = FutureDataService(futureData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)
    mds = MarketDataService(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, isReady)
    Process(name='uds', target=UnifiedDataService, args=(mds,fds)).start()
    # Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)).start()
    Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)).start()
    Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, futuresCodes, isReady, debug)).start()

    Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, isReady, debug)).start()

    #############playground
    # fds = FutureDataService(futureData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)
    # mds = MarketDataService(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, isReady)
    # uds = UnifiedDataService(mds=mds,fds=fds)

    # isReady.value = int(input())
    # Process(name='sim', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, futureData_2_exchSim_q)).start()
    # Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, futureData_2_platform_q)).start()
