# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2020

@author: hongsong chou
"""

import threading
import os
import time
from uuid import uuid1

from common.Exchange.Command import Command, OverwriteOrderBookCommand, MarketOrderCommand, LimitOrderCommand, \
    CancelOrderCommand, OrderCommand
from common.Exchange.OrderBookManager import OrderBookManager
from common.SingleStockExecution import SingleStockExecution
from threading import Lock
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels 
from common.SingleStockOrder import SingleStockOrder 
from abc import ABC,abstractmethod
from queue import PriorityQueue
from multiprocessing import Queue
from typing import List



class ExchangeSimulator:
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q,
                 tickers=["2610"],isReady=None,debug=False):
        # print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        self.isReady = isReady
        self.debug = debug
        mgrs = self.mgrs = [
            OrderBookManager(ticker=ticker,debug=self.debug) for ticker in tickers
        ]
        mgr_2_exchSim_q = Queue()
        self.mgrBundle = {
            mgr.ticker:(mgr,Queue()) for mgr in mgrs
        }

        self.supportedOrderCommand = {
            'MO':MarketOrderCommand,
            'LO':LimitOrderCommand,
            'CANCEL':CancelOrderCommand
        }

        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q,))

        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q,))

        t_exec = threading.Thread(name='exchsim.on_exec', target=self.produce_execution, args=(mgr_2_exchSim_q,exchSim_2_platform_execution_q, ))

        self.t_mgrs = dict()
        for ticker,bundle in self.mgrBundle.items():
            mgr,commandQ = bundle
            mgr.mgr_2_exchSim_q = mgr_2_exchSim_q
            t_mgr = threading.Thread(name=f'{mgr.ticker}.processCommand',
                                     target=self.processCommand,
                                     args=(mgr,commandQ))
            self.t_mgrs[mgr.ticker] = t_mgr
        t_md.start()
        t_order.start()
        t_exec.start()


    def loopUntilReady(self):
        if self.isReady is None: return
        while self.isReady.value==0:
            print("sleep for 3 secs")
            time.sleep(3)

    def processCommand(self,mgr:OrderBookManager,commandQ: Queue):
        self.loopUntilReady()
        while True:
#             print('processCommand',commandQ.qsize())
            command:Command = commandQ.get()
#             print(f'[{os.getpid()}]{mgr.ticker}.processCommand' )
            command.execute(mgr)
            mgr.displayOrderBook()


    def consume_md(self, marketData_2_exchSim_q):
        for _,t in self.t_mgrs.items(): t.start()
        self.loopUntilReady()
        while True:
#             print('[%d]ExchSim.consume_md' % (os.getpid()))
            res:OrderBookSnapshot_FiveLevels = marketData_2_exchSim_q.get()
#             # print(res.outputAsDataFrame())
            ticker = str(res.ticker)
            mgr,commandQ = self.mgrBundle.get(ticker)
            cmd = OverwriteOrderBookCommand(res)
            commandQ.put(cmd)

    def validateOrder(self,order:SingleStockOrder):
        # if order.type in ("MO","LO"):
        #     if order.size <= 0: return False
        #     if order.price < 0: return False
        # if order.type in ("CANCEL"):

        return True
    
    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q:Queue):
        self.loopUntilReady()
        while True:
#             print('[%d]ExchSim.on_order' % (os.getpid()))
            res:SingleStockOrder = platform_2_exchSim_order_q.get()

            ##############check order validity
            if not(self.validateOrder(res)):continue ###if not validate order skip the below
            if res.type in ("MO","LO"): ##if valid, send back the order with orderID
                res.exOrderID = str(uuid1())
                exchSim_2_platform_execution_q.put(res)

#             # print(res.outputAsArray())
            ticker = str(res.ticker)
            mgr,commandQ = self.mgrBundle.get(ticker)
            cmdCls = self.supportedOrderCommand.get(res.type)
            if cmdCls is not None:
                cmd = cmdCls(res)
                commandQ.put(cmd)

    def produce_execution(self, mgr_2_exchSim:Queue, exchSim_2_platform_execution_q):
        self.loopUntilReady()
        while True:
#             print('[%d]ExchSim.produce_execution' % (os.getpid()))
            execution:SingleStockExecution = mgr_2_exchSim.get()
            #########supposingly we need to maintain a list of trading platforms

            exchSim_2_platform_execution_q.put(execution)
#             print(execution.outputAsArray())

