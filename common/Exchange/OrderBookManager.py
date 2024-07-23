import threading
from abc import ABC,abstractmethod
from multiprocessing import Queue
from queue import PriorityQueue
from datetime import datetime
from typing import Mapping

import pandas as pd

# from common.Exchange.EmbeddedCommandOrder import EmbeddedCommandOrder
from common.SingleStockOrder import SingleStockOrder
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from uuid import uuid1
from common.SingleStockExecution import SingleStockExecution
from copy import deepcopy

class OrderBookManager:
    def __init__(self,ticker,debug=True):
        self.ticker = str(ticker)
        self.__initOrderBook()
        self.mgr_2_exchSim_q:Queue = None
        self.debug = debug
        self.lock = threading.Lock()


    def __initOrderBook(self):
        self.__bidQ = PriorityQueue()
        self.__askQ = PriorityQueue()
        self.__orders:Mapping[str,SingleStockOrder] = dict()
        self.__stopOrders:Mapping[str,SingleStockOrder] = dict()
        self.__lastTradePrice = None
        self.__lastTradeSize = None

    def displayOrderBook(self):
        if self.debug:
            self.lock.acquire()
            print(f"Ticker:{self.ticker}")
            print(f"lastTradePrice:{self.__lastTradePrice}")
            print(f"lastTradeSize:{self.__lastTradeSize}")
            bidQ_df = pd.DataFrame(self.__bidQ.queue,columns=['priority','time','order']).sort_values(['priority','time'],ascending=[True,True])
            askQ_df = pd.DataFrame(self.__askQ.queue,columns=['priority','time','order']).sort_values(['priority','time'],ascending=[False,False])
            print('-----Ask-----')
            for _,r in askQ_df.iterrows():
                order = r['order']
                if self.getAsk1().price == order.price:
                    print(f'Price=${order.price}:Size={order.size} <----Ask1')
                    continue
                print(f'Price=${order.price}:Size={order.size}')
            print('-----Bid-----')
            for _,r in bidQ_df.iterrows():
                order = r['order']
                if self.getBid1().price== order.price:
                    print(f'Price=${order.price}:Size={order.size} <----Bid1')
                    continue
                print(f'Price=${order.price}:Size={order.size}')
            print()
            self.lock.release()

    def overwriteOrderBook(self,snap: OrderBookSnapshot_FiveLevels):
        self.__initOrderBook()
        self.__updateLastTradePrice(None)
        now = datetime.now()
        for i in range(1,6):
            ######
            sellOrder = SingleStockOrder(
                ticker=self.ticker,
                date=now.date(),
                submissionTime=now.time(),
            )
            sellOrder.orderID = str(uuid1())
            sellOrder.price = getattr(snap,f'askPrice{i}')
            sellOrder.size = getattr(snap,f'askSize{i}')
            sellOrder.direction = -1
            sellOrder.type = "LO"
            self.addToAskQ(sellOrder, dt=now)

            ####
            buyOrder = SingleStockOrder(
                ticker=self.ticker,
                date=now.date(),
                submissionTime=now.time(),
            )
            buyOrder.orderID = str(uuid1())
            buyOrder.price = getattr(snap,f'bidPrice{i}')
            buyOrder.size = getattr(snap,f'bidSize{i}')
            buyOrder.direction = 1
            buyOrder.type = "LO"
            self.addToBidQ(buyOrder, dt=now)

    def addToBidQ(self, buyOrder: SingleStockOrder, dt):
        self.__bidQ.put((-buyOrder.price, dt, buyOrder))

    def addToAskQ(self, sellOrder:SingleStockOrder, dt):
        self.__askQ.put((sellOrder.price, dt, sellOrder))
    def getAskQ(self): return self.__askQ
    def getBidQ(self): return self.__bidQ
    def trackOrder(self, order: SingleStockOrder):
        self.__orders[order.exOrderID] = order

    def trackStopOrder(self, order: SingleStockOrder):
        self.__stopOrders[order.exOrderID] = order

    def cancelOrder(self, exOrderID):
        order = self.__orders.get(exOrderID)
        if order is not None: order.size = 0
        order = self.__stopOrders.get(exOrderID)
        if order is not None: order.size = 0
        self.__orders.pop(exOrderID,None)
        self.__stopOrders.pop(exOrderID,None)

    def isBidQEmpty(self):return self.__bidQ.empty()

    def isAskQEmpty(self):return self.__askQ.empty()

    def checkCross(self):
        bidQ = self.__bidQ
        askQ = self.__askQ
        while not(bidQ.empty()) and not(askQ.empty()):
            bid1 = self.getBid1()
            ask1 = self.getAsk1()

            #######no cross
            if bid1.price < ask1.price: return

            #######there is cross
            tradeSize = min(bid1.size,ask1.size)
            tradePrice = min(
                zip([bid1.submissionTime,ask1.submissionTime],[bid1.price,ask1.price]),
                key=lambda t: t[0]
            )[1]
            exB1 = self.__produceExecution(bid1, tradeSize,tradePrice)
            exS1 = self.__produceExecution(ask1, tradeSize,tradePrice)
            bid1.size -= tradeSize
            ask1.size -= tradeSize
            if tradeSize > 0:
                if self.debug:
                    print(f"""
                    executionID:{exB1.execID}
                    orderID:{exB1.orderID}
                    direction:{exB1.direction}
                    price:{exB1.price}
                    size:{exB1.size}
                    """)
                    print(f"""
                    executionID:{exS1.execID}
                    orderID:{exS1.orderID}
                    direction:{exS1.direction}
                    price:{exS1.price}
                    size:{exS1.size}
                    """)
                self.mgr_2_exchSim_q.put(exB1)
                self.mgr_2_exchSim_q.put(exS1)
                self.__updateLastTradePrice(tradePrice)
                self.__updateLastTradeSize(tradeSize)


            self.clean0SizeOrderFromQ(self.__askQ)
            self.clean0SizeOrderFromQ(self.__bidQ)

    def __updateLastTradeSize(self, tradeSize):
        self.__lastTradeSize = tradeSize
    def __updateLastTradePrice(self, tradePrice):
        self.__lastTradePrice = tradePrice
        # self.checkStopOrderTrigger()

    # def checkStopOrderTrigger(self):
    #     if self.__lastTradePrice is not None:
    #         for orderID in self.__stopOrders.keys():
    #             order:EmbeddedCommandOrder = self.__stopOrders[orderID]
    #             if (order.direction == 1) and (self.__lastTradePrice > order.price): ###buy stop order
    #                 #####create new market buy order
    #                 self.__stopOrders.pop(orderID,None)
    #                 order.command.execute(self)
    #             if (order.direction == -1) and (self.__lastTradePrice < order.price): ###buy stop order
    #                 ######create new market sell order
    #                 self.__stopOrders.pop(orderID,None)
    #                 order.command.execute(self)


    def getBest(self,pq:PriorityQueue) -> SingleStockOrder:
        return pq.queue[0][2]

    def getBid1(self) -> SingleStockOrder: return self.getBest(self.__bidQ)

    def getAsk1(self) -> SingleStockOrder: return self.getBest(self.__askQ)

    def __produceExecution(self, order: SingleStockOrder, tradeSize:int, tradePrice)->SingleStockExecution:
        now = datetime.now()
        ex = SingleStockExecution(
            ticker=order.ticker,
            date=now.date(),
            timeStamp=now.timestamp()
        )
        ex.execID = str(uuid1())
        ex.orderID = order.orderID
        ex.exOrderID = order.exOrderID
        ex.direction = order.direction
        ex.price = tradePrice
        ex.size = tradeSize
        return ex

    def clean0SizeOrderFromQ(self, pq:PriorityQueue=None):
        if pq is None:
            self.clean0SizeOrderFromQ(self.__askQ)
            self.clean0SizeOrderFromQ(self.__bidQ)
            return
        order = self.getBest(pq)
        while order.size == 0:
            pq.get()
            self.__orders.pop(order.exOrderID,None)
            order = self.getBest(pq)
        self.__orders = {k:v for k,v in self.__orders.items() if v.size > 0}
        self.__stopOrders = {k:v for k,v in self.__stopOrders.items() if v.size > 0}



