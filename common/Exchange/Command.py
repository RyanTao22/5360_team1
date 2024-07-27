from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock

# from common.Exchange.EmbeddedCommandOrder import EmbeddedCommandOrder
from common.Exchange.OrderBookManager import OrderBookManager
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution


class Command(ABC):

    @abstractmethod
    def execute(self,mgr:OrderBookManager): pass


class MarketDataCommand(Command):
    def __init__(self, snap: OrderBookSnapshot_FiveLevels):
        self.snap = snap

    def execute(self,mgr: OrderBookManager):
        mgr.overwriteOrderBook(self.snap)
        if self.snap.type in ("both"): ###stock
            mgr.overwriteOrderBook(self.snap)
            mgr.updateLastTradeSize(self.snap.size)
            mgr.updateLastTradePrice(self.snap.lastPx)
        elif self.snap.type in ("quotes"): ###futures quotes data
            mgr.overwriteOrderBook(self.snap)
        elif self.snap.type in ("trades"):
            mgr.updateLastTradeSize(self.snap.totalMatchSize)
            mgr.updateLastTradePrice(self.snap.totalMatchValue)

class OrderCommand(Command):
    def __init__(self, order: SingleStockOrder):
        self.order = order.copyOrder()

    def execute(self,mgr:OrderBookManager):
        if self.order.direction == 1:
            self.handleBuyOrder(mgr)
        else:
            self.handleSellOrder(mgr)

    @abstractmethod
    def handleBuyOrder(self,mgr:OrderBookManager):
        pass

    @abstractmethod
    def handleSellOrder(self,mgr:OrderBookManager):
        pass


class CancelOrderCommand(OrderCommand):
    def execute(self,mgr:OrderBookManager):
        order = self.order
        mgr.cancelOrder(order.exOrderID)
        mgr.clean0SizeOrderFromQ()

    def handleBuyOrder(self,mgr:OrderBookManager): return

    def handleSellOrder(self,mgr:OrderBookManager): return


class MarketOrderCommand(OrderCommand):
    def handleBuyOrder(self,mgr:OrderBookManager):
        order = self.order
        size = order.size
        while size > 0 and not (mgr.isAskQEmpty()):
            ask1 = mgr.getAsk1()
            tradeSize = min(size, ask1.size)
            splitOrder: SingleStockOrder = order.copyOrder()
            splitOrder.price = ask1.price
            splitOrder.size = tradeSize
            mgr.trackOrder(splitOrder)
            mgr.addToBidQ(splitOrder,dt=datetime.now())
            mgr.checkCross()
            size -= tradeSize

    def handleSellOrder(self,mgr:OrderBookManager):
        order = self.order
        size = order.size
        while size > 0 and not (mgr.isBidQEmpty()):
            bid1 = mgr.getBid1()
            tradeSize = min(size, bid1.size)
            splitOrder: SingleStockOrder = order.copyOrder()
            splitOrder.price = bid1.price
            splitOrder.size = tradeSize
            mgr.trackOrder(splitOrder)
            mgr.addToAskQ(splitOrder,dt=datetime.now())
            mgr.checkCross()
            size -= tradeSize

class LimitOrderCommand(OrderCommand):
    def handleBuyOrder(self,mgr:OrderBookManager):
        order = self.order
        mgr.trackOrder(order)
        mgr.addToBidQ(order,dt=datetime.now())
        mgr.checkCross()

    def handleSellOrder(self,mgr:OrderBookManager):
        order = self.order
        mgr.trackOrder(order)
        mgr.addToAskQ(order,dt=datetime.now())
        mgr.checkCross()

# class StopOrderCommand(OrderCommand):
#     def handleBuyOrder(self,mgr:OrderBookManager):
#         cmd = MarketOrderCommand(self.order)
#         neworder = EmbeddedCommandOrder(self.order,cmd)
#         mgr.trackStopOrder(neworder)
#
#
#     def handleSellOrder(self,mgr:OrderBookManager):
#         self.handleBuyOrder(mgr)
#
# class StopLimitOrderCommand(OrderCommand):
#     def handleBuyOrder(self,mgr:OrderBookManager):
#         cmd = LimitOrderCommand(self.order)
#
#         neworder = EmbeddedCommandOrder(self.order,cmd)
#         mgr.trackStopOrder(neworder)
#
#
#     def handleSellOrder(self,mgr:OrderBookManager):
#         self.handleBuyOrder(mgr)

