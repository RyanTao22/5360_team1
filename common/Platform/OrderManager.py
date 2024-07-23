from collections import defaultdict
from threading import Lock

from common.SingleStockOrder import SingleStockOrder
from typing import Mapping


class OrderManager:
    def __init__(self,debug=True):
        self.__orders:Mapping[str,SingleStockOrder] = defaultdict(lambda: None)
        self.lock = Lock()
        self.debug = debug


    def trackOrder(self,order: SingleStockOrder):
        self.lock.acquire()
        self.__orders[order.orderID] = order
        self.lock.release()

    def lookupOrderID(self,orderID,copy=True)->SingleStockOrder:
        order = self.__orders.get(orderID)
        if order is None: return None
        if copy: return order.copyOrder()
        return order

    def getAllOrderIDs(self): return self.__orders.keys()

    def displayOrders(self):
        if self.debug:
            self.lock.acquire()
            keys = self.__orders.keys()
            for orderID in keys:
                order = self.lookupOrderID(orderID)
                print(f"""
                orderID={orderID}
                exOrderID={order.exOrderID}
                type={order.type}
                currStatus={order.currStatus}
                price={order.price}
                size={order.size}
                filledSize={order.filledSize}
                """)
            self.lock.release()

