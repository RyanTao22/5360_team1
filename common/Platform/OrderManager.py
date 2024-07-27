from collections import defaultdict
from threading import Lock

from common.SingleStockOrder import SingleStockOrder
from typing import Mapping


class OrderManager:
    def __init__(self,debug=True):
        self.__orders:Mapping[str,SingleStockOrder] = defaultdict(lambda: None)
        self.lock = Lock()
        self.debug = debug
        self.__exOrderID2OrderID: Mapping[str,str] = defaultdict(lambda: None)


    def lookupExOrderID(self,exOrderID,copy=True):
        orderID = self.__exOrderID2OrderID[exOrderID]
        if orderID is not None: return self.lookupOrderID(orderID,copy)
        return None


    def trackOrder(self,order: SingleStockOrder):
        self.lock.acquire()
        self.__orders[order.orderID] = order.copyOrder()
        if order.exOrderID is not None:
            self.__exOrderID2OrderID[order.exOrderID] = order.orderID
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

    def __str__(self):
        return f"""
        OrderManager
        {"".join([str(order) for _,order in self.__orders.items()])}
        """
        # self.lock.acquire()
        # rep = f"""
        # OrderManager
        # {''.join([str(order) for _,order in self.__orders.items()])}
        # """
        # self.lock.release()
        # return rep
