# -*- coding: utf-8 -*-
"""
Created on Fri Jul  5 23:47:43 2019

@author: hongsong chou
"""
from copy import deepcopy
class SingleStockOrder():
    
    def __init__(self, ticker, date, submissionTime):
        self.orderID = 0
        ######### BSJ ##########
        self.exOrderID = None
        ######### BSJ ##########
        self.ticker = ticker
        self.date = date
        self.submissionTime = submissionTime
        self.currStatusTime = None
        self.currStatus = "New" #"New", "Filled", "PartiallyFilled", "Cancelled"
        self.direction = None
        self.price = None
        self.size = None
        self.filledSize = 0
        self.type = None #"MLO", "LO", "MO", "TWAP"
        self.stratID = None

    def outputAsArray(self):
        output = []
        output.append(self.date)
        output.append(self.ticker)
        output.append(self.submissionTime)
        output.append(self.orderID)
        output.append(self.exOrderID)
        output.append(self.currStatus)
        output.append(self.currStatusTime)
        output.append(self.direction)
        output.append(self.price)
        output.append(self.size)
        output.append(self.filledSize)
        output.append(self.type)
        output.append(self.stratID)

        return output
    
    def copyOrder(self):
        # returnOrder = SingleStockOrder(self.ticker, self.date,self.submissionTime)
        # returnOrder.orderID = self.orderID
        # returnOrder.exOrderID = self.exOrderID
        # returnOrder.submissionTime = self.submissionTime
        # returnOrder.currStatusTime = self.currStatusTime
        # returnOrder.currStatus = self.currStatus
        # returnOrder.direction = self.direction
        # returnOrder.price = self.price
        # returnOrder.size = self.size
        # returnOrder.type = self.type
        # returnOrder.stratID = self.stratID

        returnOrder = deepcopy(self)
        return returnOrder

    def __str__(self):
        return f"""
        SingleStockOrder
        Ticker:{self.ticker}
        orderID:{self.orderID}
        exOrderID:{self.exOrderID}
        type:{self.type}
        currStatus:{self.currStatus}
        direction:{self.direction}
        price:{self.price}
        size:{self.size}
        filledSize:{self.filledSize}
        stratID:{self.stratID}
        """

