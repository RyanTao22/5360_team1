# -*- coding: utf-8 -*-
"""
Created on Sat Jul 3 07:11:28 2019

@author: hongs
"""

class SingleStockExecution():
    
    def __init__(self, ticker, date, timeStamp):
        self.execID = 0
        self.exOrderID = 0
        self.orderID = 0
        self.ticker = ticker
        self.date = date
        self.timeStamp = timeStamp
        self.direction = None
        self.price = None
        self.size = None
        self.comm = 0 #commission for this transaction
        self.order = None

    def outputAsArray(self):
        output = []
        output.append(self.date)
        output.append(self.ticker)
        output.append(self.timeStamp)
        output.append(self.execID)
        output.append(self.orderID)
        output.append(self.exOrderID)
        output.append(self.direction)
        output.append(self.price)
        output.append(self.size)
        output.append(self.comm)
        output.append(self.order)

        return output

    def __str__(self):
        return f"""
        SingleStockExecution
        executionID:{self.execID}
        orderID:{self.orderID}        
        exOrderID:{self.exOrderID}
        direction:{self.direction}
        price:{self.price}
        size:{self.size}"""