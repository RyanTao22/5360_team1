# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2020

@author: hongsong chou
"""

import time
import random
import os
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels

# Some preparation for packages
import numpy as np
import pandas as pd
import datetime
from marketDataServiceConfig import MarketDataServiceConfig


class MarketDataService:

    rawData = {}
    fData = {}
    cData = {}


    def __init__(self, marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime,stockCodes,playSpeed, isReady=None):
        self.startDate = startDate
        self.endDate = endDate
        self.startTime = startTime
        self.stockCodes = stockCodes
        self.playSpeed = playSpeed

        print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))
        # time.sleep(3)
        # self.produce_market_data(marketData_2_exchSim_q, marketData_2_platform_q)
        print("[%d]<<<<< call MarketDataService.unzipFile" % (os.getpid(),))
        self.unzipFile()
        print("[%d]<<<<< call MarketDataService.loadinStockDataAndFilterbyTargetDate" % (os.getpid(),))
        self.loadinStockDataAndFilterbyTargetDate()
        print("[%d]<<<<< call MarketDataService.dataCleaningAndPreProcessing" % (os.getpid(),))
        self.dataCleaningAndPreProcessing()
        print("[%d]<<<<< call MarketDataService.concatStockRows" % (os.getpid(),))
        self.concatStockRows()
        print("[%d]<<<<< call MarketDataService.calculateTimestampDiff" % (os.getpid(),))
        self.calculateTimestampDiff()

        if isReady is not None:
            while isReady.value==0:
                print("sleep for 3 secs")
                time.sleep(3)

        self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)


    def unzipFile(self):
        print("start to check and unzip")
        #print(MarketDataServiceConfig.mainDir)
        if not os.path.exists(MarketDataServiceConfig.mainDir):
            if os.path.exists(MarketDataServiceConfig.mainZip):
                import zipfile
                with zipfile.ZipFile(MarketDataServiceConfig.mainZip, 'r') as zip_ref:
                    zip_ref.extractall(MarketDataServiceConfig.main)
        print("end to check and unzip")

    def loadinStockDataAndFilterbyTargetDate(self):
        print("start to loadinStockDataAndFilterbyTargetDate")

        # tDates: pairs of (yead and month) for the range from startDate to endDate
        year_month_pairs = set(pd.date_range(start=self.startDate, end=self.endDate).strftime('%Y%m'))
        tDates = list(year_month_pairs)

        for stock in self.stockCodes:
            for tDate in tDates:
                stockDataFileName = MarketDataServiceConfig.mainDir + MarketDataServiceConfig.stocksPath + stock + "_md_" + tDate + "_" + tDate + ".csv.gz"
                print(stockDataFileName)
                # concat stock's different month's data
                if os.path.exists(stockDataFileName):
                    if stock not in self.rawData: self.rawData[stock] = pd.read_csv(stockDataFileName, compression='gzip', index_col=0)
                    else:                         self.rawData[stock] = pd.concat([self.rawData[stock], pd.read_csv(stockDataFileName, compression='gzip', index_col=0)], axis=0, ignore_index=True)
                
            print("Done stock " + stock + str(len(self.rawData[stock])))
            self.fData[stock] = self.rawData[stock].loc[self.rawData[stock]['date'] >= self.startDate]
            self.fData[stock] = self.fData[stock].loc[self.fData[stock]['date'] <= self.endDate]
            self.fData[stock] = self.fData[stock].loc[self.fData[stock]['time'] >self.startTime]

            #self.fData[stock] = self.fData[stock].sort_values(by=['date', 'time'], ascending=True)
            #self.fData[stock].index = list(range(len(self.fData[stock])-1)) + [-1] # reset index to indicate the last row

            print("Done filter by date " + stock)

        self.rawData = {}

        print("end to loadinStockDataAndFilterbyTargetDate")

    def dataCleaningAndPreProcessing(self):

        print("start to dataCleaningAndPreProcessing")

        for stock in self.stockCodes:
            self.fData[stock] = self.fData[stock][self.fData[stock]['SP1'] > 0]
            self.fData[stock] = self.fData[stock][self.fData[stock]['BP1'] > 0]
            self.fData[stock] = self.fData[stock][self.fData[stock]['SP1'] > self.fData[stock]['BP1']]
            self.fData[stock]['midQ'] = (self.fData[stock]['BP1'] + self.fData[stock]['SP1']) / 2

            self.fData[stock]['lastPx'] = self.fData[stock]['lastPx'].fillna(method='ffill')
            self.fData[stock]['size'] = self.fData[stock]['size'].fillna(0)
            self.fData[stock]['ticker'] = stock

            print("Done cleaning stock " + stock)

        print("end to dataCleaningAndPreProcessing")

    def concatStockRows(self):
        print("start to concatStockRows")

        self.cData = pd.DataFrame()
        for stock in self.stockCodes:
            self.cData = pd.concat([self.cData, self.fData[stock]], axis=0, ignore_index=True)

        self.cData = self.cData.sort_values(by=['date','time'], ascending=True)
        #self.cData = list(range(len(self.cData)-1)) + [-1]  # reset index to indicate the last row
        self.fData = {}

        print("end to concatStockRows")

    def calculateTimestampDiff(self):

        print("start to calculateTimestampDiff")

        ts_diff = pd.to_datetime(self.cData['time'], format='%H%M%S%f') - pd.to_datetime(self.cData['time'].shift(1),format='%H%M%S%f')
        self.cData['ts_diff'] = ts_diff.dt.total_seconds() * 1000
        self.cData['ts_diff'] = self.cData['ts_diff'].fillna(0)

        print("end to calculateTimestampDiff")


    def produce_market_data(self, marketData_2_exchSim_q, marketData_2_platform_q):
        print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))

    # for i in range(10):
    #     self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)
    #     time.sleep(5)

    def produce_quote(self, marketData_2_exchSim_q, marketData_2_platform_q):
        print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))
        self.cData.sort_index(axis=1,inplace=True)
        for index, row in self.cData.iterrows():
            diff = float(row['ts_diff'])/1000/self.playSpeed
            now = datetime.datetime.now()
            quoteSnapshot = OrderBookSnapshot_FiveLevels(row.ticker, now.date(), now.time(),
                                                         bidPrice=row["BP1":"BP5"].tolist(),
                                                         askPrice=row["SP1":"SP5"].tolist(),
                                                         bidSize=row["BV1":"BV5"].tolist(),
                                                         askSize=row["SV1":"SV5"].tolist())
            time.sleep(diff)
            marketData_2_exchSim_q.put(quoteSnapshot)
            marketData_2_platform_q.put(quoteSnapshot)
        quoteEndOfData = OrderBookSnapshot_FiveLevels('EndOfData', now.date(), now.time(),
                                                      bidPrice=[0, 0, 0, 0, 0],
                                                      askPrice=[0, 0, 0, 0, 0],
                                                      bidSize=[0, 0, 0, 0, 0],
                                                      askSize=[0, 0, 0, 0, 0])
        marketData_2_platform_q.put(quoteEndOfData)
            # print(quoteSnapshot.outputAsDataFrame())


# bidPrice, askPrice, bidSize, askSize = [], [], [], []
# bidPrice1 = 20+random.randint(0,100)/100
# askPrice1 = bidPrice1 + 0.01
# for i in range(5):
#     bidPrice.append(bidPrice1-i*0.01)
#     askPrice.append(askPrice1+i*0.01)
#     bidSize.append(100+random.randint(0,100)*100)
#     askSize.append(100+random.randint(0,100)*100)
# quoteSnapshot = OrderBookSnapshot_FiveLevels('testTicker', '20230706', time.asctime(time.localtime(time.time())),
#                                              bidPrice, askPrice, bidSize, askSize)
# print('[%d]MarketDataService>>>produce_quote' % (os.getpid()))
# print(quoteSnapshot.outputAsDataFrame())
# marketData_2_exchSim_q.put(quoteSnapshot)
# marketData_2_platform_q.put(quoteSnapshot)
