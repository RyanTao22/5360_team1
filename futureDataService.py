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


class FutureDataService:

    #quotes data
    rawData = {}
    qfData = {}
    qcData = {}

    #trades data
    rawtData = {}
    tfData = {}
    tcData = {}

    #concat data for quotes and trades
    tqcData = {}


    def __init__(self, futureData_2_exchSim_q, futureData_2_platform_q, startDate, endDate, startTime,futuresCodes,playSpeed, backTest,resampleFreq,isReady=None):
        self.startDate = startDate
        self.endDate = endDate
        self.startTime = startTime
        self.futureCodes = futuresCodes
        self.playSpeed = playSpeed
        self.backTest = backTest
        self.futureData_2_exchSim_q = futureData_2_exchSim_q
        self.futureData_2_platform_q = futureData_2_platform_q

        print("[%d]<<<<< call FutureDataService.init" % (os.getpid(),))
        #time.sleep(3) #let marketDataService to unzip first
        # self.produce_market_data(marketData_2_exchSim_q, marketData_2_platform_q)

        print("[%d]<<<<< call FutureDataService.unzipFile" % (os.getpid(),))
        self.unzipFile()

        print("[%d]<<<<< call FutureDataService.loadinFutureQuotesAndFilterbyTargetDataAndStartTime" % (os.getpid(),))
        self.loadinFutureQuotesAndFilterbyTargetDataAndStartTime()
        print("[%d]<<<<< call FutureDataService.futureQuotesCleaningAndPreProcessing" % (os.getpid(),))
        self.futureQuotesCleaningAndPreProcessing()
        print("[%d]<<<<< call FutureDataService.concatQuotesRows" % (os.getpid(),))
        self.concatQuotesRows()

        print("[%d]<<<<< call FutureDataService.loadinFutureTradesAndFilterbyTargetDataAndStartTime" % (os.getpid(),))
        self.loadinFutureTradesAndFilterbyTargetDataAndStartTime()
        print("[%d]<<<<< call FutureDataService.futureTradesCleaningAndPreProcessing" % (os.getpid(),))
        self.futureTradesCleaningAndPreProcessing()
        print("[%d]<<<<< call FutureDataService.concatTradesRows" % (os.getpid(),))
        self.concatTradesRows()

        print("[%d]<<<<< call FutureDataService.concatQuotesWithFuture" % (os.getpid(),))
        self.concatQuotesWithTrades()

        print("[%d]<<<<< call FutureDataService.calculateTimestampDiff" % (os.getpid(),))
        self.calculateTimestampDiff()

        print("[%d]<<<<< call FutureDataService.resampleData" % (os.getpid(),))
        self.resampleData(resampleFreq)


        # if isReady is not None:
        #     while isReady.value==0:
        #         print("sleep for 3 secs")
        #         time.sleep(3)
        #
        # self.produce_future(futureData_2_exchSim_q, futureData_2_platform_q)




        # print("[%d]<<<<< call MarketDataService.loadinStockDataAndFilterbyTargetDate" % (os.getpid(),))
        # self.loadinStockDataAndFilterbyTargetDate()
        # print("[%d]<<<<< call MarketDataService.dataCleaningAndPreProcessing" % (os.getpid(),))
        # self.dataCleaningAndPreProcessing()
        # print("[%d]<<<<< call MarketDataService.concatStockRows" % (os.getpid(),))
        # self.concatStockRows()
        # print("[%d]<<<<< call MarketDataService.calculateTimestampDiff" % (os.getpid(),))
        # self.calculateTimestampDiff()
        #
        # self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)

    def resampleData(self,resampleFreq):
        if resampleFreq != None:
            self.tqcData.index = self.tqcData.apply(lambda row: datetime.datetime.strptime(row['date']+ ' ' + str(row['time']).zfill(8), '%Y-%m-%d %H%M%S%f'), axis=1)
            self.tqcData = self.tqcData.resample(resampleFreq).first()
            self.tqcData.dropna(inplace=True)
            self.tqcData.reset_index(drop = True,inplace=True)
    
    def unzipFile(self):
        print("start to check and unzip")
        #print(MarketDataServiceConfig.mainDir)
        if not os.path.exists(MarketDataServiceConfig.mainDir):
            if os.path.exists(MarketDataServiceConfig.mainZip):
                import zipfile
                with zipfile.ZipFile(MarketDataServiceConfig.mainZip, 'r') as zip_ref:
                    zip_ref.extractall(MarketDataServiceConfig.main)
        print("end to check and unzip")

    def loadinFutureQuotesAndFilterbyTargetDataAndStartTime(self):
        print("start to loadinFutureQuotesAndFilterbyTargetDataAndStartTime")

        year_month_pairs = set(pd.date_range(start=self.startDate, end=self.endDate).strftime('%Y%m'))
        tDates = list(year_month_pairs)

        for future in self.futureCodes:
            for tDate in tDates:
                futureDataFileName = MarketDataServiceConfig.mainDir + MarketDataServiceConfig.futuresQuotesPath + future + "_md_" + tDate + "_" + tDate + ".csv.gz"
                #print(futureDataFileName)
                if os.path.exists(futureDataFileName):
                    if future not in self.rawData: self.rawData[future] = pd.read_csv(futureDataFileName, compression='gzip')
                    else:                          self.rawData[future] = pd.concat([self.rawData[future], pd.read_csv(futureDataFileName, compression='gzip')], axis=0, ignore_index=True)
            print("Done future " + future)
            self.qfData[future] = self.rawData[future].loc[self.rawData[future]['date']>= self.startDate]
            self.qfData[future] = self.qfData[future].loc[self.qfData[future]['date'] <= self.endDate]
            self.qfData[future] = self.qfData[future].loc[self.qfData[future]['time'] > self.startTime]

            print("Done filter by date and time " + future)

        rawData = {}
        print("end to loadinFutureQuotesAndFilterbyTargetDataAndStartTime")

    def loadinFutureTradesAndFilterbyTargetDataAndStartTime(self):
        print("start to loadinFutureTradesAndFilterbyTargetDataAndStartTime")
        
        year_month_pairs = set(pd.date_range(start=self.startDate, end=self.endDate).strftime('%Y%m'))
        tDates = list(year_month_pairs)

        for future in self.futureCodes:
            for tDate in tDates:
                futureDataFileName = MarketDataServiceConfig.mainDir + MarketDataServiceConfig.futuresTradesPath + future + "_mdT_" + tDate + "_" + tDate + ".csv.gz"
                #print(futureDataFileName)
                if os.path.exists(futureDataFileName):
                    if future not in self.rawtData: self.rawtData[future] = pd.read_csv(futureDataFileName, compression='gzip')
                    else:                         self.rawtData[future] = pd.concat([self.rawtData[future], pd.read_csv(futureDataFileName, compression='gzip')], axis=0, ignore_index=True)
            print("Done future " + future)
            self.tfData[future] = self.rawtData[future].loc[self.rawtData[future]['Date'] >= self.startDate]
            self.tfData[future] = self.tfData[future].loc[self.tfData[future]['Date'] <= self.endDate]
            self.tfData[future] = self.tfData[future].loc[self.tfData[future]['Time'] > self.startTime]

            print("Done filter by date and time " + future)

        rawtData = {}
        print("end to loadinFutureTradesAndFilterbyTargetDataAndStartTime")




    # def loadinStockDataAndFilterbyTargetDate(self):
    #     print("start to loadinStockDataAndFilterbyTargetDate")
    #
    #     tDate = MarketDataServiceConfig.targetDate[0:4] + MarketDataServiceConfig.targetDate[5:7]
    #
    #     for stock in MarketDataServiceConfig.stockCodes:
    #         stockDataFileName = MarketDataServiceConfig.mainDir + MarketDataServiceConfig.stocksPath + stock + "_md_" + tDate + "_" + tDate + ".csv.gz"
    #         print(stockDataFileName)
    #         self.rawData[stock] = pd.read_csv(stockDataFileName, compression='gzip', index_col=0)
    #         print("Done stock " + stock)
    #         self.fData[stock] = self.rawData[stock].loc[self.rawData[stock]['date'] == MarketDataServiceConfig.targetDate]
    #
    #         print("Done filter by date " + stock)
    #
    #     self.rawData = {}
    #
    #     print("end to loadinStockDataAndFilterbyTargetDate")

    def futureQuotesCleaningAndPreProcessing(self):
        print("start to futureQuotesCleaningAndPreProcessing")

        for future in self.futureCodes:
            self.qfData[future] = self.qfData[future][self.qfData[future]['askPrice1'] > 0]
            self.qfData[future] = self.qfData[future][self.qfData[future]['bidPrice1'] > 0]
            self.qfData[future] = self.qfData[future][self.qfData[future]['askPrice1'] > self.qfData[future]['bidPrice1']]
            self.qfData[future]['midQ'] = (self.qfData[future]['askPrice1'] + self.qfData[future]['bidPrice1']) / 2

            # qfData[future]['lastPx'] = qfData[future]['lastPx'].fillna(method='ffill')
            # qfData[future]['size']   = qfData[future]['size'].fillna(0)
            self.qfData[future]['ticker'] = future
            self.qfData[future]['type'] = 'quotes'

            print("Done cleaning future " + future)
        print("end to futureQuotesCleaningAndPreProcessing")

    def futureTradesCleaningAndPreProcessing(self):
        print("start to futureTradesCleaningAndPreProcessing")

        for future in self.futureCodes:
            # qfData[future] = qfData[future][qfData[future]['askPrice1']>0]
            # qfData[future] = qfData[future][qfData[future]['bidPrice1']>0]
            # qfData[future] = qfData[future][qfData[future]['askPrice1']>qfData[future]['bidPrice1']]
            # qfData[future]['midQ'] = (qfData[future]['askPrice1']+qfData[future]['bidPrice1'])/2

            # qfData[future]['lastPx'] = qfData[future]['lastPx'].fillna(method='ffill')
            self.tfData[future]['totalMatchSize'] = self.tfData[future]['totalMatchSize'].fillna(0)
            self.tfData[future]['totalMatchValue'] = self.tfData[future]['totalMatchValue'].fillna(method='ffill')
            self.tfData[future]['avgMatchPx'] = self.tfData[future]['avgMatchPx'].fillna(method='ffill')
            self.tfData[future]['ticker'] = future
            self.tfData[future]['type'] = 'trades'

            print("Done cleaning future " + future)

        print("end to futureTradesCleaningAndPreProcessing")

    # def dataCleaningAndPreProcessing(self):
    #
    #     print("start to dataCleaningAndPreProcessing")
    #
    #     for stock in MarketDataServiceConfig.stockCodes:
    #         self.fData[stock] = self.fData[stock][self.fData[stock]['SP1'] > 0]
    #         self.fData[stock] = self.fData[stock][self.fData[stock]['BP1'] > 0]
    #         self.fData[stock] = self.fData[stock][self.fData[stock]['SP1'] > self.fData[stock]['BP1']]
    #         self.fData[stock]['midQ'] = (self.fData[stock]['BP1'] + self.fData[stock]['SP1']) / 2
    #
    #         self.fData[stock]['lastPx'] = self.fData[stock]['lastPx'].fillna(method='ffill')
    #         self.fData[stock]['size'] = self.fData[stock]['size'].fillna(0)
    #         self.fData[stock]['ticker'] = stock
    #
    #         print("Done cleaning stock " + stock)
    #
    #     print("end to dataCleaningAndPreProcessing")

    def concatQuotesRows(self):
        print("start to concatQuotesRows")
        self.qcData = pd.DataFrame()
        for future in self.futureCodes:
            self.qcData = pd.concat([self.qcData, self.qfData[future]], axis=0, ignore_index=False)

        self.qfData = {}
        print(self.qcData)
        self.qcData = self.qcData.sort_values(by=['date','time'], ascending=True)
        print("end to concatQuotesRows")

    def concatTradesRows(self):
        print("start to concatTradesRows")
        self.tcData = pd.DataFrame()
        for future in self.futureCodes:
            self.tcData = pd.concat([self.tcData, self.tfData[future]], ignore_index=True)

        self.tfData = {}
        self.tcData = self.tcData.sort_values(by=['Date','Time'], ascending=True)
        self.tcData = self.tcData.rename(columns={'Date':'date','Time': 'time'})

        print("end to concatTradesRows")

    def concatQuotesWithTrades(self):
        print("start to concatQuotesWithTrades")
        self.tqcData = pd.DataFrame()
        self.tqcData = pd.concat([self.qcData, self.tcData], ignore_index=True)
        self.qcData = {}
        self.tcData = {}

        self.tqcData = self.tqcData.sort_values(by=['date','time'], ascending=True)
        print("end to concatQuotesWithTrades")




    # def concatStockRows(self):
    #     print("start to concatStockRows")
    #
    #     self.cData = pd.DataFrame()
    #     for stock in MarketDataServiceConfig.stockCodes:
    #         self.cData = pd.concat([self.cData, self.fData[stock]], axis=0, ignore_index=True)
    #
    #     self.cData = self.cData.sort_values(by=['date','time'], ascending=True)
    #     self.fData = {}
    #
    #     print("end to concatStockRows")

    def calculateTimestampDiff(self):

        print("start to calculateTimestampDiff")

        ts_diff = pd.to_datetime(self.tqcData['time'], format='%H%M%S%f') - pd.to_datetime(self.tqcData['time'].shift(1),format='%H%M%S%f')
        self.tqcData['ts_diff'] = ts_diff.dt.total_seconds() * 1000
        self.tqcData['ts_diff'] = self.tqcData['ts_diff'].fillna(0)

        print("end to calculateTimestampDiff")


    # def produce_market_data(self, marketData_2_exchSim_q, marketData_2_platform_q):
    #     print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))

    # for i in range(10):
    #     self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)
    #     time.sleep(5)

    def produce_future(self, futureData_2_exchSim_q, futureData_2_platform_q,resampleFreq):
        print("[%d]<<<<< call FutureDataService.init" % (os.getpid(),))
        print("[%d]<<<<< call start to feed future quotes and trades" % (os.getpid(),))
        self.tqcData.sort_values(by=['date', 'time'], ascending=True, inplace=True)

        if resampleFreq != None:
            self.tqcData.index = self.tqcData.apply(lambda row: datetime.datetime.strptime(row['date']+ ' ' + str(row['time']).zfill(8), '%Y-%m-%d %H%M%S%f'), axis=1)
            self.tqcData = self.tqcData.resample(resampleFreq).first()
            self.tqcData.dropna(inplace=True)
            self.tqcData.reset_index(drop = True,inplace=True)
        
        '''获取列名,避免受顺序影响'''
        askPrice_cols_list = ['askPrice'+str(i) for i in range(1,6)]
        bidPrice_cols_list = ['bidPrice'+str(i) for i in range(1,6)]
        askSize_cols_list = ['askSize'+str(i) for i in range(1,6)]
        bidSize_cols_list = ['bidSize'+str(i) for i in range(1,6)]
        now = datetime.datetime.now()

        for index, row in self.tqcData.iterrows():
            
            '''如果是trades数据，就跳过'''
            if row['type'] == 'trades':
                continue
            

            diff = float(row['ts_diff'])/1000/self.playSpeed
            
            quoteSnapshot = OrderBookSnapshot_FiveLevels(row.ticker, datetime.datetime.strptime(row['date'], '%Y-%m-%d'),
                                                            datetime.datetime.strptime(str(row['time']), '%H%M%S%f').time(),
                                                            askPrice=[row[askPrice_cols_list[i]] for i in range(5)],
                                                            bidPrice=[row[bidPrice_cols_list[i]] for i in range(5)],
                                                            askSize=[row[askSize_cols_list[i]] for i in range(5)],
                                                            bidSize=[row[bidSize_cols_list[i]] for i in range(5)]
                                                         )
            quoteSnapshot.type = row.get("type")
            quoteSnapshot.midQ = row.get("midQ")
            quoteSnapshot.symbol = row.get("symbol")
            quoteSnapshot.totalMatchSize = row.get("totalMatchSize")
            quoteSnapshot.totalMatchValue = row.get("totalMatchValue")
            quoteSnapshot.avgMatchPx = row.get("avgMatchPx")
            quoteSnapshot.size = row.get("size")
            quoteSnapshot.volume = row.get("volume")
            quoteSnapshot.lastPx = row.get("lastPx")
            
            if not self.backTest: time.sleep(diff)
            futureData_2_exchSim_q.put(quoteSnapshot)
            futureData_2_platform_q.put(quoteSnapshot)
        '''添加一个EndOfData的信号'''
        endOfData = OrderBookSnapshot_FiveLevels(row.ticker+'_EndOfData', now.date(), now.time(),
                                                    bidPrice=[0, 0, 0, 0, 0],
                                                    askPrice=[0, 0, 0, 0, 0],
                                                    bidSize=[0, 0, 0, 0, 0],
                                                    askSize=[0, 0, 0, 0, 0])
        futureData_2_platform_q.put(endOfData)
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
