#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""
import datetime
import os
import time
from typing import Mapping
from uuid import uuid1

import pandas as pd
import numpy as np
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Platform.OrderManager import OrderManager
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution
from datetime import datetime
import lightgbm as lgb
from collections import deque
from copulae.elliptical import GaussianCopula
from scipy.stats import rankdata, norm

class QuantStrategy(Strategy):
    
    def __init__(self, stratID, stratName, stratAuthor, ticker, day):
        super(QuantStrategy, self).__init__(stratID, stratName, stratAuthor) #call constructor of parent
        self.ticker = ticker #public field
        self.day = day #public field
        
    def getStratDay(self):
        return self.day
    
    def run(self, marketData, execution):
        if (marketData is None) and (execution is None):
            return None
        elif (marketData is None) and ((execution is not None) and (isinstance(execution, SingleStockExecution))):
            #handle executions
            print('[%d] Strategy.handle_execution' % (os.getpid()))
            print(execution.outputAsArray())
            return None
        elif ((marketData is not None) and (isinstance(marketData, OrderBookSnapshot_FiveLevels))) and (execution is None):
            #handle new market data, then create a new order and send it via quantTradingPlatform.
            return SingleStockOrder('testTicker','2019-07-05',time.asctime(time.localtime(time.time())))
        else:
            return None
        

class SampleDummyStrategy(QuantStrategy):
    def __init__(self, stratID, stratName, stratAuthor, day, ticker,
                 tickers2Snapshots: Mapping[str,OrderBookSnapshot_FiveLevels],
                 orderManager:OrderManager, initial_cash, analysis_queue):
        super().__init__(stratID,stratName,stratAuthor,ticker,day)
        self.tickers2Snapshots = tickers2Snapshots
        self.orderManager = orderManager

        self.analysis_q = analysis_queue
        self.execution_record = []
        
        self.positions = {self.ticker[0]:[0], self.ticker[1]: [0]}
        self.cash = [initial_cash]
        self.networth = [initial_cash]
        self.midPrices = {self.ticker[0]:[0], self.ticker[1]: [0]}

        # self.baseline_cash = initial_cash
        # self.baseline_networth = initial_cash
        # self.baseline_positions = {}
    
        self.timestamp = []

    def run(self,execution:SingleStockExecution)->list[SingleStockOrder]:
        ticker1 = self.ticker[0]
        ticker2 = self.ticker[1]
        if execution is None:#######on receive market data
            ####get most recent market data for ticker
            
            ticker1MarketData:list[pd.DataFrame] = self.tickers2Snapshots[ticker1]
            ticker2MarketData:list[pd.DataFrame] = self.tickers2Snapshots[ticker2]
            ticker1RecentMarketData = None
            ticker2RecentMarketData = None


            if len(ticker1MarketData) > 0:
                
                ticker1RecentMarketData = ticker1MarketData[-1]
                '''更新价格1'''
                self.midPrices[ticker1].append((ticker1RecentMarketData['bidPrice1'] + ticker1RecentMarketData['askPrice1'])/2)
                #print('更新价格1',ticker1RecentMarketData['time'])
                #df['date'] + pd.to_timedelta(df['time'].astype(str))
                self.timestamp.append(pd.to_datetime(ticker1RecentMarketData['date'])  + pd.to_timedelta(ticker1RecentMarketData['time'].astype(str)))
                #self.timestamp.append(pd.to_datetime(ticker1RecentMarketData['date']  + ticker1RecentMarketData['time']))

            if len(ticker2MarketData) > 0:
                ticker2RecentMarketData = ticker2MarketData[-1]
                '''更新价格2'''
                self.midPrices[ticker2].append((ticker2RecentMarketData['bidPrice1'] + ticker2RecentMarketData['askPrice1'])/2)
                #print('更新价格2',ticker2RecentMarketData['time'])
                #self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date'].to_string + ' ' + ticker2RecentMarketData['time']))
                # unsupported operand type(s) for +: 'DatetimeArray' and 'str'
                self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date'])  + pd.to_timedelta(ticker2RecentMarketData['time'].astype(str)))
                #self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date'] + ticker2RecentMarketData['time']))

            '''使用更新后的价格计算净值'''
            netWrorth = self.cash[-1]
            if len(self.positions[ticker1]) > 0:
                netWrorth += self.positions[ticker1][-1] * self.midPrices[ticker1][-1]
            if len(self.positions[ticker2]) > 0:
                netWrorth += self.positions[ticker2][-1] * self.midPrices[ticker2][-1]
            
            #print(self.timestamp[-1],self.cash[-1],netWrorth)
            '''记录到共享队列analysis_q中'''
            # update analysis: timestamp, networth, cash
            self.analysis_q.put({
                'timestamp':  self.timestamp[-1],
                'cash': self.cash[-1],
                'networth': netWrorth,
                'positions_'+ticker1: self.positions[ticker1][-1],
                'positions_'+ticker2: self.positions[ticker2][-1],
                'midPrice_'+ticker1: self.midPrices[ticker1][-1],
                'midPrice_'+ticker2: self.midPrices[ticker2][-1]
            })
            


            if ticker1RecentMarketData is not None and ticker2RecentMarketData is not None:
                #########do some calculation with the recent market data
                #.....
                from datetime import datetime
                now = datetime.now()
                sampleOrder1 = SingleStockOrder(
                    ticker=ticker1,
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder1.orderID = f"{self.getStratID()}-2610-{str(uuid1())}"
                sampleOrder1.type = "MO"
                sampleOrder1.currStatus = "New"
                sampleOrder1.currStatusTime = now.time()
                sampleOrder1.direction = 1
                sampleOrder1.size = 1
                sampleOrder1.stratID = self.getStratID()

                sampleOrder2 = SingleStockOrder(
                    ticker=ticker2,
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder2.orderID = f"{self.getStratID()}-3374-{str(uuid1())}"
                sampleOrder2.type = "LO"
                sampleOrder2.currStatus = "New"
                sampleOrder2.currStatusTime = now.time()
                sampleOrder2.direction = 1###1 = buy; -1 = sell
                sampleOrder2.size = 2
                sampleOrder2.price = ticker2RecentMarketData['bidPrice5'].item()
                sampleOrder2.stratID = self.getStratID()

                
                
                ######return a list
                return [sampleOrder1,sampleOrder2]
        else:
            #######on receive execution
            order = self.orderManager.lookupOrderID(execution.orderID)

            
            '''记录持仓'''
            ticker, tradesize, direction, tradeprice = execution.ticker, execution.size, execution.direction,execution.price
            self.execution_record.append({
                'timestamp': execution.timeStamp,
                'ticker': ticker,
                'size': tradesize,
                'direction': direction,
                'price': tradeprice
            })
            '''更新仓位现金'''
            self.positions[ticker].append(self.positions[ticker][-1] + tradesize * direction)
            self.cash.append(self.cash[-1] - tradesize * direction * tradeprice)


            ######do something

            ####e.g. issue cancel order
            # if order.type == "LO":
            #     order.type = "CANCEL"
            return []

        return []


                

class InDevelopingStrategy(QuantStrategy):
    def __init__(self, stratID, stratName, stratAuthor, ticker, day,
                 tickers2Snapshots: Mapping[str,OrderBookSnapshot_FiveLevels],
                 orderManager:OrderManager):
        super().__init__(stratID,stratName,stratAuthor,ticker,day)
        self.tickers2Snapshots = tickers2Snapshots
        self.orderManager = orderManager
        self.day = day  # public field
        self.initial_cash = 10000000.0
        # Create the declarative base
        self.timestamp = []
        self.cash = [10000000.0]
        self.limit_cash = self.initial_cash * 0.05
        self.cashCostRatio = 0.2
        self.networth = [0]
        self.pnl = []
        self.position = {ticker[0]:[0], ticker[1]: [0]} #这个应该是一个{}的形状
        self.ret = []
        self.current_idx = 0
        self.tickers = ticker
        self.current_time = 0
        self.orderSIdx = 0
        self.orderFIdx = 0
        self.lastdirection1 = 0
        self.lastdirection2 = 0
        '''记录所有发出的订单'''
        self.submitted_order = [] #(ordertime, orderid, orderprice, ordersize, direction)
        '''记录未成交或者成交后有残余的订单'''
        self.untreated_order = [] #(ordertime, orderid, orderprice, ordersize, direction)
    def gain_timeindex(self, start_time, end_time):
        '''获取分钟级别时间间隔'''
        time_interval = datetime.timedelta(minutes=1)
        time_data = []
        current_time = start_time
        while current_time <= end_time:
            time_data.append(current_time.strftime('%H:%M'))
            current_time += time_interval
        return time_data

    def gain_datetime(self, time, date='2023-01-01'):
        '''转换时间类型'''
        year = int(date[:4])
        month = int(date[5:7])
        day = int(date[8:10])
        hour = time // 10000000
        minute = (time // 100000) % 100
        second = (time // 1000) % 100
        dt = datetime.datetime(year, month, day, hour, minute, second)
        return dt

    def stock_dataprocess(self, rawStock):
        '''处理股票数据'''
        df = rawStock.copy()
        df = df.reset_index(drop=True)
        start_time = self.gain_datetime(df['time'][0])
        end_time = self.gain_datetime(df['time'][-1])
        time_data = self.gain_timeindex(start_time, end_time)

        df['time_trans'] = df['time'].astype('str').str.zfill(9).apply(lambda x: x[:2] + ':' + x[2:4])
        df['lastPx'] = df['lastPx'].fillna(method='ffill')
        df_min = pd.DataFrame()
        grouped = df.groupby('time_trans')
        df_min['high'] = grouped['lastPx'].max()
        df_min['low'] = grouped['lastPx'].min()
        df_min['close'] = grouped['lastPx'].last()
        df_min['open'] = grouped['lastPx'].first()

        cols = ['date', 'volume', 'SP5', 'SP4', 'SP3', 'SP2', 'SP1', 'BP1', 'BP2', 'BP3', 'BP4', 'BP5', 'SV5', 'SV4',
                'SV3', 'SV2', 'SV1', 'BV1', 'BV2', 'BV3', 'BV4', 'BV5']

        for col in cols:
            df_min[col] = grouped[col].last()
        df_min = df_min.reindex(time_data)
        df_min.index.name = 'time'
        df_min.reset_index(inplace=True)
        return df_min

    def future_dataprocess(self, rawFutureQ, rawFutureT):
        '''处理期货数据'''
        # 盘口数据
        df_Quotes = rawFutureQ.copy()
        df_Quotes = df_Quotes.reset_index(drop=True)
        start_time = self.gain_datetime(df_Quotes['time'][0])
        end_time = self.gain_datetime(df_Quotes['time'][-1])
        time_data_ft = self.gain_timeindex(start_time, end_time)
        df_Quotes['time_trans'] = df_Quotes['time'].astype('str').str.zfill(9).apply(lambda x: x[:2] + ':' + x[2:4])

        df_Quotes_min = pd.DataFrame()
        grouped = df_Quotes.groupby('time_trans')
        cols = ['date', 'symbol', 'askPrice5', 'askPrice4', 'askPrice3', 'askPrice2', 'askPrice1', 'bidPrice1',
                'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5', 'askSize5', 'askSize4', 'askSize3', 'askSize2',
                'askSize1', 'bidSize1', 'bidSize2', 'bidSize3', 'bidSize4', 'bidSize5']
        for col in cols:
            df_Quotes_min[col] = grouped[col].last()
        df_Quotes_min = df_Quotes_min.reindex(time_data_ft)

        # 成交数据
        df_Trades = rawFutureT.copy()
        df_Trades = df_Trades.reset_index(drop=True)
        start_time = self.gain_datetime(df_Trades['Time'][0])
        end_time = self.gain_datetime(df_Trades['Time'][-1])
        time_data_ft = self.gain_timeindex(start_time, end_time)

        df_Trades['time'] = df_Trades['Time'].astype('str').str.zfill(9).apply(lambda x: x[:2] + ':' + x[2:4])
        grouped = df_Trades.groupby('time')
        df_Trades_min = pd.DataFrame()
        df_Trades_min['totalMatchValue'] = grouped['totalMatchValue'].sum()
        df_Trades_min['totalMatchSize'] = grouped['totalMatchSize'].sum()
        df_Trades_min['avgMatchPx'] = df_Trades_min['totalMatchValue'] / df_Trades_min['totalMatchSize']
        df_Trades_min['avgMatchPx'].fillna(method='ffill', inplace=True)
        df_Trades_min = df_Trades_min.reindex(time_data_ft)
        df_concat = pd.concat([df_Trades_min, df_Quotes_min], axis=1)
        df_concat.index.name = 'time'
        df_concat.reset_index(inplace=True)
        return df_concat
    def get_data(self, stockdf, futuredfQ, futuredfT):

        '''从project3的传输中得到数据
           n:需要回看的数据长度
            tickers:获取对应标的的数据（tickers可以是单只股票（期货），也可以是一堆，这里也可以考虑每次只取一个ticker的后面再合成
            返回 数据表（如果长度小于n（则要么返回现有长度，要么由于存在回看，小于说明数据量不够，无法回看，直接返回空'''

        processedDataStock = self.stock_dataprocess(stockdf)
        processedDataFuture = self.future_dataprocess(futuredfQ, futuredfT)
        return processedDataStock, processedDataFuture
    def generate_signal(self, df_stock, df_future, past_step, col='return_5'):

        '''生成策略相关信号
        每个分钟开始时回看past_step个分钟，假设传入一天的数据，输出一整天的分钟频信号。
        每次只传入一对股票期货对'''
        if len(df_stock) < past_step or len(df_future) < past_step:
            return
        df_stock['return'] = np.log(1 + df_stock['close'].pct_change())
        df_stock['return_5'] = np.log(1 + df_stock['close'].pct_change(periods=5))
        df_stock['return_10'] = np.log(1 + df_stock['close'].pct_change(periods=10))
        df_future['return'] = np.log(1 + df_future['avgMatchPx'].pct_change())
        df_future['return_5'] = np.log(1 + df_future['avgMatchPx'].pct_change(periods=5))
        df_future['return_10'] = np.log(1 + df_future['avgMatchPx'].pct_change(periods=10))

        return_df = pd.merge(df_stock[['time', col]], df_future[['time', col]], on='time', how='inner')
        return_df = return_df.dropna().reset_index(drop=True)

        signal = []
        time = return_df['time'][-1]
            # print(time)
        returns_1 = return_df[col + '_x'][-past_step:]
        returns_2 = return_df[col + '_y'][-past_step:]
        u1 = rankdata(returns_1) / (len(returns_1) + 1)
        u2 = rankdata(returns_2) / (len(returns_2) + 1)
        u = np.vstack((u1, u2)).T
            # print(u)
        cop = GaussianCopula(dim=2)
        cop.fit(u)

        # 假设我们有未来的预测值,这里直接用最近的一个收益率值
        ######## 可以替换成机器学习 ##########
        future_return_1 = return_df[col + '_x'][-1]  # 未来预测的资产1的对数收益率
        future_return_2 = return_df[col + '_y'][-1]  # 未来预测的资产2的对数收益率

         # 将未来的预测值转换为均匀分布
        future_u1 = norm.cdf(future_return_1, np.mean(returns_1), np.std(returns_1))
        future_u2 = norm.cdf(future_return_2, np.mean(returns_2), np.std(returns_2))

        # 计算Copula CDF值
        cop_cdf = cop.cdf([future_u1, future_u2])

        # 计算边缘分布的概率
        marginal_u1 = norm.cdf(future_return_1, np.mean(returns_1), np.std(returns_1))
        marginal_u2 = norm.cdf(future_return_2, np.mean(returns_2), np.std(returns_2))

        # 计算条件概率
        conditional_u1_given_u2 = cop_cdf / marginal_u2
        conditional_u2_given_u1 = cop_cdf / marginal_u1

        # 基于条件概率生成交易信号
        threshold = 0.5  # 假设一个阈值
        # buy_signal: buy u1, and sell u2
        sell_signal_u1 = conditional_u1_given_u2 > threshold
        buy_signal_u1 = conditional_u1_given_u2 < (1 - threshold)
        buy_signal_u2 = conditional_u2_given_u1 > threshold
        sell_signal_u2 = conditional_u2_given_u1 < (1 - threshold)

        signal.append({"time": time,
                           "Buy Signal based on U1:": buy_signal_u1,
                           "Sell Signal based on U1": sell_signal_u1,
                           "Buy Signal based on U2": buy_signal_u2,
                           "Sell Signal based on U2": sell_signal_u2
                           })

        signal_df = pd.DataFrame(signal)
        return signal_df
    def run(self,execution:SingleStockExecution)->list[SingleStockOrder]:
        if execution is None:#######on receive market data
            ####get most recent market data for ticker
            '''we assume ticker1 is stock, ticker2 is future 
            the data we acquire is a list of dataframe containg all data frmo the beginning to the lastest one'''
            ticker1 = "2610"
            ticker2 = "3374"
            ticker1MarketData:list[pd.DataFrame] = self.tickers2Snapshots[ticker1]
            ticker2MarketDataQ:list[pd.DataFrame] = self.tickers2Snapshots[ticker2]['Quote']
            ticker2MarketDataT: list[pd.DataFrame] = self.tickers2Snapshots[ticker2]['Trade']
            ticker1RecentMarketData = None
            ticker2RecentMarketData = None

            if len(ticker1MarketData) > 0:
                # ticker1RecentMarketData = ticker1MarketData[-1]
                ticker1RecentMarketData = ticker1MarketData[-1]

            if len(ticker2MarketDataQ) > 0:
                ticker2RecentMarketDataQ = ticker2MarketDataQ[-1]

            if len(ticker2MarketDataQ) > 0:
                ticker2RecentMarketDataT = ticker2MarketDataT[-1]


            if ticker1RecentMarketData is not None and ticker2RecentMarketDataQ is not None and ticker2RecentMarketDataT is not None:
                #########do some calculation with the recent market data that means we get new data?
                #.....
                stock_df = pd.concat()
                df1, df2 = self.get_data(self.tickers2Snapshots[ticker1], self.tickers2Snapshots[ticker2]['Quote'],
                                         self.tickers2Snapshots[ticker2]['Trade'])

                orders = self.generate_signal(df1, df2, 10)
                direction_stock = 1 if orders.iloc[:, 1].item() == 1 else -1 if orders.iloc[:, 2].item() == 1 else 0
                direction_futures = 1 if orders.iloc[:, 3].item() == 1 else -1 if orders.iloc[:, 4].item() == 1 else 0
                if self.cash[-1] <= self.limit_cash:
                    print('cash is not enough')
                    cash_stock = 0
                    cash_future = 0
                else:
                    cash_stock = self.cash[-1] * self.cashCostRatio // 2
                    cash_future = self.cash[- 1] * self.cashCostRatio // 2
                if direction_stock > 0:
                    ordersizeStock = cash_stock / ticker1RecentMarketData['AskPrice1']
                    odprice = ticker1RecentMarketData['AskPrice1']
                elif direction_stock < 0:
                    ordersizeStock = cash_stock / ticker1RecentMarketData['BidPrice1']
                    odprice = ticker1RecentMarketData['BidPrice1']
                else:
                    ordersizeStock = 0
                    odprice = 0

                if direction_futures > 0:
                    ordersizeFutures = cash_stock / ticker2MarketDataQ['AskPrice1']
                    odpriceF = ticker2MarketDataQ['AskPrice1']
                elif direction_futures < 0:
                    ordersizeFutures = cash_stock / ticker2MarketDataQ
                    odpriceF = ticker2MarketDataQ
                else:
                    ordersizeFutures = 0
                    odpriceF = 0
                if ordersizeFutures == 0 and ordersizeStock == 0:
                    return
                from datetime import datetime
                now = datetime.now()
                sampleOrder1 = SingleStockOrder(
                    ticker="2610",
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder1.orderID = f"{self.getStratID()}-2610-{str(uuid1())}"
                sampleOrder1.type = "MO"
                sampleOrder1.currStatus = "New"
                sampleOrder1.currStatusTime = now.time()
                sampleOrder1.direction = direction_stock
                sampleOrder1.size = ordersizeStock
                sampleOrder1.stratID = self.getStratID()

                sampleOrder2 = SingleStockOrder(
                    ticker="3374",
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder2.orderID = f"{self.getStratID()}-3374-{str(uuid1())}"
                sampleOrder2.type = "LO"
                sampleOrder2.currStatus = "New"
                sampleOrder2.currStatusTime = now.time()
                sampleOrder2.direction = direction_futures###1 = buy; -1 = sell
                sampleOrder2.size = ordersizeFutures
                sampleOrder2.price = ticker2RecentMarketData['bidPrice5'].item()
                sampleOrder2.stratID = self.getStratID()

                self.submitted_order.append(sampleOrder1.orderID)
                self.submitted_order.append(sampleOrder2.orderID)
                self.untreated_order.append(sampleOrder1.orderID)
                self.untreated_order.append(sampleOrder2.orderID)
                ######return a list
                return [sampleOrder1,sampleOrder2]
        else:
            #######on receive execution
            order = self.orderManager.lookupOrderID(execution.orderID)

            ######do something
            if order.currStatus == 'executed':
                self.untreated_order.remove(order.orderID)
                ticker, tradesize, direction, tradeprice = order.ticker, order.size, order.direction,order.price
                self.position[ticker].append(self.position[ticker][-1] + tradesize * direction)
                self.pnl.append(self.pnl[-1] - tradesize * direction * tradeprice)
                self.cash.append(self.cash[-1] - tradesize * direction * tradeprice)
                '''price1 和 price2 应该取最新的成交价，具体那个字段我目前不太清楚'''

                self.networth.append(self.position[self.tickers[0]][-1] * price1 + self.position[self.tickers[1]][
                    -1] * price2 + self.cash[-1])

            ####e.g. issue cancel order
            for id in self.untreated_order:
                order = self.orderManager.lookupOrderID(id)
                if order.type == 'LO':
                    now = datetime.now()
                    currentTime = now.time()
                    if order.submissionTime < currentTime - timedelta(seconds=10):
                        self.untreated_order.remove(id)
                        order.type = 'CANCEL'
                    elif order.ticker == self.tickers[0]:
                        if self.lastdirection1 != order.direction:
                            order.type = 'CANCEL'
                            self.untreated_order.remove(id)
                    elif order.ticker == self.tickers[1]:
                        if self.lastdirection2 != order.direction:
                            order.type = 'CANCEL'
                            self.untreated_order.remove(id)
                    else:
                        continue
            # if order.type == "LO":
            #     order.type = "CANCEL"
            return []



        return []
