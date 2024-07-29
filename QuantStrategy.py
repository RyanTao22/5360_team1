#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Created on Thu Jun 20 10:26:05 2020

@author: hongsong chou
"""

import os
import time
from typing import Mapping
from uuid import uuid1

import pandas as pd

from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Platform.OrderManager import OrderManager
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution
from datetime import datetime, timedelta
import numpy as np
import lightgbm as lgb
from collections import deque
from copulae.elliptical import GaussianCopula
from copulae.archimedean import ClaytonCopula,GumbelCopula,FrankCopula
from sklearn.linear_model import LinearRegression
from scipy.integrate import quad
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
            #print(execution.outputAsArray())
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
        self.ticker = ticker
        
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

            ticker1MarketData:list[pd.DataFrame] = self.tickers2Snapshots['stocks'][ticker1]
            ticker2MarketData:list[pd.DataFrame] = self.tickers2Snapshots['futures_quotes'][ticker2]
            ticker2MarketData_trades:list[pd.DataFrame] = self.tickers2Snapshots['futures_trades'][ticker2]

            ticker1RecentMarketData = None
            ticker2RecentMarketData = None
            ticker2RecentMarketData_trades = None

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


            if len(ticker2MarketData_trades) > 0:
                ticker2RecentMarketData_trades = ticker2MarketData_trades[-1]

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
                sampleOrder1.orderID = f"{self.getStratID()}-{ticker1}-{str(uuid1())}"
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
                sampleOrder2.orderID = f"{self.getStratID()}-{ticker2}-{str(uuid1())}"
                sampleOrder2.type = "LO"
                sampleOrder2.currStatus = "New"
                sampleOrder2.currStatusTime = now.time()
                sampleOrder2.direction = 1###1 = buy; -1 = sell
                sampleOrder2.size = 2
                sampleOrder2.price = ticker2RecentMarketData['bidPrice5'].item()
                sampleOrder2.stratID = self.getStratID()


                ######return a list
                print(sampleOrder1,sampleOrder2)
                return [sampleOrder1,sampleOrder2]
        else:
            #######on receive execution
            order = self.orderManager.lookupOrderID(execution.orderID)
            #print('execution!!!!!!')
            print(order)


            '''记录持仓'''
            if execution.price is None and execution.size is None: return [] ######CANCEL order will produce an execution with None price and None size
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
            if order.currStatus == "PartiallyFilled":
                from datetime import datetime
                now = datetime.now()
                sampleOrder3 = order.copyOrder()
                sampleOrder3.type = "CANCEL"
                return [sampleOrder3]

            return []



        return []


class InDevelopingStrategy(QuantStrategy):
    def __init__(self, stratID, stratName, stratAuthor, day, ticker,
                 tickers2Snapshots: Mapping[str,OrderBookSnapshot_FiveLevels],
                 orderManager:OrderManager, initial_cash, analysis_queue):
        super().__init__(stratID,stratName,stratAuthor,ticker,day)
        self.tickers2Snapshots = tickers2Snapshots
        self.orderManager = orderManager
        self.day = day  # public field
        self.initial_cash = initial_cash
        # Create the declarative base
        self.timestamp = []
        self.cash = [self.initial_cash]
        self.limit_cash = self.initial_cash * 0.05
        self.absoluteCash = self.limit_cash
        self.lastorder = 0
        self.cashCostRatio = 0.05
        self.networth = [0]
        self.pnl = [0]
        self.position = {ticker[0]:[0], ticker[1]: [0]} #这个应该是一个{}的形状
        self.ret = []
        self.current_idx = 0
        self.tickers = ticker
        self.current_time = 0
        self.orderSIdx = 0
        self.orderFIdx = 0
        self.lastdirection1 = 0
        self.lastdirection2 = 0
        self.stockdf = [pd.DataFrame()]
        self.futuredfQ = [pd.DataFrame()]
        # self.futuredfT = [pd.DataFrame()]
        '''记录所有发出的订单'''
        self.submitted_order = [] #(ordertime, orderid, orderprice, ordersize, direction)
        '''记录未成交或者成交后有残余的订单'''
        self.untreated_order = [] #(ordertime, orderid, orderprice, ordersize, direction)

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


    def gain_timeindex(self, start_time, end_time):
        '''Get minute level intervals'''
        time_interval = timedelta(minutes=1)
        time_data = []
        current_time = start_time
        while current_time <= end_time:
            time_data.append(current_time.strftime('%H:%M'))
            current_time += time_interval
        return time_data

    def gain_datetime(self, time, date='2023-01-01'):
        '''Convert Time Type'''
        year = int(date[:4])
        month = int(date[5:7])
        day = int(date[8:10])
        hour = time.hour
        minute = time.minute
        second = time.second
        dt = datetime(year, month, day, hour, minute, second)
        return dt

    def stock_dataprocess(self, rawStock):
        '''Processing Stock Data'''
        df = rawStock.copy()
        df = df.reset_index(drop=True)
        start_time = self.gain_datetime(df['time'].iloc[0])
        end_time = self.gain_datetime(df['time'].iloc[-1])
        time_data = self.gain_timeindex(start_time, end_time)

        df['time_trans'] = df['time'].astype('str').str.zfill(9).apply(lambda x: x[:2]  + x[2:5])
        df['lastPx'] = df['lastPx'].fillna(method='ffill')
        df_min = pd.DataFrame()
        grouped = df.groupby('time_trans')
        df_min['high'] = grouped['lastPx'].max()
        df_min['low'] = grouped['lastPx'].min()
        df_min['close'] = grouped['lastPx'].last()
        df_min['open'] = grouped['lastPx'].first()

        cols = ['date',"volume",
                  'askPrice5','askPrice4','askPrice3','askPrice2','askPrice1', \
                  'bidPrice1','bidPrice2','bidPrice3','bidPrice4','bidPrice5', \
                  'askSize5','askSize4','askSize3','askSize2','askSize1', \
                  'bidSize1','bidSize2','bidSize3','bidSize4','bidSize5']

        for col in cols:
            df_min[col] = grouped[col].last()
        df_min = df_min.reindex(time_data)
        df_min.index.name = 'time'
        df_min.reset_index(inplace=True)
        return df_min

    def future_dataprocess(self, rawFutureQ):
        '''Processing Future Data'''
        df_Quotes = rawFutureQ.copy()
        df_Quotes = df_Quotes.reset_index(drop=True)
        start_time = self.gain_datetime(df_Quotes['time'].iloc[0])
        end_time = self.gain_datetime(df_Quotes['time'].iloc[-1])
        time_data_ft = self.gain_timeindex(start_time, end_time)
        df_Quotes['time_trans'] = df_Quotes['time'].astype('str').str.zfill(9).apply(lambda x: x[:2] + x[2:5])

        df_Quotes_min = pd.DataFrame()
        grouped = df_Quotes.groupby('time_trans')
        cols = ['date', 'symbol', 'askPrice5', 'askPrice4', 'askPrice3', 'askPrice2', 'askPrice1', 'bidPrice1',
                'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5', 'askSize5', 'askSize4', 'askSize3', 'askSize2',
                'askSize1', 'bidSize1', 'bidSize2', 'bidSize3', 'bidSize4', 'bidSize5']
        for col in cols:
            df_Quotes_min[col] = grouped[col].last()
        df_Quotes_min = df_Quotes_min.reindex(time_data_ft)
        df_Quotes_min.index.name = 'time'
        df_Quotes_min.reset_index(inplace=True)
        return df_Quotes_min
    def get_data(self, stockdf, futuredfQ):

        '''从project3的传输中得到数据
           n:需要回看的数据长度
            tickers:获取对应标的的数据（tickers可以是单只股票（期货），也可以是一堆，这里也可以考虑每次只取一个ticker的后面再合成
            返回 数据表（如果长度小于n（则要么返回现有长度，要么由于存在回看，小于说明数据量不够，无法回看，直接返回空'''
        
        processedDataStock = self.stock_dataprocess(stockdf)
        processedDataFuture = self.future_dataprocess(futuredfQ)
        # print(processedDataFuture)
        return processedDataStock, processedDataFuture
    def generate_signal_copula(self, df_stock, df_future, past_step, valid_min, trust_prob=0.7, base='future'):
        '''
        Generate strategy-related signals, look back at past_step minutes when passing in new data, and only pass in one pair of stock futures at a time.
        
        Parameters
        ----------
        past_step: int 
            Minutes to watch back
        valid_min: int 
            Signal effective duration (signal frequency)
        trust_prob: float
            Probability threshold
        base: str
            Conditional probability based on which asset
        
        Returns
        -------
        op: int
            Trading signal:
            1: Buy stock, sell future
            0: No act
            -1: Sell stock, buy future
            2: Close all positions
        '''

        if len(df_stock) < past_step + valid_min or len(df_future) < past_step + valid_min:
            return 0
            
        df_stock[f'return_{valid_min}_stock'] = np.log(1 + df_stock['close'].pct_change(periods=valid_min))
        df_future[f'return_{valid_min}_future'] = np.log(1 + df_future.eval('(askPrice1 + bidPrice1) / 2').pct_change(periods=valid_min))
        return_df = pd.merge(df_stock[['time',f'return_{valid_min}_stock']], df_future[['time',f'return_{valid_min}_future']], on='time', how='inner')

        op_lis = []
        sign_record = []
        time = return_df['time'].iloc[-1]

        ret_stock = return_df[f'return_{valid_min}_stock'][-past_step:]
        ret_future = return_df[f'return_{valid_min}_future'][-past_step:]

        Ustock = norm.cdf(ret_stock, np.mean(ret_stock), np.std(ret_stock))
        Ufuture = norm.cdf(ret_future, np.mean(ret_future), np.std(ret_future))
        u = np.vstack((Ustock, Ufuture)).T
        
        if np.isnan(u).any():
            return 0
        
        cop1 = GaussianCopula(dim=2)
        cop2 = ClaytonCopula(dim=2)
        cop3 = GumbelCopula(dim=2)
        # cop4 = FrankCopula(dim=2)

        cop1.fit(u, method='ml')
        cop2.fit(u, method='ml')
        cop3.fit(u, method='ml')
        # cop4.fit(u, method='ml')

        loglik_lis = [cop1.log_lik(u),cop2.log_lik(u),cop3.log_lik(u)]
        max_id = loglik_lis.index(max(loglik_lis))
        cop = [cop1,cop2,cop3][max_id] 

        ret_stock_pred = return_df[f'return_{valid_min}_stock'].iloc[-1] 
        ret_future_pred = return_df[f'return_{valid_min}_future'].iloc[-1]  

        Ustock_pred = norm.cdf(ret_stock_pred, np.mean(ret_stock), np.std(ret_stock))
        Ufuture_pred = norm.cdf(ret_future_pred, np.mean(ret_future), np.std(ret_future))

        def condition(t,upper_bound):
            return 1 if t < upper_bound else 0
        def gain_sign(base):
            if base == 'future':
                numerator, _ = quad(lambda t: cop.pdf([t, Ufuture_pred]) * condition(t,Ufuture_pred), 0, Ufuture_pred)
                sign = numerator
            elif base == 'stock':
                numerator, _ = quad(lambda t: cop.pdf([Ustock_pred,t]) * condition(t,Ustock_pred), 0, Ustock_pred)
                sign = numerator
            
            return sign

        if base == 'future':
            op = 1 if gain_sign(base) < 1-trust_prob else -1 if gain_sign(base) > trust_prob else 2
        elif base == 'stock':
            op = -1 if gain_sign(base) < 1-trust_prob else 1 if gain_sign(base) > trust_prob else 2
        
        op_lis.append({"time":time,"op":op})
        
        return op
    

    def generate_signal_cointegration(self, df_stock, df_future, past_step, sign_last, smooth_min, bounds=[1,2], op_last=None):
        '''
        Generate strategy-related signals, look back at past_step minutes at the beginning of each tick,
        and pass in only one pair of stock futures at a time.

        Parameters
        ----------
        past_step : int
            Number of minutes to look back.
        smooth_min : int
            Number of minutes for smoothing.
        sign_last : int
            Last observed signal.
        op_last : int
            Last operation performed.
        bounds : list
            Bounds for cointegration method.

        Returns
        -------
        op: int
            Trading signal:
            1: Buy stock, sell future
            0: No action
            -1: Sell stock, buy future
            2: Close all positions when mean reversion
            4: Forced closing
        '''

        if len(df_stock) < past_step or len(df_future) < past_step:
            return

        lower_bound = bounds[0]
        upper_bound = bounds[1]


        df_stock['lnP_stock'] = np.log(df_stock['close'])
        df_future['lnP_future'] = np.log(df_future['avgMatchPx'].ffill())

        df_stock[f'lnP_ma{smooth_min}_stock'] = df_stock['lnP_stock'].rolling(smooth_min).mean()
        df_future[f'lnP_ma{smooth_min}_future'] = df_future['lnP_future'].rolling(smooth_min).mean()

        df_price = pd.merge(df_stock[['time', f'lnP_ma{smooth_min}_stock']], df_future[['time',f'lnP_ma{smooth_min}_future']], on='time', how='inner')
        df_price = df_price.dropna().reset_index(drop=True)


        time = df_price['time'].iloc[-1]
        model = LinearRegression()
        x = [x for x in df_price[-past_step:][f'lnP_ma{smooth_min}_future']]
        X = [[k] for k in x]
        y = [ y for y in df_price[-past_step:][f'lnP_ma{smooth_min}_stock']]

        model.fit(X, y)
        y_pred = model.predict(X)
        resids = y - y_pred
        resid_std = np.std(resids)
        sign = resids[-1]/resid_std

        op = []

        if sign_last is None:
            sign_record.append({"time":time,"sign":sign})
        else:
            if sign_last > lower_bound and sign <= lower_bound and sign > 0:
                op.append({"time":time,"op":1})
                op_last = 1
                
            elif sign_last < -lower_bound and sign >= -lower_bound and sign < 0:
                op.append({"time":time,"op":-1})
                op_last = -1

            elif abs(op_last)==1 and abs(sign) >= upper_bound:
                op.append({"time":time,"op":4})
                op_last = 0

            elif abs(op_last)==1 and sign*sign_last<0:
                op.append({"time":time,"op":2}) 
                op_last = 0
            else:
                op.append({"time":time,"op": op_last})
                    
        sign_record = pd.DataFrame(sign_record)
        op = pd.DataFrame(op)
        return op, sign, op_last


    def run(self, execution: SingleStockExecution) -> list[SingleStockOrder]:
        ticker1 = self.ticker[0]
        ticker2 = self.ticker[1]
        flag = 0
        if execution is None:  #######on receive market data
            ####get most recent market data for ticker

            ticker1MarketData:list[pd.DataFrame] = self.tickers2Snapshots['stocks'][ticker1]
            ticker2MarketData:list[pd.DataFrame] = self.tickers2Snapshots['futures_quotes'][ticker2]
            # ticker2MarketData_trades:list[pd.DataFrame] = self.tickers2Snapshots['futures_trades'][ticker2]
            # print(len(ticker2MarketData_trades))

            ticker1RecentMarketData = None
            ticker2RecentMarketData = None
            ticker2RecentMarketData_trades = None


            if len(ticker1MarketData) > 0:
                
                ticker1RecentMarketData = ticker1MarketData[-1]
                '''更新价格1'''
                self.midPrices[ticker1].append(
                    (ticker1RecentMarketData['bidPrice1'] + ticker1RecentMarketData['askPrice1']) / 2)
                # print('更新价格1',ticker1RecentMarketData['time'])
                # df['date'] + pd.to_timedelta(df['time'].astype(str))
                self.timestamp.append(pd.to_datetime(ticker1RecentMarketData['date']) + pd.to_timedelta(
                    ticker1RecentMarketData['time'].astype(str)))
                # self.timestamp.append(pd.to_datetime(ticker1RecentMarketData['date']  + ticker1RecentMarketData['time']))
                # 将新信息存储到历史表列表中
                self.stockdf.append(ticker1RecentMarketData.copy())

            if len(ticker2MarketData) > 0:
                
                ticker2RecentMarketData = ticker2MarketData[-1]
                '''更新价格2'''
                self.midPrices[ticker2].append(
                    (ticker2RecentMarketData['bidPrice1'] + ticker2RecentMarketData['askPrice1']) / 2)
                # print('更新价格2',ticker2RecentMarketData['time'])
                # self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date'].to_string + ' ' + ticker2RecentMarketData['time']))
                # unsupported operand type(s) for +: 'DatetimeArray' and 'str'
                self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date']) + pd.to_timedelta(
                    ticker2RecentMarketData['time'].astype(str)))
                # self.timestamp.append(pd.to_datetime(ticker2RecentMarketData['date'] + ticker2RecentMarketData['time']))
                self.futuredfQ.append(ticker2RecentMarketData.copy())

            # if len(ticker2MarketData_trades) > 0:
            #     flag += 1
            #     ticker2RecentMarketData = ticker2MarketData_trades[-1]
            #     # 更新trade表
            #     self.futuredfT.append(ticker2RecentMarketData.copy())
                
            # print(len(self.stockdf), len(self.futuredfQ), len(self.futuredfT))
           
            netWrorth = self.cash[-1]
            if len(self.positions[ticker1]) > 0:
                netWrorth += self.positions[ticker1][-1] * self.midPrices[ticker1][-1]
            if len(self.positions[ticker2]) > 0:
                netWrorth += self.positions[ticker2][-1] * self.midPrices[ticker2][-1]

            # print(self.timestamp[-1],self.cash[-1],netWrorth)
            '''记录到共享队列analysis_q中'''
            # update analysis: timestamp, networth, cash
            self.analysis_q.put({
                'timestamp': self.timestamp[-1],
                'cash': self.cash[-1],
                'networth': netWrorth,
                'positions_' + ticker1: self.positions[ticker1][-1],
                'positions_' + ticker2: self.positions[ticker2][-1],
                'midPrice_' + ticker1: self.midPrices[ticker1][-1],
                'midPrice_' + ticker2: self.midPrices[ticker2][-1]
            })
            
            
            if len(self.stockdf) > 1 and len(self.futuredfQ) > 1:
                #########do some calculation with the recent market data
                # .....
                
                stock_df = pd.concat(self.stockdf)
                future_dfQ = pd.concat(self.futuredfQ) 
                # print(len(stock_df), len(future_dfQ))
                # future_dfT = pd.concat(self.futuredfT)
                df1, df2 = self.get_data(stock_df, future_dfQ)
                                        

                order = self.generate_signal_copula(df1, df2, 10, valid_min = 3, trust_prob=0.7, base='future')
                if order == 0:
                    return []
                if order != self.lastorder:
                    self.absoluteCash = self.initial_cash
                self.lastorder = order
                if order == 2:
                    print('order:----------------------------------------------------------------', order)
                ordersizeStock, ordersizeFutures, direction_stock, direction_futures = 0, 0, 0, 0
                if order == 1:
                    direction_stock, direction_futures = 1, -1
                elif order == -1:
                    direction_stock, direction_futures = -1, 1
                elif order == 2 and self.lastorder != 2:
                    '''强制平仓'''
                    stockPosition = self.position[ticker1][-1]
                    futurePosition = self.position[ticker2][-1]
                    #如果本来就是空仓，直接返回
                    if stockPosition == 0 and futurePosition == 0:
                        return []
                    if stockPosition != 0:
                        print(stockPosition)
                        ordersizeStock = abs(stockPosition)
                        direction_stock = -np.sign(stockPosition)
                    if futurePosition != 0:
                        print(futurePosition)
                        ordersizeFutures = abs(futurePosition)
                        direction_futures = -np.sign(futurePosition)
                    self.absoluteCash = self.initial_cash
                else:
                    return []
                if self.absoluteCash > self.limit_cash:
                    cash_stock = self.initial_cash * self.cashCostRatio // 2
                    cash_future = self.initial_cash * self.cashCostRatio // 2
                else:
                    print('cash is not enough')
                    return []
                
                #######
                if order != 2:
                    if direction_stock > 0:
                        if (self.lastorder == 0 or self.lastorder == 2) or order == self.lastorder:
                            ordersizeStock = cash_stock // stock_df.iloc[-1,]['askPrice1']
                        else:
                            ordersizeStock = self.position[ticker1][-1] + cash_stock // stock_df.iloc[-1,]['askPrice1']
                    # odprice = stock_df.iloc[-1,]['askPrice1']
                    elif direction_stock < 0:
                        if (self.lastorder == 0 or self.lastorder == 2) or order == self.lastorder:
                            ordersizeStock = cash_stock // stock_df.iloc[-1,]['bidPrice1'] + cash_stock // stock_df.iloc[-1,]['askPrice1']
                        else:
                            ordersizeStock = self.position[ticker1][-1]
                        # print(stock_df.iloc[-1,]['bidPrice1'], ordersizeStock)
                    # odprice = stock_df.iloc[-1,]['bidPrice1']

                    if direction_futures > 0:
                        if (self.lastorder == 0 or self.lastorder == 2) or order == self.lastorder:
                            ordersizeFutures = cash_future // future_dfQ.iloc[-1,]['askPrice1']
                        else:
                            ordersizeFutures= self.position[ticker2][-1] + cash_future // future_dfQ.iloc[-1,]['askPrice1']
                    # odprice = stock_df.iloc[-1,]['askPrice1']
                    elif direction_futures < 0:
                        if (self.lastorder == 0 or self.lastorder == 2) or order == self.lastorder:
                            ordersizeFutures = cash_future// future_dfQ.iloc[-1,]['bidPrice1'] 
                        else:
                            ordersizeFutures = self.position[ticker2][-1] + cash_future // future_dfQ.iloc[-1,]['askPrice1']
                #######
                # if direction_stock != 0:
                #     ordersizeStock = 1
                # if direction_futures != 0:
                #     ordersizeFutures = 1
                if order != 0:
                    print('ordersizeStock',ordersizeStock)
                    print('ordersizeFutures',ordersizeFutures)
                if ordersizeFutures == 0 and ordersizeStock == 0:
                    print(2)
                    return []
                # print(ordersizeStock, ordersizeFutures)
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
                sampleOrder1.direction = direction_stock
                sampleOrder1.size = ordersizeStock
                sampleOrder1.stratID = self.getStratID()

                sampleOrder2 = SingleStockOrder(
                    ticker=ticker2,
                    date=now.date(),
                    submissionTime=now.time()
                )
                sampleOrder2.orderID = f"{self.getStratID()}-3374-{str(uuid1())}"
                sampleOrder2.type = "MO"
                sampleOrder2.currStatus = "New"
                sampleOrder2.currStatusTime = now.time()
                sampleOrder2.direction = direction_futures  ###1 = buy; -1 = sell
                sampleOrder2.size = ordersizeFutures
                sampleOrder2.stratID = self.getStratID()

                self.submitted_order.append(sampleOrder1.orderID)
                self.submitted_order.append(sampleOrder2.orderID)
                self.untreated_order.append(sampleOrder1.orderID)
                self.untreated_order.append(sampleOrder2.orderID)

                ######return a list
                #print(sampleOrder1, sampleOrder2)
                return [sampleOrder1, sampleOrder2]
        else:
            #######on receive execution
            print('execution!!!!!!')
            order = self.orderManager.lookupOrderID(execution.orderID)
            #print(order)
            if order.currStatus == 'Filled':
                self.untreated_order.remove(order.orderID)
                ticker, tradesize, direction, tradeprice = execution.ticker, execution.size, execution.direction, execution.price
                if tradeprice==0:
                    print(f'ticker:{ticker}, tradesize:{tradesize}, direction:{direction}, tradeprice:{tradeprice}')
                self.position[ticker].append(self.position[ticker][-1] + tradesize * direction)
                #self.pnl.append(self.pnl[-1] - tradesize * direction * tradeprice)
                self.cash.append(self.cash[-1] - tradesize * direction * tradeprice)
                self.absoluteCash -= tradesize * tradeprice
                self.execution_record.append({
                    'timestamp': execution.timeStamp,
                    'ticker': ticker,
                    'size': tradesize,
                    'direction': direction,
                    'price': tradeprice
                })

            ####e.g. issue cancel order
            cancelOrders = []
            for id in self.untreated_order:
                order = self.orderManager.lookupOrderID(id)
                if order.type == 'LO':
                    now = datetime.now()
                    currentTime = now.time()
                    if order.submissionTime < currentTime - timedelta(seconds=10):
                        self.untreated_order.remove(id)
                        order.currStatus = 'Cancelled'
                        sampleOrder3 = order.copyOrder()
                        sampleOrder3.type = "CANCEL"
                        cancelOrders.append(sampleOrder3)
                    elif order.ticker == self.tickers[0]:
                        if self.lastdirection1 != order.direction:
                            order.currStatus = 'Cancelled'
                            self.untreated_order.remove(id)
                            sampleOrder3 = order.copyOrder()
                            sampleOrder3.type = "CANCEL"
                            cancelOrders.append(sampleOrder3)
                    elif order.ticker == self.tickers[1]:
                        if self.lastdirection2 != order.direction:
                            order.currStatus = 'Cancelled'
                            self.untreated_order.remove(id)
                            sampleOrder3 = order.copyOrder()
                            sampleOrder3.type = "CANCEL"
                            cancelOrders.append(sampleOrder3)
                elif order.currStatus == "PartiallyFilled":
                    print('partially filled')
                    from datetime import datetime
                    now = datetime.now()
                    sampleOrder3 = order.copyOrder()
                    sampleOrder3.type = "CANCEL"
                    return cancelOrders.append(sampleOrder3)
                else:
                    continue

            # if order.type == "LO":
            #     order.type = "CANCEL"
            return cancelOrders

        return []

