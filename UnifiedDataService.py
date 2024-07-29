from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from marketDataService import MarketDataService
from futureDataService import FutureDataService
import pandas as pd
import datetime
import time
class UnifiedDataService:
        def __init__(self, mds: MarketDataService,fds: FutureDataService):
            self.mds = mds
            self.fds = fds
            self.produce_data()


        def produce_data(self):
            self.fullData = pd.concat([
                self.mds.cData.assign(type="both"),
                self.fds.tqcData
            ]).sort_values(['date','time'])
            ts_diff = pd.to_datetime(self.fullData['time'], format='%H%M%S%f') - pd.to_datetime(self.fullData['time'].shift(1),format='%H%M%S%f')
            self.fullData['ts_diff'] = ts_diff.dt.total_seconds() * 1000
            self.fullData['ts_diff'] = self.fullData['ts_diff'].fillna(0)

            self.fullData.sort_index(axis=1,inplace=True)####sort column names for easy access to bid/ask(1-5)

            askPrice_cols_list = ['askPrice'+str(i) for i in range(1,6)]
            bidPrice_cols_list = ['bidPrice'+str(i) for i in range(1,6)]
            askSize_cols_list = ['askSize'+str(i) for i in range(1,6)]
            bidSize_cols_list = ['bidSize'+str(i) for i in range(1,6)]

            BP_cols_list = ['BP'+str(i) for i in range(1,6)]
            SP_cols_list = ['SP'+str(i) for i in range(1,6)]
            BV_cols_list = ['BV'+str(i) for i in range(1,6)]
            SV_cols_list = ['SV'+str(i) for i in range(1,6)]


            now = datetime.datetime.now()

            for index, row in self.fullData.iterrows():
                if row['type'] == "both":
                    diff = float(row['ts_diff'])/1000/self.mds.playSpeed
                    # date should be row['date'] in a proper format as now.date() (from 2024-04-01)
                    # time should be row['time'] in a proper format as now.time() (from 90515951: 09:05:15.951000)

                    quoteSnapshot = OrderBookSnapshot_FiveLevels(row.ticker, datetime.datetime.strptime(row['date'], '%Y-%m-%d'),
                                                                 datetime.datetime.strptime(str(row['time']), '%H%M%S%f').time(),
                                                                 bidPrice=[row[BP_cols_list[i]] for i in range(5)],
                                                                 askPrice=[row[SP_cols_list[i]] for i in range(5)],
                                                                 bidSize=[row[BV_cols_list[i]] for i in range(5)],
                                                                 askSize=[row[SV_cols_list[i]] for i in range(5)])
                    quoteSnapshot.type = "both"
                    quoteSnapshot.midQ = row.get("midQ")
                    quoteSnapshot.symbol = row.get("symbol")
                    quoteSnapshot.totalMatchSize = row.get("totalMatchSize")
                    quoteSnapshot.totalMatchValue = row.get("totalMatchValue")
                    quoteSnapshot.avgMatchPx = row.get("avgMatchPx")
                    quoteSnapshot.size = row.get("size")
                    quoteSnapshot.volume = row.get("volume")
                    quoteSnapshot.lastPx = row.get("lastPx")

                    if not self.mds.backTest: time.sleep(diff)
                    self.mds.marketData_2_exchSim_q.put(quoteSnapshot)
                    self.mds.marketData_2_platform_q.put(quoteSnapshot)
                elif row['type'] in ("quotes","trades"):
                    '''如果是trades数据，就跳过'''

                    if row['type'] == 'trades':
                        continue


                    diff = float(row['ts_diff'])/1000/self.fds.playSpeed

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

                    if not self.fds.backTest: time.sleep(diff)
                    self.fds.futureData_2_exchSim_q.put(quoteSnapshot)
                    self.fds.futureData_2_platform_q.put(quoteSnapshot)

            endOfData = OrderBookSnapshot_FiveLevels(row.ticker+'_EndOfData', now.date(), now.time(),
                                                        bidPrice=[0, 0, 0, 0, 0],
                                                        askPrice=[0, 0, 0, 0, 0],
                                                        bidSize=[0, 0, 0, 0, 0],
                                                        askSize=[0, 0, 0, 0, 0])
            self.fds.futureData_2_platform_q.put(endOfData)
            self.mds.marketData_2_platform_q.put(endOfData)


            print()
