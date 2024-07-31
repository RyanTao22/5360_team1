import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import random
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objs as go
from multiprocessing import Process, Queue
from marketDataService import MarketDataService
from futureDataService import FutureDataService
from UnifiedDataService import UnifiedDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import time
from marketDataServiceConfig import MarketDataServiceConfig
import os
import pandas as pd

def calculate_indicators(net_worth_list, baseline_networth, initial_cash, timestamps, annual_risk_free_rate=0.04):
    trading_minutes_per_day = 4 * 60
    trading_days_per_year = 252

    returns = np.diff(net_worth_list) / net_worth_list[:-1]
    baseline_returns = np.diff(baseline_networth) / baseline_networth[:-1]

    # Annualized risk-free rate per minute
    rf_per_minute = annual_risk_free_rate / (trading_days_per_year * trading_minutes_per_day)

    strategy_return = (net_worth_list[-1] - initial_cash) / initial_cash
    baseline_return = (baseline_networth[-1] - initial_cash) / initial_cash
    excess_return = strategy_return - baseline_return

    # Beta calculation
    covariance = np.cov(returns, baseline_returns)[0, 1]
    variance = np.var(baseline_returns)
    beta = covariance / variance

    # Alpha calculation
    alpha = excess_return - beta * (baseline_return - rf_per_minute * trading_days_per_year * trading_minutes_per_day)

    # Sharpe Ratio
    sharpe_ratio = (np.mean(returns) - rf_per_minute) / np.std(returns) * np.sqrt(trading_days_per_year * trading_minutes_per_day)

    # High watermark and drawdown
    highwatermark = np.maximum.accumulate(net_worth_list)
    drawdown = np.array(net_worth_list) / highwatermark - 1
    max_drawdown = np.min(drawdown)

    # Sortino Ratio
    downside_returns = returns[returns < 0]
    sortino_ratio = (np.mean(returns) - rf_per_minute) / np.std(downside_returns) * np.sqrt(trading_days_per_year * trading_minutes_per_day)

    # Average daily excess return
    average_daily_excess_return = np.mean(returns - (baseline_return / len(returns)))

    # Max excess return drawdown
    excess_return_drawdown = 1 - (1 + returns) / np.maximum.accumulate(1 + returns)
    max_excess_return_drawdown = np.max(excess_return_drawdown)
    
    # Excess return Sharpe ratio
    excess_return_sharpe_ratio = (np.mean(returns - (baseline_return / len(returns))) - rf_per_minute) / np.std(returns - (baseline_return / len(returns))) * np.sqrt(trading_days_per_year * trading_minutes_per_day)

    # Signal Ratio
    signal_ratio = (np.mean(returns) - rf_per_minute) / np.std(returns)
    
    # Strategy and benchmark volatility
    strategy_volatility = np.std(returns) * np.sqrt(trading_days_per_year * trading_minutes_per_day)
    benchmark_volatility = np.std(baseline_returns) * np.sqrt(trading_days_per_year * trading_minutes_per_day)

    # Max drawdown period
    start_idx = np.where(drawdown == max_drawdown)[0][0]
    end_idx = np.argmax(highwatermark[:start_idx])
    max_drawdown_period = (timestamps[start_idx], timestamps[end_idx])

    results = {
        'strategy_return': strategy_return,
        'excess_return': excess_return,
        'baseline_return': baseline_return,
        'alpha': alpha,
        'beta': beta,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown,
        'sortino_ratio': sortino_ratio,
        'average_daily_excess_return': average_daily_excess_return,
        'max_excess_return_drawdown': max_excess_return_drawdown,
        'excess_return_sharpe_ratio': excess_return_sharpe_ratio,
        'signal_ratio': signal_ratio,
        'strategy_volatility': strategy_volatility,
        'benchmark_volatility': benchmark_volatility,
        'max_drawdown_period': max_drawdown_period
    }
    return results

def run_backtest(backtest_2_dash_q, startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest):
    '''Function to start the processes of the system'''
    marketData_2_exchSim_q = Queue()
    marketData_2_platform_q = Queue()
    futureData_2_exchSim_q = Queue()
    futureData_2_platform_q = Queue()
    platform_2_exchSim_order_q = Queue()
    exchSim_2_platform_execution_q = Queue()
    platform_2_futuresExchSim_order_q = Queue()
    platform_2_strategy_md_q = Queue()
    strategy_2_platform_order_q = Queue()
    platform_2_strategy_execution_q = Queue()
    analysis_q = Queue()
    isReady = None
    resampleFreq = '1T'
    #resampleFreq = None

    backTest = True if backTest == 'True' else False
    if backTest == False: resampleFreq = None

    fds = FutureDataService(futureData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, resampleFreq, isReady)
    mds = MarketDataService(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, resampleFreq, isReady)
    Process(name='uds', target=UnifiedDataService, args=(mds, fds)).start()
    Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)).start()
    Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, futuresCodes, isReady, debug)).start()
    Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, isReady, debug)).start()
    net_worth_list = []
    timestamps = []
    baseline_networth = []

    '''Receive analysis data from the queue and send it to the dashboard'''
    while True:
        data = analysis_q.get()
        if 'signal' in data and data['signal'] == 'EndOfData':
            # print('----------tmp: EndOfData--------')
            # backtest_2_dash_q.put(('Done', net_worth_list, timestamps, baseline_networth))
            break
        net_worth_list.append(data['networth'][0])
        timestamps.append(data['timestamp'][0])
        baseline_networth.append(initial_cash)
        baseline_networth[-1] = baseline_networth[-1] * (1 + 0.0001 * random.randint(-10, 10))
        backtest_2_dash_q.put((net_worth_list, timestamps, baseline_networth))
    
def back_test_analysis():
    '''Function to build and start the dashboard'''
    app = dash.Dash(__name__)
    backtest_2_dash_q = Queue()

    app.layout = html.Div([
        html.Div([
            html.Label('Start Date', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='start_date', value='20240628', type='text', style={'margin-right': '10px', 'width': '80px'}),
            html.Label('End Date', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='end_date', value='20240628', type='text', style={'margin-right': '10px', 'width': '80px'}),
            html.Label('Start Time', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='start_time', value=121000000, type='number', style={'margin-right': '10px', 'width': '100px'}),
            html.Label('Initial Cash', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='initial_cash', value=1000000, type='number', style={'margin-right': '10px', 'width': '100px'}),
            html.Label('Play Speed', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='play_speed', value=1, type='number', style={'margin-right': '10px', 'width': '60px'}),
            html.Label('ticker1', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='ticker1', value='2618', type='text', style={'margin-right': '10px', 'width': '60px'}),
            html.Label('ticker2', style={'font-weight': 'bold', 'margin-right': '10px'}),
            dcc.Input(id='ticker2', value='HSF1', type='text', style={'margin-right': '10px', 'width': '60px'}),
            html.Div([
                html.Label('BackTest', style={'font-weight': 'bold', 'margin-right': '10px'}),
                dcc.RadioItems(
                    id='backTest',
                    options=[
                        {'label': 'BackTest', 'value': 'True'},
                        {'label': 'RePlay', 'value': 'False'}
                    ],
                    value='True',
                    labelStyle={'display': 'inline-block', 'margin-right': '10px'}
                ),
                html.Button('Run Backtest', id='run_backtest', n_clicks=0, style={'background-color': '#4CAF50', 'color': 'white', 'padding': '5px 10px', 'border': 'none', 'cursor': 'pointer', 'font-weight': 'bold'})
            ], style={'margin-left': 'auto', 'display': 'flex', 'align-items': 'center'}),
        ], style={'display': 'flex', 'flex-wrap': 'wrap', 'align-items': 'center'}),
        html.Div([
            html.Table([
                html.Tr([
                    html.Th('Strategy Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Excess Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Baseline Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}),
                    html.Th('Alpha', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Beta', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Sharpe Ratio', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Max Drawdown', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Sortino Ratio', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'})
                ]),
                html.Tr([
                    html.Td(id='strategy_return', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='excess_return', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='baseline_return', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='alpha', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='beta', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='sharpe_ratio', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='max_drawdown', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='sortino_ratio', children='--', style={'text-align': 'center', 'padding': '10px'})
                ]),
                html.Tr([
                    html.Th('Avg Daily Excess Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Max Excess Return Drawdown', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Excess Return Sharpe Ratio', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}),
                    html.Th('Signal Ratio', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Strategy Volatility', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Benchmark Volatility', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}),
                    html.Th('Max Drawdown Period', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'})
                ]),
                html.Tr([
                    html.Td(id='average_daily_excess_return', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='max_excess_return_drawdown', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='excess_return_sharpe_ratio', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='signal_ratio', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='strategy_volatility', children='--', style={'text-align': 'center', 'padding': '10px'}), 
                    html.Td(id='benchmark_volatility', children='--', style={'text-align': 'center', 'padding': '10px'}),
                    html.Td(id='max_drawdown_period', children='--', style={'text-align': 'center', 'padding': '10px'})
                ])
            ], style={'border-collapse': 'collapse', 'width': '100%', 'margin-bottom': '20px'})
        ]),
        html.Div([
            dcc.Graph(id='networth_graph')
        ]),
        dcc.Interval(id='interval_component', interval=1*1000, n_intervals=0)
    ])

    @app.callback(
        Output('interval_component', 'disabled'),
        [
            Input('run_backtest', 'n_clicks'),
        ],
        [
            State('start_date', 'value'),
            State('end_date', 'value'),
            State('start_time', 'value'),
            State('initial_cash', 'value'),
            State('play_speed', 'value'),
            State('ticker1', 'value'),
            State('ticker2', 'value'),
            State('backTest', 'value')
        ]
    )
    def start_backtest(n_clicks, start_date, end_date, start_time, initial_cash, play_speed, ticker1, ticker2, backTest):
        if n_clicks > 0:
            start_date = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            stock_codes_full = ['0050', '2392', '2498', '2610', '2618', '3035', '3264', '3374', '5347', '6443']
            futures_codes_full = ['DBF1', 'GLF1', 'HCF1', 'HSF1', 'IPF1', 'NEF1', 'NLF1', 'NYF1', 'QLF1', 'RLF1']

            stockCodes = []
            futuresCodes = []
            
            if ticker1 in stock_codes_full: stockCodes.append(ticker1)
            else: futuresCodes.append(ticker1)
            if ticker2 in stock_codes_full: stockCodes.append(ticker2)
            else: futuresCodes.append(ticker2)

            debug = False
            Process(target=run_backtest, args=(backtest_2_dash_q, start_date, end_date, start_time, stockCodes, futuresCodes, play_speed, initial_cash, debug, backTest)).start()
            return False
        return True
    
    @app.callback(
        Output('networth_graph', 'figure'),
        [Input('interval_component', 'n_intervals')]
    )
    def update_graph_live(n):

        while not backtest_2_dash_q.empty():
            data = backtest_2_dash_q.get()

            net_worth_list, timestamps, baseline_networth = data
            adjusted_timestamps = [ts for ts in timestamps if ts.time() >= datetime.strptime('09:30:00', '%H:%M:%S').time() and ts.time() <= datetime.strptime('13:30:00', '%H:%M:%S').time()]
            adjusted_net_worth_list = [nw for ts, nw in zip(timestamps, net_worth_list) if ts in adjusted_timestamps]

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=adjusted_timestamps,
                y=adjusted_net_worth_list,
                mode='lines',
                name='Net Worth'
            ))
            fig.update_xaxes(showgrid=False,
                        zeroline=False,
                        showticklabels=True,
                        showspikes=True,
                        spikemode='across',
                        spikesnap='cursor',
                        showline=False,
                        rangebreaks=[dict(bounds=[16,9.5], pattern='hour')],
                        rangeslider_visible=False)

            fig.update_layout(
                title='Backtest Net Worth Curve',
                xaxis_title='Time',
                yaxis_title='Net Worth',
                showlegend=True
            )
            return fig
        return dash.no_update
    
    @app.callback(
        [
            Output('strategy_return', 'children'),
            Output('excess_return', 'children'),
            Output('baseline_return', 'children'),
            Output('alpha', 'children'),
            Output('beta', 'children'),
            Output('sharpe_ratio', 'children'),
            Output('max_drawdown', 'children'),
            Output('sortino_ratio', 'children'),
            Output('average_daily_excess_return', 'children'),
            Output('max_excess_return_drawdown', 'children'),
            Output('excess_return_sharpe_ratio', 'children'),
            Output('signal_ratio', 'children'),
            Output('strategy_volatility', 'children'),
            Output('benchmark_volatility', 'children'),
            Output('max_drawdown_period', 'children')
        ],
        [Input('interval_component', 'n_intervals'),
         ]
    )
    def update_indicators(n):

        if not backtest_2_dash_q.empty():
        
            data = backtest_2_dash_q.get()
            if len(data[1])<=30:return dash.no_update
                
            #data = final_results
            #print('----------Updating indicators--------')
            net_worth_list, timestamps, baseline_networth = data
            results = calculate_indicators(net_worth_list, baseline_networth, net_worth_list[0], timestamps)

            def format_percentage(value):
                return f'{value * 100:.2f}%'

            def color_value(value):
                if value > 0:
                    return html.Span(format_percentage(value), style={'color': 'green'})
                elif value < 0:
                    return html.Span(format_percentage(value), style={'color': 'red'})
                else:
                    return html.Span(format_percentage(value), style={'color': 'black'})

            return (
                color_value(results['strategy_return']),
                color_value(results['excess_return']),
                color_value(results['baseline_return']),
                format_percentage(results['alpha']),
                format_percentage(results['beta']),
                format_percentage(results['sharpe_ratio']),
                format_percentage(results['max_drawdown']),
                format_percentage(results['sortino_ratio']),
                format_percentage(results['average_daily_excess_return']),
                format_percentage(results['max_excess_return_drawdown']),
                format_percentage(results['excess_return_sharpe_ratio']),
                format_percentage(results['signal_ratio']),
                format_percentage(results['strategy_volatility']),
                format_percentage(results['benchmark_volatility']),
                str(results['max_drawdown_period'][1].strftime('%Y%m%d %H:%M:%S%z')+';'+results['max_drawdown_period'][0].strftime('%Y%m%d %H:%M:%S%z'))
            )
        
        return dash.no_update

    app.run_server(debug=True)

if __name__ == '__main__':
    back_test_analysis()
