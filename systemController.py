import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import random
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objs as go
from multiprocessing import Process, Queue, set_start_method
from marketDataService import MarketDataService
from futureDataService import FutureDataService
from exchangeSimulator import ExchangeSimulator
from quantTradingPlatform import TradingPlatform
import os
import pandas as pd
import time

# Explicitly set the start method to 'spawn'
#set_start_method('spawn', force=True)

def calculate_indicators(net_worth_list, baseline_networth, initial_cash, timestamps, n_loops_a_day,annual_risk_free_rate=0.04):
    returns = np.diff(net_worth_list) / net_worth_list[:-1]
    # Metrics Calculation
    strategy_return = (net_worth_list[-1] - initial_cash) / initial_cash
    annualized_return = ((1 + strategy_return) ** (252 / len(set([t.date() for t in timestamps]))) - 1)
    baseline_return = (baseline_networth[-1] - initial_cash) / initial_cash
    excess_return = strategy_return - baseline_return
    beta = np.cov(returns, np.diff(baseline_networth) / baseline_networth[:-1])[0, 1] / np.var(np.diff(baseline_networth) / baseline_networth[:-1])
    alpha = excess_return - beta * (baseline_return - annual_risk_free_rate / 252)
    sharpe_ratio = (np.mean(returns) - annual_risk_free_rate / 252) / np.std(returns) * np.sqrt(252 * n_loops_a_day)
    
    highwatermark = np.maximum.accumulate(net_worth_list)
    drawdown = np.array(net_worth_list) / highwatermark - 1
    max_drawdown = np.min(drawdown)
    
    sortino_ratio = (np.mean(returns) - annual_risk_free_rate / 252) / np.std([r for r in returns if r < 0]) * np.sqrt(252 * n_loops_a_day)
    average_daily_excess_return = np.mean(returns - (baseline_return / len(returns)))
    excess_return_drawdown = 1 - np.array(returns) / np.maximum.accumulate(returns)
    max_excess_return_drawdown = np.max(excess_return_drawdown)
    excess_return_sharpe_ratio = (np.mean(returns - (baseline_return / len(returns))) - annual_risk_free_rate / 252) / np.std(returns - (baseline_return / len(returns))) * np.sqrt(252 * n_loops_a_day)
    signal_ratio = (np.mean(returns) - annual_risk_free_rate / 252) / np.std(returns)
    strategy_volatility = np.std(returns) * np.sqrt(252 * n_loops_a_day)
    benchmark_volatility = np.std(np.diff(baseline_networth) / baseline_networth[:-1]) * np.sqrt(252 * n_loops_a_day)
    
    start_idx = np.where(drawdown == max_drawdown)[0][0]
    end_idx = np.argmax(highwatermark[:start_idx])
    
    max_drawdown_period = (timestamps[start_idx], timestamps[end_idx])

    results = {
        'strategy_return': strategy_return,
        'annualized_return': annualized_return,
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


def run_backtest(startDate, endDate, startTime, stockCodes, futuresCodes, playSpeed, initial_cash, debug, backTest):
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

    processes = []

    # try:
    if futuresCodes == []:
        processes.append(Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, isReady)))
        processes.append(Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)))
    elif stockCodes == []:
        processes.append(Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, futureData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)))
        processes.append(Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, futuresCodes, isReady, debug)))
    else:
        processes.append(Process(name='md', target=MarketDataService, args=(marketData_2_exchSim_q, marketData_2_platform_q, startDate, endDate, startTime, stockCodes, playSpeed, backTest, isReady)))
        processes.append(Process(name='futured', target=FutureDataService, args=(futureData_2_exchSim_q, futureData_2_platform_q, startDate, endDate, startTime, futuresCodes, playSpeed, backTest, isReady)))
        processes.append(Process(name='stockExchange', target=ExchangeSimulator, args=(marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q, stockCodes, isReady, debug)))
        processes.append(Process(name='futureExchange', target=ExchangeSimulator, args=(futureData_2_exchSim_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, futuresCodes, isReady, debug)))

    processes.append(Process(name='platform', target=TradingPlatform, args=(marketData_2_platform_q, platform_2_exchSim_order_q, platform_2_futuresExchSim_order_q, exchSim_2_platform_execution_q, stockCodes, futuresCodes, initial_cash, analysis_q, isReady, debug)))
    
    for p in processes:
        p.start()
        #time.sleep(1)  # Ensure processes start in sequence to avoid race conditions

    net_worth_list = []
    timestamps = []
    baseline_networth = []

    while True:
        data = analysis_q.get()
        if 'signal' in data and data['signal'] == 'EndOfData':
            break
        net_worth_list.append(data['networth'])
        timestamps.append(data['timestamp'])
        #baseline_networth.append(data['baseline_networth'])
        baseline_networth.append(initial_cash)
        baseline_networth[-1] = baseline_networth[-1] * (1 + 0.0001 * random.randint(-10, 10))
    # finally:
    #     for p in processes:
    #         print(p.name,'terminate')
    #         p.terminate()

    return net_worth_list, timestamps, baseline_networth

def back_test_analysis():
    app = dash.Dash(__name__)

    app.layout = html.Div([
        html.Div([
            html.Label('Start Date', style={'font-weight': 'bold', 'margin-bottom': '10px'}),
            dcc.Input(id='start_date', value='20240628', type='text', style={'margin-bottom': '10px'}),
            html.Label('End Date', style={'font-weight': 'bold'}),
            dcc.Input(id='end_date', value='20240628', type='text', style={'margin-bottom': '10px'}),
            html.Label('Start Time', style={'font-weight': 'bold'}),
            # should be int
            dcc.Input(id='start_time', value=132315869, type='number', style={'margin-bottom': '10px'}),
            html.Label('Initial Cash', style={'font-weight': 'bold'}),
            dcc.Input(id='initial_cash', value=1000000, type='number', style={'margin-bottom': '10px'}),
            html.Label('Play Speed', style={'font-weight': 'bold'}),
            dcc.Input(id='play_speed', value=10000000, type='number', style={'margin-bottom': '10px'}),
            html.Label('ticker1', style={'font-weight': 'bold'}),
            dcc.Input(id='ticker1', value='2610', type='text', style={'margin-bottom': '10px'}),
            html.Label('ticker2', style={'font-weight': 'bold'}),
            dcc.Input(id='ticker2', value='3374', type='text', style={'margin-bottom': '10px'}),
            html.Button('Run Backtest', id='run_backtest', n_clicks=0, style={'background-color': '#4CAF50', 'color': 'white', 'padding': '10px', 'border': 'none', 'cursor': 'pointer', 'font-weight': 'bold', 'float': 'right'}),

            # todo: BackTest按钮
            # dcc.Checklist('Back Test', id='back_test', value=True, style={'margin-bottom': '10px'}),
            # dcc.RadioItems(
            #     id='back_test',
            #     options=[
            #         {'label': 'Yes', 'value': True}, 
            #         {'label': 'No', 'value': False},
            #     ],
            #     value=True,
            #     style={'margin-bottom': '10px'},
            #     inline=True
            # ),
            
        ], style={'margin-bottom': '20px', 'background-color': '#f2f2f2'}),
        html.Div([
            html.Table([
                html.Tr([
                    html.Th('Strategy Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
                    html.Th('Annualized Return', style={'text-align': 'center', 'background-color': '#f2f2f2', 'padding': '10px'}), 
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
                    html.Td(id='annualized_return', children='--', style={'text-align': 'center', 'padding': '10px'}),
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
        ])
    ])

    @app.callback(
        [
            Output('strategy_return', 'children'),
            Output('annualized_return', 'children'),
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
            Output('max_drawdown_period', 'children'),
            Output('networth_graph', 'figure'),
        ],
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
        ]
    )
    def update_backtest(n_clicks, start_date,  end_date, start_time, initial_cash, play_speed,ticker1,ticker2):
        if n_clicks == 0:
            return ['--']*16 + [{}]

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

        '''此处设置为很大，以缩短使得到结果。可根据需求改小'''
        # start_time = 132315869
        # play_speed = 1000
        debug = False
        backTest = True
        

        net_worth_list, timestamps, baseline_networth = run_backtest(start_date, end_date, start_time, stockCodes, futuresCodes, play_speed, initial_cash, debug, backTest)
        
        #pd.DataFrame({'timestamp':timestamps,'net_worth':net_worth_list}).to_csv('net_worth.csv',index=False)
        print(net_worth_list)
        print(baseline_networth)

        '''调整由Queue得到的数据格式，以便于计算'''
        timestamps = [ts[0] for ts in timestamps]
        net_worth_list = [nw[0] for nw in net_worth_list]

        results = calculate_indicators(net_worth_list, baseline_networth, initial_cash, timestamps,n_loops_a_day = int(len(timestamps) / len(set([t.date() for t in timestamps]))))
        

        def format_percentage(value):
            return f'{value * 100:.2f}%'

        def color_value(value):
            if value > 0:
                return html.Span(format_percentage(value), style={'color': 'green'})
            elif value < 0:
                return html.Span(format_percentage(value), style={'color': 'red'})
            else:
                return html.Span(format_percentage(value), style={'color': 'black'})

        adjusted_timestamps = []
        adjusted_net_worth_list = []
        for ts, nw in zip(timestamps, net_worth_list):
            if ts.time() >= datetime.strptime('09:30:00', '%H:%M:%S').time() and ts.time() <= datetime.strptime('13:30:00', '%H:%M:%S').time():
                adjusted_timestamps.append(ts)
                adjusted_net_worth_list.append(nw)

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

        return (
            color_value(results['strategy_return']),
            color_value(results['annualized_return']),
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
            str(results['max_drawdown_period'][1].strftime('%Y%m%d %H:%M:%S%z')+';'+results['max_drawdown_period'][0].strftime('%Y%m%d %H:%M:%S%z')),
            fig
        )
    
    app.run_server(debug=True)

#back_test_analysis()



if __name__ == '__main__':
    back_test_analysis()