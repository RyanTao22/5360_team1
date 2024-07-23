from pathlib import Path
from os.path import join

parent_dir = Path(__file__).parent
class MarketDataServiceConfig:
    #v1 config
    targetDate = '2024-06-28'
    stockCodes = ['0050', '2392', '2498', '2610', '2618', '3035', '3264', '3374', '5347', '6443']
    mainDir = join(parent_dir,"processedData_2024")#'V:\\notebook\\project\\processedData_2024'
    mainZip = 'V:\\notebook\\project\\processedData_2024.zip'
    main = 'V:\\notebook\\project'
    stocksPath = '\\stocks\\'
    playSpeed = 0.2

    #config added for v2
    startTime = 90515869  # hhmmss.sss
    futureCodes = ['DBF1', 'GLF1', 'HCF1', 'HSF1', 'IPF1', 'NEF1', 'NLF1', 'NYF1', 'QLF1', 'RLF1']
    futuresQuotesPath = '\\futuresQuotes\\'
    futuresTradesPath = '\\futuresTrades\\'
