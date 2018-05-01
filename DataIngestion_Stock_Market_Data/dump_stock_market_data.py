##############################################################
# Data Ingestion for Stock Market Dataset
# Project: Investment Assistant Based on News Feed Analytics
#
# Author: Xialiang Liu
# E-mail: xialiang.liu@hotmail.com
##############################################################


import os
import numpy as np
import pandas as pd

from yahoo_historical import Fetcher

# Path initailization
TICKER_DIR = "./tickers/"
# List of name for the stock exchanges
EXCHANGES = ['nyse', 'amex', 'nasdaq'] 

# Output files are saved here
OUTPUT_DIR = "/scratch/xl2053/stock_dataset/"
if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)

# Parameters for stock market data ingestion
START = [2010,1,1]
END = [2018,4,1]


for exchange in EXCHANGES:
    
    sub_output_dir = os.path.join(OUTPUT_DIR, exchange)
    if not os.path.exists(sub_output_dir):
        os.mkdir(sub_output_dir)

    ticker_file = TICKER_DIR + exchange + ".csv"
    tickers = pd.read_csv(ticker_file)["Symbol"].values
    #DEBUG
    tickers = ["ALR", "ECOM    ", "MITT^A"]
    
    
    tickers = [t.strip() for t in tickers]

    
    # Keep track of the saved tickers and unsaved tickers
    saved_tickers = []
    failed_tickers = []

    for ticker in tickers:
        # output file name
        output_file = os.path.join(sub_output_dir, ticker+".csv")
        try:
            data = Fetcher(ticker, START, END)
            if data.getHistorical().empty:
                failed_tickers.append(ticker)
            else:
                data.getHistorical().to_csv(output_file, index=False)
                saved_tickers.append(ticker)
        except:
            failed_tickers.append(ticker)
        
    # If there are tickers failed to download, 
    # report the name list of tickers.
    if failed_tickers:
        saved_tickers_output = os.path.join(sub_output_dir, "saved_tickers.txt")
        failed_tickers_output = os.path.join(sub_output_dir, "failed_tickers.txt")
        # Write successful ticker list
        f = open(saved_tickers_output, 'wb')
        f.write("\n".join(saved_tickers))
        f.close()
        # Write failed ticker list
        f = open(failed_tickers_output, 'wb')
        f.write("\n".join(failed_tickers))
        f.close()

