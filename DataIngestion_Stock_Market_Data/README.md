### Data Ingestion for Stock Market Data

We use yahoo-historical package to scrap stock price data from Yahoo! Finance. 
Simply run:

`python dump_stock_market_data.py`

Make sure that you have the tickers folder at current directory. 

```
- DataIngestion_Stock_Market_Data
    - tickers
        - nyse.csv
        - amex.csv
        - nasdaq.csv
        - custom.csv 
    - dump_stock_market_data.py
```

You could only put the tickers you want in `custom.csv` and make corresponding changes in the python script. 

*The `sbatch_task` is a shell script I use on NYU HPC cluster.
