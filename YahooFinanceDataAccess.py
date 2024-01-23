import json
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import time
# Get List from SEC
with open("📈 Data/company_tickers.json", "r") as f:
    tickers = json.load(f)

# print(json.dumps(tickers))
ticker_list = []
for i in tickers.values():
    ticker_list.append(i["ticker"])
print(len(ticker_list),ticker_list)
#Bulk API Requests

#Starts in 1962
data = yf.download("GE")

Stock_Market = pd.DataFrame(index=data.index, columns=["test"])

for ticker in ticker_list[:3]:
    data = yf.download(ticker)
    data[f'{ticker}'] = data.apply(lambda row: (row['Open'], row['High'], row['Low'], row['Close'], row["Volume"]), axis=1)
    data.drop(columns = ["Open", "High", "Low", "Close","Volume", "Adj Close"], inplace=True)
    Stock_Market = pd.merge(Stock_Market, data, how= "left", left_index=True, right_index=True)

# Function to download stock data with rate limiting and retry mechanism
def download_stock_data(increment, ticker):
    while True:
        try:
            # Wait for the next available request slot
            time.sleep(2)
            # Download the data
            return yf.download(ticker)
        except:
            # If any exception occurs, wait for 5 minutes before retrying
            print(f"Error occurred for {ticker} at increment {increment}. Waiting for 5 minutes before retrying...")
            time.sleep(300)

# Starts in 1962
initial_data = download_stock_data(0,"AMZN")
Stock_Market = pd.DataFrame(index=initial_data.index, columns=["test"])

for i, ticker in tqdm(enumerate(ticker_list)):
    data = download_stock_data(i, ticker)
    if data is not None:
        data[f'{ticker}'] = data.apply(lambda row: (row['Open'], row['High'], row['Low'], row['Close'], row["Volume"]), axis=1)
        data.drop(columns=["Open", "High", "Low", "Close", "Volume", "Adj Close"], inplace=True)
        Stock_Market = pd.merge(Stock_Market, data, how="left", left_index=True, right_index=True)
    if i%100 == 0:
        Stock_Market.to_csv("📦 Output/Full Stock Market 2000.csv")

    
Stock_Market.to_csv("📦 Output/Full Stock Market 2000.csv")
print(Stock_Market.columns, Stock_Market)