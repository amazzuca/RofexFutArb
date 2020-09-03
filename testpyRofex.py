# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 00:27:55 2020

@author: amazz
"""

import pyRofex
import pandas as pd
from datetime import datetime
import config


prices = pd.DataFrame(columns=["Time",'symbol', "last"])
prices.set_index('Time', inplace=True)

# Set the the parameter for the REMARKET environment
pyRofex.initialize(user=config.username,
                   password=config.password,
                   account=config.account,
                   environment=pyRofex.Environment.REMARKET)


# First we define the handlers that will process the messages and exceptions.
def market_data_handler(message):
    global prices
    print("Market Data Message Received: {0}".format(message))
    print('datetime {}, symbol {},  last {}'.format(datetime.fromtimestamp(message["timestamp"]/1000), message['instrumentId']['symbol'], message["marketData"]["LA"]["price"] ))
    
    last = message["marketData"]["LA"]["price"]
    symbol =  message['instrument']['symbol']
    print('datetime {}, symbol {},  last {}'.format(datetime.fromtimestamp(message["timestamp"]/1000),symbol, last))
    prices.loc[datetime.fromtimestamp(message["timestamp"]/1000)] = [
        symbol, last]
    print(prices)
    
  

    
def order_report_handler(message):
    print("Order Report Message Received: {0}".format(message))
def error_handler(message):
    print("Error Message Received: {0}".format(message))
def exception_handler(e):
    print("Exception Occurred: {0}".format(e.message))



# Initiate Websocket Connection
pyRofex.init_websocket_connection(market_data_handler=market_data_handler,
                                  error_handler=error_handler,
                                  exception_handler=exception_handler)

# Instruments list to subscribe
instruments = ["RFX20Sep20", "I.RFX20"]
# Uses the MarketDataEntry enum to define the entries we want to subscribe to
entries = [pyRofex.MarketDataEntry.BIDS,
           pyRofex.MarketDataEntry.OFFERS,
           pyRofex.MarketDataEntry.LAST]

# Subscribes to receive market data messages **
pyRofex.market_data_subscription(tickers=instruments,
                                 entries=entries)

# Subscribes to receive order report messages (default account will be used) **
pyRofex.order_report_subscription()