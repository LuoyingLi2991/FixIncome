# -*- coding: utf-8 -*-
"""
Created on Wed Nov 08 14:06:20 2017

@author: luoying.li
"""
import sys
sys.path.append('P:\Python Library')  

import datetime
import pandas as pd
import xlwings as xw
from BLPApi import BLPApi
from DataBaseConnection import DataBaseConnection
from ExcelGUIUtility import ExcelGUIUtility
import numpy as np


@xw.func
@xw.arg('Tickers', np.array, ndim=2)
def AutoRun(Tickers,path):
    """Function takes in Tickers and Directory of Database,
       Extract hourly intraday bar data from bloomberg and write data to database
    """
    T=[x[0] for x in Tickers]  # Convert Tickers to list
    eu=ExcelGUIUtility()  
    Tickers=eu.removeUni(T)  # Convert string to UTF-8 format
    api=BLPApi()  
    DBConn=DataBaseConnection(path,"TradeStorage.accdb")  # Connect to Database
    df_dict={}
    TblNames=DBConn.GetTableNames()  # Get all Table names in Database
    for each in Tickers:
        if ''.join(each.split()) in TblNames:  # If table exists, get last 5 days data
            S=datetime.datetime.now()-datetime.timedelta(days=5)
        else:  # Get Data starts from 30/09/2016
            S=datetime.datetime.combine(datetime.date(2016,9,30), datetime.time(00, 00))
        df=api.IntradayBarRequest(each,"TRADE",60,S,endDateTime=datetime.datetime.now()-datetime.timedelta(hours=8))
        df_dict[each]=df  # Fill Each dataframe into a dictionary
    DBConn.Write2DB(df_dict)  # Write Data to dataframe
    
    
    








if __name__ == '__main__':
    """
    ticker_list="CNH+1M Curncy"
    S=datetime.datetime.combine(datetime.date(2016,9,30), datetime.time(00, 00))
    E=datetime.datetime.combine(datetime.date(2017,11,7), datetime.time(21, 00))
    setvals=[]
    setvals.append(("security", ticker_list))
    setvals.append(("eventType", "TRADE"))
    setvals.append(("interval", 60))
    setvals.append(("startDateTime",S))
    setvals.append(("endDateTime",E))
    rtype="IntradayBarRequest"
    print IntradayBar(ticker_list,"TRADE",60,S)  
    """
    Tickers=[["CNH+1M Curncy"]]
    path="C:\Users\luoying.li\Project 2"
    AutoRun(Tickers,path)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    