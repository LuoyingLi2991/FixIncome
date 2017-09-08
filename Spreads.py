# -*- coding: utf-8 -*-
"""
Created on Thu Sep 07 12:12:29 2017

@author: luoying.li
"""
import testAccess
from YieldCurve import YieldCurve
from UtilityClass import UtilityClass
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta


def SelectFTable2DF(crsr, LookBackWindow):
    """select data within a LookBackWindow and convert to data frame
    Argument
    crsr --database crsr
    LookBackWindow  --e.g '1y' means select 1 year data starting from the latest
                      date from database
                    --Possible values: '1y','2y','3y','4y','5y','10y'       
    """
    dict_Para={'1y':1,'2y':2,'3y':3,'4y':4,'5y':5,'10y':10} 
    df_dict={}
    tbls = crsr.tables(tableType='TABLE').fetchall() # select all tables from database
    # extract all table names
    tbls_names=[]
    for tbl in tbls:
        tbls_names.append(str(tbl.table_name)) 
    # select part of the database     
    for each in tbls_names:
        #select last index in current database
        idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]
        #Compute the begining date of period
        dd=idx_last-relativedelta(years=dict_Para[LookBackWindow]) 
        #Select all data later than begining date
        crsr.execute("select * from "+str(each)+" where [Date]>=?", dd)
        #Fetch all data
        val_list = []
        while True:
            row = crsr.fetchone()
            if row is None:
                break
            val_list.append(list(row))
        # get column names of database
        header=[]
        for col in crsr.columns(table=tbl.table_name):
            header.append(col[3])
        # Create a dataframe    
        temp_df = pd.DataFrame(val_list, columns=header)
        temp_df.set_index(keys=header[0], inplace=True) # First Column [Date] as Key
        df_dict[each]=temp_df # return dictionary of dataframes
    return df_dict 

def spread (t1, t2, *args,**kwargs):
    """ Compute a time series of spread between two tenors and return z_score and percentile
    Argument:
        t1        --tenor 1 e.g '1y' means 1 year.
        t2        --tenor 2 e.g '1y' means 1 year, t2 should be longer than t1.
        *args     --e.g '1d', '1w', '1m', For purpose of return zscore of the data 1 day, 1 week
                  or 1 month before the last date. This parameter is omittable
        **kwargs  --Key word argument e.g LookBackWindow='1y'. 
                  Possible values: '1y','2y','3y','4y','5y','10y'
                  This parameter is omittable
    Output: dictionary of results with key as table names and values as a list of zscore and 
            percentile.
    """
    tenorDict = {'3m': 0.25, '6m': 0.5, '9m': 0.75, '1y': 1, '2y': 2, '3y': 3, '4y': 4, '5y': 5, '6y': 6, '7y': 7,
                 '8y': 8, '9y': 9, '10y': 10, '20y': 20, '30y': 30}
    t1=tenorDict[t1]
    t2=tenorDict[t2]
    u=UtilityClass()
    # Build Connnect with database
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=testAccess.Build_Access_Connect(conn_str)
    LookBackWindow=kwargs.values()[0] # get lookbackwindow
    if LookBackWindow=={}: # if no parameter was key in, take all dataset
        df_dict=testAccess.Tables2DF(crsr)
    else: # else select data from dataframe according to lookbackwindow
        df_dict=SelectFTable2DF(crsr,LookBackWindow) 
    result={}
    for key, df in df_dict.items():  # for each table in database, compute spread z score and percentile 
        header = list(df) # get header
        index = df.index  # get index
        vals_list = df.values.tolist() 
        spreads=[]
        for vals in vals_list: # for each curve, compute spread between t1 and t2 
            kwarg = dict(zip(header, vals))
            yieldcurve = YieldCurve(**kwarg)
            temp=yieldcurve.build_curve([t1,t2])
            spreads.append(temp[1]-temp[0])
        spread_pd=pd.DataFrame(spreads, index=index) # dataframe of spreads
        result[key]=tuple([u.calc_z_score(spread_pd,False,*args),u.calc_percentile(spread_pd,*args)])
    cnxn.close() # close database connection
    return result


def butterfly(t1,t2,t3,*args, **kwargs):
    """ Compute a time series of spread between two tenors and return z_score and percentile
    Argument:
        t1        --tenor 1 e.g '1y' means 1 year.
        t2        --tenor 2 e.g '1y' means 1 year, t2 should be longer than t1.
        t3        --tenor 3 e.g '1y' means 1 year, t3 should be longer than t2.
        *args     --e.g '1d', '1w', '1m', For purpose of return zscore of the data 1 day, 1 week
                  or 1 month before the last date. This parameter is omittable
        **kwargs  --Key word argument e.g LookBackWindow='1y'. 
                  Possible values: '1y','2y','3y','4y','5y','10y'
                  This parameter is omittable
    Output: dictionary of results with key as table names and values as a list of zscore and 
            percentile of last butterfly.
    """
    tenorDict = {'3m': 0.25, '6m': 0.5, '9m': 0.75, '1y': 1, '2y': 2, '3y': 3, '4y': 4, '5y': 5, '6y': 6, '7y': 7,
                 '8y': 8, '9y': 9, '10y': 10, '20y': 20, '30y': 30}
    t1=tenorDict[t1]
    t2=tenorDict[t2]
    t3=tenorDict[t3]
    u=UtilityClass()
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=testAccess.Build_Access_Connect(conn_str)
    LookBackWindow=kwargs.values()[0]
    if LookBackWindow=={}:
        df_dict=testAccess.Tables2DF(crsr)
    else: 
        df_dict=SelectFTable2DF(crsr,LookBackWindow) 
    result={}
    for key, df in df_dict.items():
        header = list(df)
        index = df.index
        vals_list = df.values.tolist()
        spreads=[]
        for vals in vals_list:
            kwarg = dict(zip(header, vals))
            yieldcurve = YieldCurve(**kwarg)
            temp=yieldcurve.build_curve([t1,t2,t3])
            spreads.append(2*temp[1]-temp[0]-temp[2]) # Compute butterfly from each curve
        spread_pd=pd.DataFrame(spreads, index=index) # create database
        #print spread_pd
        result[key]=tuple([u.calc_z_score(spread_pd,False,*args),u.calc_percentile(spread_pd,*args)])
    cnxn.close()
    return result
'''
def test_FRA(t1,t2):
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=testAccess.Build_Access_Connect(conn_str)
    df_dict={}
    tbls = crsr.tables(tableType='TABLE').fetchall() # select all tables from database
    # extract all table names
    tbls_names=[]
    for tbl in tbls:
        tbls_names.append(str(tbl.table_name)) 
    # select part of the database     
    for each in tbls_names:
        #select last index in current database
        idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]
        #Compute the begining date of period
        dd=idx_last-datetime.timedelta(days=30) 
        #Select all data later than begining date
        crsr.execute("select * from "+str(each)+" where [Date]>=?", dd)
        #Fetch all data
        val_list = []
        while True:
            row = crsr.fetchone()
            if row is None:
                break
            val_list.append(list(row))
        # get column names of database
        header=[]
        for col in crsr.columns(table=tbl.table_name):
            header.append(col[3])
        # Create a dataframe    
        temp_df = pd.DataFrame(val_list, columns=header)
        temp_df.set_index(keys=header[0], inplace=True) # First Column [Date] as Key
        df_dict[each]=temp_df # return dictionary of dataframes
    print df_dict 
    for key, df in df_dict.items():
        header = list(df)
        index = df.index
        vals_list = df.values.tolist()
        FRA=[]
        for vals in vals_list:
            kwarg = dict(zip(header, vals))
            yieldcurve = YieldCurve(**kwarg)
            FRA.append(yieldcurve.calc_FRA(t1,t2,360)) # Compute butterfly from each curve
        FRA_pd=pd.DataFrame(FRA, index=index) # create database
        print FRA_pd
        #result[key]=tuple([u.calc_z_score(spread_pd,False,*args),u.calc_percentile(spread_pd,*args)])
    cnxn.close()
'''   
   
if __name__ == "__main__":
  print  spread('2y','5y',LookBackWindow='1y')
  print butterfly('2y','5y','10y','1d','1w',LookBackWindow='1y')
  #test_FRA('1y','2y') 