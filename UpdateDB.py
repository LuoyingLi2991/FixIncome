# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 10:16:33 2017

@author: luoying.li
"""

import xlwings as xw
import pandas as pd
import numpy as np
import datetime
import BLP2DB
import win32com
import os


@xw.func
@xw.arg('Tickers', pd.DataFrame, index=True, header=True)
@xw.arg('start', np.array, ndim=2)
def updateDB(Tickers,start,path):
    """Update YieldsData database which stores Spot/Forward Yields for different Countries
    Tickers: a dataframe of Tickers 
    start: start date
    path: database directory
    """
    headers=Tickers.index  # Get tenors
    table_names=BLP2DB.removeUni(list(Tickers))  # Get table names
    tickers_list=Tickers.T.values.tolist()  # Get tickers for each table
    for i,each in enumerate(tickers_list):
        tickers_list[i]=BLP2DB.removeUni(each)
    flds=["PX_LAST"]  # Set Field
    
    DB='DBQ='+str(path + '\\YieldsData.accdb')  
    conn = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + DB)  
    [crsr,cnxn]=BLP2DB.Build_Access_Connect(conn)
    
    DB_tbls = crsr.tables(tableType='TABLE').fetchall()
    DB_tbls_names=[]
    for tbl in DB_tbls:
        DB_tbls_names.append(tbl[2])
    if table_names[-1] in DB_tbls_names:
        start=(datetime.date.today()-datetime.timedelta(days=60)).strftime('%Y%m%d')  # Last 60 days
    else: start=str(int(start[0][0]))  # Set start date
    print start
    
    data={}
    for name, tickers in zip(table_names,tickers_list): # For each table, extract data from bloomberg
        data[name]=BLP2DB.DF_Merge(tickers,headers,flds,start)

    BLP2DB.pd2DB(data, crsr)  # Write to database
    cnxn.commit() 
    cnxn.close()
    
    
@xw.func
def Repair_Compact_DB(path):
    """Repair and Compact the DataBase"""
    oApp = win32com.client.Dispatch("Access.Application")
    srcDB=str(path+'\\YieldsData.accdb')
    destDB = str(path+'\\YieldsData_backup.accdb')
    oApp.CompactRepair(srcDB,destDB)
    os.remove(destDB)
    oApp = None
    
@xw.func
def UpdateElements(Country,path):
    """Update Temp Data database
    Country: Country name
    path: database Directory
    """
    print Country
    tempDB= 'DBQ='+str(path + '\\TempData.accdb')  # Connect to TempData Database
    conn1 = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + tempDB)  #Create database connection string
    [crsr1,cnxn1]=BLP2DB.Build_Access_Connect(conn1)
    tbls1 = crsr1.tables(tableType='TABLE').fetchall()
    
    # Check if Temp data tables exist for this Country
    cntyExist=False
    t=Country+"SpotSpreadsAdjRD"  
    for tbl in tbls1:
        if tbl[2]==t:
            cntyExist=True
    
    # Connect to YieldData database        
    yieldsDB= 'DBQ='+str(path+'\\YieldsData.accdb')   
    conn2 = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + yieldsDB)  #Create database connection string
    [crsr2,cnxn2]=BLP2DB.Build_Access_Connect(conn2)
    tbls2=crsr2.tables(tableType='TABLE').fetchall()
    
    # Extract all tables related to this Country in YieldsData database
    cnty_tbls=[]
    for tbl in tbls2:
        if tbl[2].startswith(Country):
            cnty_tbls.append(tbl[2])
    cnty_tbls=BLP2DB.removeUni(cnty_tbls)

    
    if not cntyExist:  # If no TempData table exists for this Country, Extract full range data from YieldsData databse
        df_dict=BLP2DB.Tables2DF(crsr2,*cnty_tbls)
    else:  # Else extract part of tables in YieldsData DataBase
        df_dict=BLP2DB.Tables2DF(crsr2,*cnty_tbls,LB='2m')
    
    # Compute Spreads for this Country and write to database
    spread_dict=BLP2DB.Spreads(df_dict)
    BLP2DB.pd2DB(spread_dict,crsr1)
    cnxn1.commit()
    
    # Compute Flys for this Country and write to database
    fly_dict=BLP2DB.Flys(df_dict)
    BLP2DB.pd2DB(fly_dict,crsr1)
    cnxn1.commit()
    
    # Compute rolldown for this Country and write to database
    RD_dict=BLP2DB.RollDown(df_dict)
    BLP2DB.pd2DB(RD_dict,crsr1)
    cnxn1.commit()
    
    # Compute Carry/TR for this Country and write to database
    C_dict=BLP2DB.Carry(df_dict)
    BLP2DB.pd2DB(C_dict,crsr1)
    cnxn1.commit()
    TR_dict=BLP2DB.TotalReturn(df_dict)
    BLP2DB.pd2DB(TR_dict,crsr1)
    cnxn1.commit()
    
    # Compute vols of Spreads/Flys for this Country and write to database
    [spreadvol,flyvol]=BLP2DB.SpreadsFlysVol(df_dict,crsr1)
    BLP2DB.pd2DB(spreadvol,crsr1)
    cnxn1.commit()
    BLP2DB.pd2DB(flyvol,crsr1)
    cnxn1.commit()
    
    # Compute vol of for this Country and write to database
    yldsvol=BLP2DB.YieldsVol(df_dict,crsr2)
    BLP2DB.pd2DB(yldsvol,crsr1)
    cnxn1.commit()
    
    # Compute Adjrolldown/AdjCarry/AdjTotalReturn for this Country and write to database
    adjRD=BLP2DB.AdjRD(df_dict,crsr1)
    BLP2DB.pd2DB(adjRD,crsr1)
    cnxn1.commit()
    rlt=BLP2DB.AdjCarryTR(df_dict,crsr1)
    BLP2DB.pd2DB(rlt,crsr1)
    cnxn1.commit()
    
    # Compute SpreadsTotalReturn/FlysTotalReturn for this Country and write to database
    [spreadRD,flysRD]=BLP2DB.SpreadsFlysTR(df_dict)
    BLP2DB.pd2DB(spreadRD,crsr1)
    cnxn1.commit()
    BLP2DB.pd2DB(flysRD,crsr1)
    cnxn1.commit()
    
    # Compute Adjusted SpreadsTotalReturn/FlysTotalReturn for this Country and write to database
    AdjSpreadsRD=BLP2DB.AdjSpreadsTR(df_dict,crsr1)
    AdjFlysRD=BLP2DB.AdjFlysTR(df_dict,crsr1)
    BLP2DB.pd2DB(AdjSpreadsRD,crsr1)
    cnxn1.commit()
    BLP2DB.pd2DB(AdjFlysRD,crsr1)    
    cnxn1.commit()

    cnxn1.close()
    cnxn2.close()

