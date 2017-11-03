# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 10:33:48 2017
@author: luoying.li

This script has supporting function for script "UpdateDB.py"

-Function bdh is to extract data from Bloomberg
-Function DF_Merge is to merge serveral single-column dataframe into one dataframe
-Function pd2DB write pandas dataframe to the database
-Function removeUni converts Unicode string read from Excel Interfacec to UTF-8 code
-Function Table2DF extracts data from Database and summarizes data into dataframe
-Functions Spreads,Flys,Rolldown,Carry,TotalReturn,SpreadsFlysVol,YieldsVol,
SpreadsFlysTR,AdjSpreadsTR,AdjFlysTR,AdjRD are supporting functions to compute intermediary data
"""
import blpapi
from collections import defaultdict
from datetime import datetime, date
from pandas._libs.tslib import Timestamp
import pandas as pd
import numpy as np
from YieldCurve import YieldCurve
from SpotCurve import SpotCurve
from dateutil.relativedelta import relativedelta
import pyodbc


def bdh(ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY'):
        """
        Get ticker_list and field_list
        return dictionary of pandas dataframe with ticker as keyname
        """
        host='localhost'
        port=8194 # Create and fill the request for the historical data
        sessionOptions = blpapi.SessionOptions() # Create SessionOption Object
        sessionOptions.setServerHost(host)  # Set Session Option host
        sessionOptions.setServerPort(port)  # Set Session Option port, default as 8194
        session = blpapi.Session(sessionOptions) # Create Session Object
        if not session.start():  
            print("Failed to start session.")
        
        if not session.openService("//blp/refdata"):  
            print("Failed to open //blp/refdata")
        
        refDataService = session.getService("//blp/refdata")  # get refdata service for historical data
        if isinstance(ticker_list,str): # change a single string to list
            ticker_list = [ticker_list]
        if isinstance(fld_list,str):  # change a single string to list
            fld_list = [fld_list]
        if hasattr(start_date, 'strftime'): # convert to datetime format
            start_date = start_date.strftime('%Y%m%d')
            print start_date
        if hasattr(end_date, 'strftime'): # convert to datetime format
            end_date = end_date.strftime('%Y%m%d')
            print end_date
        #print ticker_list,fld_list,start_date, end_date
        request = refDataService.createRequest("HistoricalDataRequest")  # Create request
        for t in ticker_list:
            request.getElement("securities").appendValue(t)  # Set Request Securites element
        for f in fld_list:
            request.getElement("fields").appendValue(f)  # Set Request Fields element
        request.set("periodicityAdjustment", "CALENDAR") # Set as the last business day of period required
        request.set("periodicitySelection", periodselection) # Set Period e.g WEEKLY
        request.set("startDate", start_date) # Set Request Start Date
        request.set("endDate", end_date) # Set Request End Date

        #print("Sending Request:", request)
        session.sendRequest(request)  # Send the request
        data = defaultdict(dict)  # defaultdict - later convert to pandas
        # Process received events
        while (True):
            ev = session.nextEvent() 
            if ev.eventType() in [5,6]: # filter events that contain historical data
                for msg in ev:
                   #print msg
                   ticker = msg.getElement('securityData').getElement('security').getValue()  # get security ticker from event
                   fieldData = msg.getElement("securityData").getElement("fieldData") 
                   # Fill default dictionary
                   for i in range(fieldData.numValues()):  
                        for j in range(1, fieldData.getValue(i).numElements()):
                            data[(ticker, fld_list[j - 1])][
                                    fieldData.getValue(i).getElement(0).getValue()] = fieldData.getValue(i).getElement(j).getValue()
            if ev.eventType() == blpapi.Event.RESPONSE: # Response completly received, so we could exit
                break
        
        pd_dict=dict() # Create empty dictionary 
        
        if len(data) == 0: # In case of security error case
            return pd.DataFrame()
        
        Default_Dict_Keys=data.keys() #  get keys of default dictionary
        check=[0]*len(ticker_list) #  Purpose: check whether the a ticker has a dataframe or not 
        
        for i in range(len(ticker_list)):
            for each in Default_Dict_Keys:
                # ticker doesn't have a dataframe yet, create dataframe with field and date as index 
                if each[0]==ticker_list[i] and check[i]==0:  
                    pd_dict[ticker_list[i]]=pd.DataFrame(zip(data[each].values(),data[each].keys()),columns=[each[1],'Date'])
                    pd_dict[ticker_list[i]].set_index('Date',inplace=True)
                    check[i]=1
                # ticker has a dataframe, add new field to existing dataframe
                if each[0]==ticker_list[i] and check[i]==1:
                   pd_dict[ticker_list[i]].loc[:,each[1]]=pd.Series(data[each].values(),index=pd_dict[ticker_list[i]].index)
            # sort dataframe based on column names and index
            pd_dict[ticker_list[i]].sort_index(inplace=True)
            pd_dict[ticker_list[i]].sort_index(axis=1, inplace=True)
        return pd_dict
    


def pd2DB (data,crsr):
    """Write DataFrame to  Database from dataframe dictionary
    Argument:
    data --dataframe dictionary
    crsr --cursor from database connection
    """
    # extract all table names from database
    tbls = crsr.tables(tableType='TABLE').fetchall()
    tbls_names=[]
    for tbl in tbls:
        tbls_names.append(str(tbl.table_name))
    #write each df to database
    for key, df in data.items():
        #print key
        key = ''.join(key.split()) # del all spaces in key
        # if table not created, create a new table
        if key not in tbls_names: 
            header=list(df) # get column names of dataframe
            Index=df.index.name # get index name
            # create SQL query for create new table
            cols=" (["+Index+"] date"
            for each in header:
                cols=cols+", ["+each+"]"+" double"
            cols=cols+", PRIMARY KEY([Date]))"
            query_ct="CREATE TABLE "+ str(key)+cols
            crsr.execute(query_ct)
            # Insert each row in DataFrame to Database
            for index, row in df.iterrows():
                row=list(row)
                row.insert(0,index)
                var_string = ', '.join('?' * len(row))
                query_insert="INSERT INTO "+str(key)+" VALUES (%s);" % var_string
                crsr.execute(query_insert,row)
        # write new data to database if a table already exists
        else: 
            # Find last index in the database
            query_last="select top 1 [Date] from "+str(key)+" order by [Date] desc"
            crsr.execute(query_last)  
            Last_Index = datetime.date(crsr.fetchone()[0])
            # Find first Index in dataframe
            Fir=df.index.tolist()[0]
           
            if isinstance(Fir,date):
                df_first_index=Fir
            if isinstance(Fir,Timestamp):
                df_first_index=datetime.date(Fir)
            
            if df_first_index <Last_Index: 
                DBTbl=Tables2DF(crsr,key,LB='2m').values()[0].tail(5)
                DBIdx=DBTbl.index.tolist()
                DBIdx.sort()
                Tempdf=df.loc[DBIdx]
                nIdx=[]
                for i, dt in enumerate(DBIdx):
                    dic1=DBTbl.loc[dt].to_dict()
                    dic2=Tempdf.loc[dt].to_dict()
                    if cmp(dic1,dic2)!=0:
                        Last_Index=datetime.date(dt)
                        nIdx=nIdx+DBIdx[i :]
                        break
                
                if len(nIdx)==0:
                    nIdx.append(DBIdx[-1])
                    Last_Index=datetime.date(DBIdx[-1])
                for each in nIdx:
                    crsr.execute("delete from "+str(key)+" where [Date]=?",each)

                df=df.loc[Last_Index :]   # create a dataframe piece
                for index, row in df.iterrows():
                    # write to the database starting from the second row of df piece
                    row=list(row)
                    row.insert(0,index)
                    var_string = ', '.join('?' * len(row))
                    query_insert="INSERT INTO "+str(key)+" VALUES (%s);" % var_string
                    crsr.execute(query_insert,row)
            else: print "Extract Dates Range is not enough!"
            
   

def DF_Merge(value,heads,flds,start,end=date.today().strftime('%Y%m%d')):
    """Merge seperate DataFrames into one TimeSeries df
    Argument
    value -- list of tickers
    flds -- list of fields
    start --start date e.g "20070101"
    end --end date e.g "20070101"
    heads -- desired col sequence in list format, match sequence with value
    Output
    Dictionary of DataFrame
    """    
    #print key,value,heads,flds,start,end
    data=bdh(value,flds,start,end)
    count=0 
    headers=dict(zip(value,heads))
    for key, each in data.items():
        each.rename(columns={flds[0]:headers[key]},inplace=True)
        #print each.head()
        if count==0:
            result=each
            count=count+1
        else:
            result=pd.merge(result,each, left_index=True, right_index=True)
    result=result[heads] # re-arrange column sequence has heads
    #print result.head()
    return result


def removeUni(l):
    """convert unicode to string"""
    result=[]
    for each in l:
        each=each.replace(u'\xa0', ' ').encode('utf-8')
        result.append(each)
    return result


def Build_Access_Connect(conn_str):
    """Build Connnection with Access DataBase
    Argument:
    conn_str  ---a string contains Driver and file Path
    Output:
    cnxn  ---connection
    crsr  ---cursor
    """
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()
    return crsr,cnxn

def Tables2DF(crsr,*selected_table_name,**LookBackWindow):
    """Reformat Tables in DataBase to Pandas DataFrame and Stored in a dictionary with table_names as keys
    Argument:
    crsr                  ---cursor from access
    *selected_table_name  ---table names in string format e.g "Spot", return all tables if ommited
    **LookBackWindow      ---Select part of the data regarding with LookBackWindow, return full range if ommitted
                          Possible Values: '1y','2y','3y','4y','5y','10y'
    Output:
    Dictionary of DataFrames with table_names as keys
    """
    dict_Para={'1y':1,'2y':2,'3y':3,'4y':4,'5y':5,'10y':10} # used to convert LookBackWindow to number format
    db_schema = dict() # used to save table names and table column names of all tables in database
     
    if selected_table_name==():
        tbls = crsr.tables(tableType='TABLE').fetchall() 
        for tbl in tbls:
            if tbl.table_name not in db_schema.keys(): 
                db_schema[tbl.table_name] = list()
            for col in crsr.columns(table=tbl.table_name):
                db_schema[tbl.table_name].append(col[3])
    else:
        for tbl in selected_table_name:
            if tbl not in db_schema.keys(): 
                db_schema[tbl] = list()
            for col in crsr.columns(table=tbl):
                db_schema[tbl].append(col[3])
            
    if selected_table_name==() and LookBackWindow=={}: # Return all tables 
        df_dict=dict()
        for tbl, cols in db_schema.items():
            sql = "SELECT * from %s" % tbl  
            crsr.execute(sql)
            val_list = []
            while True: # Fetch lines from database
                row = crsr.fetchone()
                if row is None:
                    break
                val_list.append(list(row))
            temp_df = pd.DataFrame(val_list, columns=cols) #Convert to dataframe format
            temp_df.set_index(keys=cols[0], inplace=True) # First Column as Key
            df_dict[tbl]=temp_df.sort_index() # Save each dataframe in dictionary
        return df_dict
            
    elif selected_table_name==() and LookBackWindow!={}: # Return part of each table in database
        LB=LookBackWindow.values()[0] 
        df_dict={}
        tbls_names=db_schema.keys()  # extract all table names
        for each in tbls_names:  # select part of the database     
            idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]  # select part of the database     
            if LB=='2m':
                dd=idx_last-relativedelta(days=61)
            else:
                dd=idx_last-relativedelta(years=dict_Para[LB])  #Compute the begining date of periodgit
            crsr.execute("select * from "+str(each)+" where [Date]>=?", dd)  #Select all data later than begining date
            val_list = []  #Fetch all data
            while True:
                row = crsr.fetchone()
                if row is None:
                    break
                val_list.append(list(row))
            header=db_schema[each] # get column names of database
            temp_df = pd.DataFrame(val_list, columns=header)
            temp_df.set_index(keys=header[0], inplace=True) # First Column [Date] as Key
            df_dict[each]=temp_df.sort_index() # return dictionary of dataframes
        return df_dict
        
    elif selected_table_name!=() and LookBackWindow=={}:  # Return full range of selected tables
         df_dict=dict()
         for each in selected_table_name: # Extract tables one by one
             sql = "SELECT * from %s" % each  
             crsr.execute(sql)
             val_list = []
             while True:  # Fetch lines
                 row = crsr.fetchone()
                 if row is None:
                     break
                 val_list.append(list(row))
             temp_df = pd.DataFrame(val_list, columns=db_schema[each]) # Create a dataframe
             temp_df.set_index(keys=db_schema[each][0], inplace=True) # First Column as Key
             df_dict[each]=temp_df.sort_index()
         return df_dict
    else:  # Return part of the selected tables
         LB=LookBackWindow.values()[0]
         df_dict=dict()
         for each in selected_table_name:
             idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]  # select part of the database     
             if LB=='2m':
                 dd=idx_last-relativedelta(days=61)
             else:
                 dd=idx_last-relativedelta(years=dict_Para[LB])  #Compute the begining date of periodgit
             crsr.execute("select * from "+str(each)+" where [Date]>=?", dd)  #Select all data later than begining date
             val_list = []  #Fetch all data
             while True:
                 row = crsr.fetchone()
                 if row is None:
                     break
                 val_list.append(list(row))
             header=db_schema[each] # get column names of database
             temp_df = pd.DataFrame(val_list, columns=header)
             temp_df.set_index(keys=header[0], inplace=True) # First Column [Date] as Key
             df_dict[each]=temp_df.sort_index() # return dictionary of dataframes
         return df_dict


def Spreads(df_dict):
    """Compute Spreads for each row of each dataframe in df_dict and return a dictionary of dataframe of spreads"""
    Convert_dict={'2s5s':[2,5],'5s10s':[5,10],'2s10s':[2,10],'1s2s':[1,2],'2s3s':[2,3],'1s3s':[1,3],'3s5s':[3,5],'5s7s':[5,7]}
    spreads=['2s5s','5s10s','2s10s','1s2s','2s3s','1s3s','3s5s','5s7s']  # desired spreads
    
    tenors=[]
    for each in spreads:  # Convert and merge all spreads into one list
        for t in Convert_dict[each]:
            tenors.append(t)   
    
    rlt={}
    for tbl in df_dict.keys():  # Compute spreads for each table
        indx = df_dict[tbl].index.tolist()  # get index 
        s=[]
        for each in indx: # for each curve, compute spread between t1 and t2 
            kwarg = df_dict[tbl].loc[each].to_dict()
            yc = YieldCurve(**kwarg)
            ylds=yc.build_curve(tenors)
            s.append([x-y for x,y in zip(ylds[1::2],ylds[0::2])])
        key=tbl+"Spreads"  # Construct table names 
        df=pd.DataFrame(s, index=indx,columns=spreads)
        df.index.name='Date'  # Set index name
        rlt[key]=df
    return rlt

def Flys(df_dict):
    """Compute Flys for each row of each dataframe in df_dict and return a dictionary of dataframe of flys"""
    Convert_dict={'2s5s10s':[2,5,10],'5s7s10s':[5,7,10],'1s3s5s':[1,3,5],'3s5s7s':[3,5,7],'1s2s3s':[1,2,3]}
    flys=['2s5s10s','5s7s10s','1s3s5s','3s5s7s','1s2s3s']
    
    tenors=[]
    for each in flys:  # Merge all tenors of flys into one lists
        tenors=tenors+Convert_dict[each]
    
    rlt={}
    for tbl in df_dict.keys():
        indx = df_dict[tbl].index.tolist()  # get index
        s=[]
        for each in indx: # for each curve, compute flys between t1 and t2 
            kwarg = df_dict[tbl].loc[each].to_dict()
            yc = YieldCurve(**kwarg)
            ylds=yc.build_curve(tenors)
            s.append([-2*y+z+x for x,y,z in zip(ylds[0::3],ylds[1::3],ylds[2::3])])
        key=tbl+"Flys" # Construct table names 
        df=pd.DataFrame(s, index=indx,columns=flys)
        df.index.name='Date'
        rlt[key]=df
    return rlt        

def RollDown(df_dict):
    """Compute 3m rolldown for each row of each dataframe in df_dict and return a dictionary of dataframe of rolldown"""
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)
    
    for each in df_dict.keys():
        if each.endswith("Spot"):  # Find Spot Curve
            spottbl=each
            spot_vals=df_dict[each].values.tolist()
            spot_idx=df_dict[each].index.tolist()
    rlt={}
    for tbl in df_dict.keys():
        if tbl.endswith("Spot"):  # Compute rolldown for spot curve
            roll_down_list = []
            for vals in spot_vals:
                kwarg = dict(zip(tenors, vals))
                yieldcurve = YieldCurve(**kwarg)
                rd = yieldcurve.calc_roll_down(tenors[1:], prd)
                roll_down_list.append(rd)
            df = pd.DataFrame(roll_down_list, index=spot_idx,columns=tenors[1:]) 
            df.index.name='Date'
            key=tbl+'RD'
            rlt[key]=df
            
        else:   # Compute rolldown for forward curve
            f=tbl[-2:]
            roll_down_list = []
            indx=df_dict[tbl].index.tolist()
            dels=[]
            for each in indx:
                if each in spot_idx:   # Starting from matched index
                    s=df_dict[spottbl].loc[each].to_dict()
                    kwarg=df_dict[tbl].loc[each].to_dict()
                    y=YieldCurve(**kwarg)
                    rd=y.calc_roll_down(tenors[1:],prd,s,f)
                    roll_down_list.append(rd)
                else: 
                    dels.append(each)
            for each in dels:
                indx.remove(each)
            df = pd.DataFrame(roll_down_list, index=indx,columns=tenors[1:]) 
            df.index.name='Date'
            key=tbl+'RD'
            rlt[key]=df
    return rlt


def Carry(df_dict):
    """Calculate 3m Carry for Spot Curve in df_dict"""
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)    
    
    rlt={}
    for each in df_dict.keys():
        if each.endswith("Spot"): # Find Spot Curve
            key=each
            spottbl=df_dict[each]
            spot_idx=df_dict[each].index.tolist()
        if each.endswith("3m"):  # Find forward 3m Curve
            fwdtbl=df_dict[each]
            fwd_idx=df_dict[each].index.tolist()
    
    dels=[]
    carry_list = []
    for each in spot_idx:   # Calculate 3m Carry with spot curve and forward 3m curve
        if each in fwd_idx:
            s = spottbl.loc[each].to_dict()
            f = fwdtbl.loc[each].to_dict()
            SC = SpotCurve(s,f)
            carry_list.append(SC.calc_carry(tenors[1:], prd))
        else:
            dels.append(each)
    for each in dels:
        spot_idx.remove(each)
    df = pd.DataFrame(carry_list, index=spot_idx,columns=tenors[1 :]) 
    df.index.name='Date'
    key=key+'Carry'
    rlt[key]=df
    return rlt
   
def TotalReturn(df_dict):
    """Calculate 3m Total Return for Spot Curve"""
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)    
    
    rlt={}
    for each in df_dict.keys():
        if each.endswith("Spot"):  # Find Spot Curve
            key=each
            spottbl=df_dict[each]
            spot_idx=df_dict[each].index.tolist()
        if each.endswith("3m"):  # Find 3m Forward Curve
            fwdtbl=df_dict[each]
            fwd_idx=df_dict[each].index.tolist()
    
    dels=[]
    tr_list = []
    for each in spot_idx:  # Calculate 3m Total Return 
        if each in fwd_idx:
            s = spottbl.loc[each].to_dict()
            f = fwdtbl.loc[each].to_dict()
            SC = SpotCurve(s,f)
            tr_list.append(SC.calc_total_return(tenors[1:], prd))
        else:
            dels.append(each)
    for each in dels:
        spot_idx.remove(each)
    df = pd.DataFrame(tr_list, index=spot_idx,columns=tenors[1 :]) 
    df.index.name='Date'
    key=key+'TR'
    rlt[key]=df
    return rlt

def SpreadsFlysVol(df_dict,crsr,frequency='d'):
    """Calculate Volatility of spreads and flys"""
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    tbls1=[x+'Spreads' for x in df_dict.keys()]  # Construct Spreads table name
    tbls2=[x+'Flys' for x in df_dict.keys()]  # Construct Flys table name

    if len(df_dict.values()[0])>60:  # Extract full range of Spreads/Flys
        spreads_dict=Tables2DF(crsr,*tbls1)
        flys_dict=Tables2DF(crsr,*tbls2)
    else:
        spreads_dict=Tables2DF(crsr,*tbls1,LB='1y')  # Extract last 1y's Spreads/Flys
        flys_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    
    rlt1={}
    rlt2={}
    for key1,df1 in spreads_dict.items(): # Compute Spreads Volatility
        df1.sort_index(inplace=True)
        df1=df1.diff()
        df1=df1.dropna()
        v1=df1.rolling(window=66).std()*np.sqrt(frequency_dict[frequency])
        v1=v1.dropna()
        key1=key1+'Vol'
        rlt1[key1]=v1
    
    for key2,df2 in flys_dict.items():  # Compute Flys Volatility
        df2.sort_index(inplace=True)
        df2=df2.diff()
        df2=df2.dropna()
        v2=df2.rolling(window=66).std()*np.sqrt(frequency_dict[frequency])
        v2=v2.dropna()
        key2=key2+'Vol'
        rlt2[key2]=v2
     
    return rlt1,rlt2
    
def YieldsVol(df_dict,crsr,frequency='d'): 
    """Compute Yield Volatility"""
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    tbls=df_dict.keys()
    
    if len(df_dict.values()[0])>60:   # Extract full range of Yields
        ylds_dict=Tables2DF(crsr,*tbls)
    else:
        ylds_dict=Tables2DF(crsr,*tbls,LB='1y')  # Extract Last 1y's Yields
    
    rlt1={}
    for key,df in ylds_dict.items():  # Compute Volatility
        df.sort_index(inplace=True)
        df=df.diff()
        df=df.dropna()
        v=df.rolling(window=66,min_periods=66).std()*np.sqrt(frequency_dict[frequency])
        v=v.dropna()
        key=key+'Vol'
        rlt1[key]=v
    
    return rlt1
    
    
    
def AdjRD(df_dict,crsr):
    """Compute Adjusted Rolldown"""
    tbls1=[x+'RD' for x in df_dict.keys()]  # Construct table name of Rolldown
    tbls2=[x+'Vol' for x in df_dict.keys()]  # Construct table name of Yield Volatility
   
    # Extract Data from Database
    if len(df_dict.values()[0])>60:  
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items():  # Compute Adjusted Rolldown
        key2=key[: -2]+'Vol'
        df2=vols_dict[key2]
        idx2=df2.index.tolist()
        idx1=df.index.tolist()
        idx=list(set(idx1).intersection(idx2))
        idx.sort()
        df=df.loc[idx]
        df2=df2.loc[idx]
        df2=df2[df.columns]
        ard=df.div(df2)
        key3=key[: -2]+'AdjRD'
        rlt[key3]=ard
    return rlt

def AdjCarryTR(df_dict,crsr):
    """Compute Adjusted Carry and Adjusted Total Return"""
    # Construct Table names
    for each in df_dict.keys():
        if each.endswith('Spot'):
            tbl1=each+'Carry'
            tbl2=each+'Vol'
            tbl3=each+'TR'
    
    # Extract Data
    if len(df_dict.values()[0])>60:
        dfs=Tables2DF(crsr,tbl1,tbl2,tbl3)
    else:
        dfs=Tables2DF(crsr,tbl1,tbl2,tbl3,LB='1y')
    
    rlt={}
    df1=dfs[tbl1]
    df2=dfs[tbl2]
    df3=dfs[tbl3]
    idx2=df2.index.tolist()
    idx1=df1.index.tolist()
    idx3=df3.index.tolist()
    idx_c=list(set(idx1).intersection(idx2))  
    idx_c.sort()
    idx_tr=list(set(idx3).intersection(idx2))
    idx_tr.sort()
    
    # Compute Adjusted Carry
    df1=df1.loc[idx_c]  
    df2_c=df2.loc[idx_c]
    df2_c=df2[df1.columns]
    ac=df1.div(df2_c)
    key3=tbl1[:-5]+'AdjCarry'
    rlt[key3]=ac
    
    # Compute Adjusted Total Return
    df3=df3.loc[idx_tr]
    df2_tr=df2.loc[idx_tr]
    df2_tr=df2[df3.columns]
    atr=df3.div(df2_tr)
    key4=tbl3[:-2]+'AdjTR'
    rlt[key4]=atr    
    
    return rlt
    
    
def SpreadsFlysTR(df_dict):
    """Compute Total Return of Spreads and Flys"""
    Convert_dict1={'2s5s':['2y','5y'],'5s10s':['5y','10y'],'2s10s':['2y','10y'],'1s2s':['1y','2y'],'2s3s':['2y','3y'],'1s3s':['1y','3y'],'3s5s':['3y','5y'],'5s7s':['5y','7y']}
    spreads=['2s5s','5s10s','2s10s','1s2s','2s3s','1s3s','3s5s','5s7s']
    
    Convert_dict2={'2s5s10s':['2y','5y','10y'],'5s7s10s':['5y','7y','10y'],'1s3s5s':['1y','3y','5y'],'3s5s7s':['3y','5y','7y'],'1s2s3s':['1y','2y','3y']}
    flys=['2s5s10s','5s7s10s','1s3s5s','3s5s7s','1s2s3s']
    
    
    tenors1=[]
    for each in spreads: # Merge tenors in Spreads into one list
        tenors1=tenors1+Convert_dict1[each]
    prd1 = ['3m'] * len(tenors1)
    
    tenors2=[]
    for each in flys: # Merge tenors in Flys into one list
        tenors2=tenors2+Convert_dict2[each]
    prd2 = ['3m'] * len(tenors2)
    
    
    for each in df_dict.keys():
        if each.endswith("Spot"):  # Find Spot curves' table name and index
            spottbl=df_dict[each]
            spot_idx=df_dict[each].index.tolist()
        if each.endswith("3m"):  # Find 3m Forward curves' table name and index
            f3tbl=df_dict[each]
            f3_idx=df_dict[each].index.tolist()
    
    rlt1={}
    rlt2={}
    for tbl in df_dict.keys():
        indx=df_dict[tbl].index.tolist()
        r1=[]
        r2=[]
        dels=[]
        for each in indx:
            if tbl.endswith("Spot"):  # For spot curves, compute spread total return
                if each in f3_idx:
                    s_dict = df_dict[tbl].loc[each].to_dict()
                    f_dict = f3tbl.loc[each].to_dict()
                    SC = SpotCurve(s_dict,f_dict)
                    tr1=SC.calc_total_return(tenors1,prd1)
                    tr2=SC.calc_total_return(tenors2,prd2)
                    r1.append([x-y for x,y in zip(tr1[1::2],tr1[0::2])])
                    r2.append([-2*y+z+x for x,y,z in zip(tr2[0::3],tr2[1::3],tr2[2::3])])
                else: dels.append(each)
            else:
                if each in spot_idx:  # For forward curves, compute spread roll down
                    s_dict = spottbl.loc[each].to_dict()
                    f_dict = df_dict[tbl].loc[each].to_dict()
                    yy = YieldCurve(**f_dict)
                    tr1=yy.calc_roll_down(tenors1,prd1,s_dict,tbl[-2:])
                    tr2=yy.calc_roll_down(tenors2,prd2,s_dict,tbl[-2:])
                    r1.append([x-y for x,y in zip(tr1[1::2],tr1[0::2])])
                    r2.append([-2*y+z+x for x,y,z in zip(tr2[0::3],tr2[1::3],tr2[2::3])])
                else: dels.append(each)
        for each in dels:
            indx.remove(each)
        rd1=pd.DataFrame(r1, index=indx,columns=spreads) # Construct a spread TR dataframe  
        rd1.index.name='Date'
        key1=tbl+'SpreadsRD'
        rlt1[key1]=rd1
        rd2=pd.DataFrame(r2, index=indx,columns=flys) # Construct a spread TR dataframe  
        rd2.index.name='Date'
        key2=tbl+'FlysRD'
        rlt2[key2]=rd2
    return rlt1,rlt2
    
def AdjSpreadsTR(df_dict,crsr):
    """Compute Adjusted Total Return of Spreads"""
    tbls1=[x+'SpreadsRD' for x in df_dict.keys()]  # construct Spread Rolldown table 
    tbls2=[x+'SpreadsVol' for x in df_dict.keys()]  # construct Spreads Volatility table
    # Extract Data from Database
    if len(df_dict.values()[0])>60:
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items(): # Compute Volatility adjusted Spreads Rolldown
        key2=key[:-2]+'Vol'
        df2=vols_dict[key2]
        idx2=df2.index.tolist()
        idx1=df.index.tolist()
        idx=list(set(idx1).intersection(idx2))
        idx.sort()
        df=df.loc[idx]
        df2=df2.loc[idx]
        ard=df.div(df2)
        key3=key[:-2]+'AdjRD'
        rlt[key3]=ard
    return rlt
    
    
def AdjFlysTR(df_dict,crsr):
    """Compute Adjusted Total Return of Flys"""
    tbls1=[x+'FlysRD' for x in df_dict.keys()]  # construct Flys Rolldown table
    tbls2=[x+'FlysVol' for x in df_dict.keys()] # construct Flys Volatility table
    # Extract Data from Database
    if len(df_dict.values()[0])>60:
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items(): # Compute Volatility adjusted Flys Rolldown
        key2=key[:-2]+'Vol'
        df2=vols_dict[key2]
        idx2=df2.index.tolist()
        idx1=df.index.tolist()
        idx=list(set(idx1).intersection(idx2))
        idx.sort()
        df=df.loc[idx]
        df2=df2.loc[idx]
        ard=df.div(df2)
        key3=key[:-2]+'AdjRD'
        rlt[key3]=ard
    return rlt


