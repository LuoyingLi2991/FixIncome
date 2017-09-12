# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 16:47:53 2017

@author: luoying.li
"""
import xlwings as xw
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import pyodbc
from dateutil.relativedelta import relativedelta
from YieldCurve import YieldCurve
from UtilityClass import UtilityClass
from SpotCurve import SpotCurve


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
    tbls = crsr.tables(tableType='TABLE').fetchall()  
    for tbl in tbls:
        if tbl.table_name not in db_schema.keys(): 
            db_schema[tbl.table_name] = list()
        for col in crsr.columns(table=tbl.table_name):
            db_schema[tbl.table_name].append(col[3])
                
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
            df_dict[tbl]=temp_df # Save each dataframe in dictionary
        return df_dict
            
    elif selected_table_name==() and LookBackWindow!={}: # Return part of each table in database
        LB=LookBackWindow.values()[0] 
        df_dict={}
        tbls_names=db_schema.keys()  # extract all table names
        for each in tbls_names:  # select part of the database     
            idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]  # select part of the database     
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
            df_dict[each]=temp_df # return dictionary of dataframes
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
             df_dict[each]=temp_df
         return df_dict
    else:  # Return part of the selected tables
         LB=LookBackWindow.values()[0]
         df_dict=dict()
         for each in selected_table_name:
             idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]  # select part of the database     
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
             df_dict[each]=temp_df # return dictionary of dataframes
         return df_dict
                      
@xw.func
def PlotSpot(db_str, fDir):
    """xlwings function: Plot 'Today', '1 Week Before', 1 Month Before' Spot Yield Curves
    Argument:
        db_str:database file directory in string format
        fDir: output file directory
    """
    db_str= 'DBQ='+str(db_str)   
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn_str) #Build Connection with database
    sht=xw.Book(fDir).sheets[0]  # Get sheet address
    idx_last=crsr.execute("select top 1 [Date] from Spot"+" order by [Date] desc").fetchone()[0] # Get Last Date
    Last_W=idx_last-datetime.timedelta(weeks=1) # Get Date one week before
    Last_M=idx_last-datetime.timedelta(days=30) # Get Date one Month before
    Data_now=list(crsr.execute("select * from  Spot where [Date]=?", idx_last).fetchone()) # get Last line data
    while crsr.execute("select * from  Spot where [Date]=?", Last_W).fetchone()==None: 
        Last_W=Last_W-datetime.timedelta(days=1) 
    Data_LastW=list(crsr.execute("select * from  Spot where [Date]=?", Last_W).fetchone())  # get data one week before
    while crsr.execute("select * from  Spot where [Date]=?", Last_M).fetchone()==None:
        Last_W=Last_W-datetime.timedelta(days=1)
    Data_LastM=list(crsr.execute("select * from  Spot where [Date]=?", Last_M).fetchone()) # get data one week before
    header=[]
    for col in crsr.columns(table='Spot'): # get column names 
        header.append(col[3])
    values=[Data_now,Data_LastW,Data_LastM]
    df = pd.DataFrame(values, columns=header) # Create a dataframe
    df.set_index(keys=header[0], inplace=True) # set date as key
    df=df.T # Transpose the dataframe
    ax = df.plot(legend=False,title="Spot Curve") # plot the dataframe 
    ax.legend(["Today","1W Before","1M Before"])
    fig = ax.get_figure() # get figure from plot
    plot=sht.pictures.add(fig, left=sht.range("B5:L23").left,top=sht.range("B5:L23").top) # put figure to the sheet
    plot.height=200  # set figure size
    plot.width = 300  # set figure size


@xw.func
#@xw.ret(expand='table')
def SpreadsTable(db_str, LookBackWindow):
    """Return Today, 1week before and 1month before's Spreads Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['Today', 'Today', 'Today','1W Before', '1W Before', '1W Before', '1M Before','1M Before' ,'1M Before']),
              np.array(['Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile'])]
    Index=np.array(['2s5s','5s10s','2s10s','1s2s','2s3s','1s3s','3s5s','5s7s']) # designate spreads
    Convert_dict={'2s5s':[2,5],'5s10s':[5,10],'2s10s':[2,10],'1s2s':[1,2],'2s3s':[2,3],'1s3s':[1,3],'3s5s':[3,5],'5s7s':[5,7]}
    if str(LookBackWindow)!="ALL": # Select part of table
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0] 
    Values=[] 
    u=UtilityClass() 
    for each in Index:
        tenors=Convert_dict[each]
        header = list(df) # get header
        index = df.index  # get index
        vals_list = df.values.tolist() 
        spreads=[]
        for vals in vals_list: # for each curve, compute spread between t1 and t2 
            kwarg = dict(zip(header, vals))
            yieldcurve = YieldCurve(**kwarg)
            yields=yieldcurve.build_curve([tenors[0],tenors[1]])
            spreads.append(yields[1]-yields[0])
        spread_pd=pd.DataFrame(spreads, index=index) # create dataframe for spreads
        [lvl,zscore]=u.calc_z_score(spread_pd,False,'1w','1m')
        ptl=u.calc_percentile(spread_pd,'1w','1m')
        row=[]
        for i in range (len(lvl)): # Write in one line
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[0])
        Values.append(row)
    #print Values
    tt=np.asarray(Values)
    s = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return s

@xw.func    
def ButterFlysTable(db_str, LookBackWindow):
    """Return Today, 1week before and 1month before's Flys Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['today', 'today', 'today','1W Before', '1W Before', '1W Before', '1M Before','1M Before' ,'1M Before']),
              np.array(['Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile'])]
    Index=np.array(['2s5s10s','5s7s10s','1s3s5s','3s5s7s','1s2s3s']) # Designate tenors
    Convert_dict={'2s5s10s':[2,5,10],'5s7s10s':[5,7,10],'1s3s5s':[1,3,5],'3s5s7s':[3,5,7],'1s2s3s':[1,2,3]}
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0]
    Values=[]
    u=UtilityClass()
    for each in Index:
        tenors=Convert_dict[each]
        header = list(df) # get header
        index = df.index  # get index
        vals_list = df.values.tolist() 
        flys=[]
        for vals in vals_list: # for each curve, compute fly among t1 t2 and t3 
            kwarg = dict(zip(header, vals))
            yieldcurve = YieldCurve(**kwarg)
            yields=yieldcurve.build_curve([tenors[0],tenors[1],tenors[2]])
            flys.append(2*yields[1]-yields[0]-yields[2]) 
        flys_pd=pd.DataFrame(flys, index=index)
        [lvl,zscore]=u.calc_z_score(flys_pd,False,'1w','1m')
        ptl=u.calc_percentile(flys_pd,'1w','1m')
        row=[]
        for i in range (len(lvl)): #write all results in one line
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[0])
        Values.append(row)
    #print Values
    tt=np.asarray(Values)
    s = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return s

@xw.func
def RollDownTable(db_str, LookBackWindow):
    """Return Today, 1week before and 1month before's RollDown, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['today', 'today', 'today','1W Before', '1W Before', '1W Before', '1M Before','1M Before' ,'1M Before']),
              np.array(['Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile'])]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0]
    cols = list(df)
    indx = df.index
    Index=np.asarray(cols)
    prd = ['3m'] * (len(cols) - 1)
    roll_down_list = []
    vals_list = df.values.tolist()
    for vals in vals_list:
        kwarg = dict(zip(cols, vals))
        yieldcurve = YieldCurve(**kwarg)
        temp = yieldcurve.calc_roll_down(cols[1:], prd)
        temp.insert(0, vals[0])
        roll_down_list.append(temp)
    df_roll_down = pd.DataFrame(roll_down_list, index=indx,columns=cols) 
    #print df_roll_down
    Values=[]
    for each in cols:  
        s=df_roll_down[each]
        temp=pd.DataFrame(s,index=indx) # For each column, create a df and pass to calc z score and percentile
        [lvl,zscore]=u.calc_z_score(temp,False,'1w','1m')
        ptl=u.calc_percentile(temp,'1w','1m')
        row=[]
        for i in range (len(lvl)): # write all results in oneline
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[0])
        Values.append(row)
    tt=np.asarray(Values) 
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt

@xw.func
def CarryTable(db_str, LookBackWindow):
    """Return Today, 1week before and 1month before's Carry Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['today', 'today', 'today','1W Before', '1W Before', '1W Before', '1M Before','1M Before' ,'1M Before']),
              np.array(['Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile'])]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot','Fwd3m',LB=str(LookBackWindow)).values()
    else: df=Tables2DF(crsr,'Spot','Fwd3m').values()
    spot=df[0]
    fwd=df[1]
    cols = list(spot)
    indx = spot.index
    Index=np.asarray(cols)
    prd = ['3m'] * (len(cols) - 1)
    carry_list = []
    vals_s = spot.values.tolist()
    vals_f = fwd.values.tolist()
    for s, f in zip(vals_s, vals_f):
        s_dict = dict(zip(cols, s))
        f_dict = dict(zip(cols, f))
        SC = SpotCurve(s_dict,f_dict)
        temp = SC.calc_carry(cols[1:], prd)
        temp.insert(0, -s[0])
        carry_list.append(temp)
    df_carry = pd.DataFrame(carry_list, index=indx,
                                columns=cols) 
    Values=[]
    for each in cols:
        s=df_carry[each]
        temp=pd.DataFrame(s,index=indx)
        [lvl,zscore]=u.calc_z_score(temp,False,'1w','1m')
        ptl=u.calc_percentile(temp,'1w','1m')
        row=[]
        for i in range (len(lvl)):
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[0])
        Values.append(row)
    tt=np.asarray(Values)
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt

@xw.func
def TRTable(db_str, LookBackWindow):
    """Return Today, 1week before and 1month before's Total Return Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['today', 'today', 'today','1W Before', '1W Before', '1W Before', '1M Before','1M Before' ,'1M Before']),
              np.array(['Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile', 'Level', 'Z score', 'Percentile'])]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot','Fwd3m',LB=str(LookBackWindow)).values()
    else: df=Tables2DF(crsr,'Spot','Fwd3m').values()
    spot=df[0]
    fwd=df[1]
    cols = list(spot)
    indx = spot.index
    Index=np.asarray(cols)
    prd = ['3m'] * (len(cols) - 1)
    TR_list = []
    vals_s = spot.values.tolist()
    vals_f = fwd.values.tolist()
    for s, f in zip(vals_s, vals_f):
        s_dict = dict(zip(cols, s))
        f_dict = dict(zip(cols, f))
        SC = SpotCurve(s_dict,f_dict)
        temp = SC.calc_total_return(cols[1:], prd)
        temp.insert(0, 0.0001)
        TR_list.append(temp)
    df_TR = pd.DataFrame(TR_list, index=indx,
                                columns=cols) 
    #print df_TR
    Values=[]
    for each in cols:
        s=df_TR[each]
        temp=pd.DataFrame(s,index=indx)
        #print temp
        [lvl,zscore]=u.calc_z_score(temp,False,'1w','1m')
        ptl=u.calc_percentile(temp,'1w','1m')
        row=[]
        for i in range (len(lvl)):
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[0])
        Values.append(row)
    tt=np.asarray(Values)
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt


if __name__ == "__main__":
    #conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                #'DBQ=C:\\Test.accdb;')
    dbstr='C:\\Test.accdb;'
    LB='1y'
    #[crsr,cnxn]=Build_Access_Connect(conn_str)
    #PlotSpot(crsr)
    sss= TRTable(dbstr,LB)
    
    #Tables2DF(crsr,LB='2y')
    
