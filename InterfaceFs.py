
import xlwings as xw
import pandas as pd
import numpy as np
import pyodbc
from dateutil.relativedelta import relativedelta
from YieldCurve import YieldCurve
from UtilityClass import UtilityClass
from SpotCurve import SpotCurve
import BLP2DF


@xw.func
@xw.arg('Tickers', pd.DataFrame, index=True, header=True)
@xw.arg('start', np.array, ndim=2)
def updateDB(Tickers,start):
    headers=Tickers.index
    table_names=BLP2DF.removeUni(list(Tickers))
    tickers_list=Tickers.T.values.tolist()
    for i,each in enumerate(tickers_list):
        tickers_list[i]=BLP2DF.removeUni(each)
    flds=["PX_LAST"]
    start=str(int(start[0][0]))
    data={}
    for name, tickers in zip(table_names,tickers_list):
        data[name]=BLP2DF.DF_Merge(tickers,headers,flds,start)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    BLP2DF.pd2DB(data, crsr)
    cnxn.commit() 
    cnxn.close()

@xw.func
@xw.ret(expand='table')
def Plottesting():
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    df=Tables2DF(crsr,'Spot',LB='10y').values()[0]
    #r=df.resample('W-FRI').last()
    #d=df.index[-1]
    #r.rename({r.index[-1]: d}, inplace=True)# Last Date
    r=df
    r.drop(df.columns[range(1,7)], axis=1, inplace=True)
    r['Aver']=[np.mean(r['3m'].tolist())]*len(r['3m'].tolist())
    sd=np.std(r['3m'].tolist())
    r['+1sd']=[r['Aver'].values[0]+sd]*len(r['3m'].tolist())
    r['-1sd']=[r['Aver'].values[0]-sd]*len(r['3m'].tolist())
    r['+2sd']=[r['Aver'].values[0]+2*sd]*len(r['3m'].tolist())
    r['-2sd']=[r['Aver'].values[0]-2*sd]*len(r['3m'].tolist())
    return r

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
def calc_historic_vol(db_str, LookBackWindow, frequency='d'):
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    if str(LookBackWindow)!="ALL": # Select part of table
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0] 
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    idx=df.index[1:]
    col_names=list(df)
    for i,col in enumerate(col_names):
        timeseries=df[col].values.tolist()
        dyields=[(timeseries[x+1]-timeseries[x])/timeseries[x] for x in range(len(timeseries)-1)]
        if i==0:
            rlt=pd.DataFrame(dyields,index=idx,columns=['dy'])
            rlt[col] = rlt['dy'].rolling(window=66).std()*np.sqrt(frequency_dict[frequency])  # annulized three months rolling window std
            rlt.drop('dy',1,inplace=True)
        else:
            rlt.loc[:,'dy']=pd.Series(dyields,index=idx)
            rlt[col] = rlt['dy'].rolling(window=66).std()*np.sqrt(frequency_dict[frequency])  # annulized three months rolling window std
            rlt.drop('dy',1,inplace=True)
            
    rlt = rlt.dropna()
    return rlt
    
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
    headers = [np.array(['LastD']*3+ ['1W Before']*3 +['1M Before']*3),
              np.array(['Level', 'Z', 'PCTL']*3 )]
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
            row.append(ptl[i])
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
    headers = [np.array(['LastD']*3+ ['1W Before']*3 +['1M Before']*3),
              np.array(['Level', 'Z', 'PCTL']*3 )]
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
            row.append(ptl[i])
        Values.append(row)
    #print Values
    tt=np.asarray(Values)
    s = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return s



@xw.func
@xw.arg('vols', pd.DataFrame, index=True, header=True)
def RollDownTable(db_str, LookBackWindow, vols):
    """Return Today, 1week before and 1month before's RollDown, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['LastD']*6+ ['1W Before']*6 +['1M Before']*6),
              np.array((['Level', 'Z', 'PCTL']+['adj_Level', 'adj_Z', 'adj_PCTL'])*3 )]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0]
    start_vol=vols.index[0]
    df=df.loc[start_vol :]
    cols = list(df)
    indx = df.index
    Index=np.asarray(cols[1:])
    prd = ['3m'] * len(cols)
    roll_down_list = []
    vals_list = df.values.tolist()
    for vals in vals_list:
        kwarg = dict(zip(cols, vals))
        yieldcurve = YieldCurve(**kwarg)
        temp = yieldcurve.calc_roll_down(cols[1:], prd)
        roll_down_list.append(temp)
    df_roll_down = pd.DataFrame(roll_down_list, index=indx,columns=cols[1:]) 
    #print df_roll_down
    Values=[]
    for each in cols[1:]:  
        rd=df_roll_down[each].tolist()
        v=vols[each].tolist()
        adj_rd=[x/y for x,y in zip(rd,v)]
        temp_rd=pd.DataFrame(rd,index=indx) # For each column, create a df and pass to calc z score and percentile
        temp_adj_rd=pd.DataFrame(adj_rd,index=indx)
        [rd_lvl,rd_zscore]=u.calc_z_score(temp_rd,False,'1w','1m')
        rd_ptl=u.calc_percentile(temp_rd,'1w','1m')
        [adj_rd_lvl,adj_rd_zscore]=u.calc_z_score(temp_adj_rd,False,'1w','1m')
        adj_rd_ptl=u.calc_percentile(temp_adj_rd,'1w','1m')
        
        row=[]
        for i in range (len(rd_lvl)): # write all results in oneline
            row.append(rd_lvl[i])
            row.append(rd_zscore[i])
            row.append(rd_ptl[i])
            row.append(adj_rd_lvl[i])
            row.append(adj_rd_zscore[i])
            row.append(adj_rd_ptl[i])
        Values.append(row)
    tt=np.asarray(Values) 
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt

@xw.func
@xw.arg('vols', pd.DataFrame, index=True, header=True)
def CarryTable(db_str, LookBackWindow, vols):
    """Return Today, 1week before and 1month before's Carry Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['LastD']*6+ ['1W Before']*6 +['1M Before']*6),
              np.array((['Level', 'Z', 'PCTL']+['adj_Level', 'adj_Z', 'adj_PCTL'])*3 )]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot','Fwd3m',LB=str(LookBackWindow)).values()
    else: df=Tables2DF(crsr,'Spot','Fwd3m').values()
    start_vol=vols.index[0]
    spot=df[0].loc[start_vol :]
    fwd=df[1].loc[start_vol :]
    cols = list(spot)
    indx = spot.index
    indx_fwd=fwd.index
    Index=np.asarray(cols[1 :])
    prd = ['3m'] * len(cols)
    carry_list = []
    for each in indx:
        if each in indx_fwd:
            s = spot.loc[each].tolist()
            f = fwd.loc[each].tolist()
            s_dict = dict(zip(cols, s))
            f_dict = dict(zip(cols, f))
            SC = SpotCurve(s_dict,f_dict)
            carry_list.append( SC.calc_carry(cols[1:], prd))
        else:
            indx.remove(each)
    df_carry = pd.DataFrame(carry_list, index=indx,columns=cols[1 :]) 
    Values=[]
    for each in cols[1 :]:
        c=df_carry[each].tolist()
        v=vols[each].tolist()
        adj_c=[x/y for x,y in zip(c,v)]
        temp_c=pd.DataFrame(c,index=indx)
        temp_adj_c=pd.DataFrame(adj_c,index=indx)
        [lvl,zscore]=u.calc_z_score(temp_c,False,'1w','1m')
        ptl=u.calc_percentile(temp_c,'1w','1m')
        [adj_lvl,adj_zscore]=u.calc_z_score(temp_adj_c,False,'1w','1m')
        adj_ptl=u.calc_percentile(temp_adj_c,'1w','1m')
        row=[]
        for i in range (len(lvl)):
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[i])
            row.append(adj_lvl[i])
            row.append(adj_zscore[i])
            row.append(adj_ptl[i])
        Values.append(row)
    tt=np.asarray(Values)
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt

@xw.func
@xw.arg('vols', pd.DataFrame, index=True, header=True)
def TRTable(db_str, LookBackWindow, vols):
    """Return Today, 1week before and 1month before's Total Return Level, Z_score, Percentile
    Arguments:
        db_str: database file directory
        LookBackWindow: Whether to select part of the table
    """
    db_str= 'DBQ='+str(db_str)
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    headers = [np.array(['LastD']*6+ ['1W Before']*6 +['1M Before']*6),
              np.array((['Level', 'Z', 'PCTL']+['adj_Level', 'adj_Z', 'adj_PCTL'])*3 )]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot','Fwd3m',LB=str(LookBackWindow)).values()
    else: df=Tables2DF(crsr,'Spot','Fwd3m').values()
    start_vol=vols.index[0]
    spot=df[0].loc[start_vol :]
    fwd=df[1].loc[start_vol :]
    cols = list(spot)
    indx = spot.index
    indx_fwd=fwd.index
    Index=np.asarray(cols[1 :])
    prd = ['3m'] * len(cols)
    TR_list = []
    for each in indx:
        if each in indx_fwd:
            s = spot.loc[each].tolist()
            f = fwd.loc[each].tolist()
            s_dict = dict(zip(cols, s))
            f_dict = dict(zip(cols, f))
            SC = SpotCurve(s_dict,f_dict)
            TR_list.append(SC.calc_total_return(cols[1 :], prd))
        else:
            indx.remove(each)
    df_TR = pd.DataFrame(TR_list, index=indx,columns=cols[1 :])                      
    Values=[]
    for each in cols[1 :]:
        tr=df_TR[each].tolist()
        v=vols[each].tolist()
        adj_tr=[x/y for x,y in zip(tr,v)]
        temp_tr=pd.DataFrame(tr,index=indx)
        temp_adj_tr=pd.DataFrame(adj_tr,index=indx)
        
        [lvl,zscore]=u.calc_z_score(temp_tr,False,'1w','1m')
        ptl=u.calc_percentile(temp_tr,'1w','1m')
        [adj_lvl,adj_zscore]=u.calc_z_score(temp_adj_tr,False,'1w','1m')
        adj_ptl=u.calc_percentile(temp_adj_tr,'1w','1m')
        row=[]
        for i in range (len(lvl)):
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[i])
            row.append(adj_lvl[i])
            row.append(adj_zscore[i])
            row.append(adj_ptl[i])
        Values.append(row)
    tt=np.asarray(Values)
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt

@xw.func
def SpotCurveLvLs(db_str,LookBackWindow):
    db_str= 'DBQ='+str(db_str)   
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + db_str)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn_str) #Build Connection with database
    headers = [np.array(['LastD']*3+ ['1W Before']*3 +['1M Before']*3),
              np.array(['Level', 'Z', 'PCTL']*3 )]
    u=UtilityClass()
    if str(LookBackWindow)!="ALL":
        df=Tables2DF(crsr,'Spot',LB=str(LookBackWindow)).values()[0]
    else: df=Tables2DF(crsr,'Spot').values()[0]
    cols = list(df)
    indx = df.index
    Index=np.asarray(cols)
    Values=[]
    for each in cols:
        s=df[each]
        temp=pd.DataFrame(s,index=indx)
        #print temp
        [lvl,zscore]=u.calc_z_score(temp,False,'1w','1m')
        ptl=u.calc_percentile(temp,'1w','1m')
        row=[]
        for i in range (len(lvl)):
            row.append(lvl[i])
            row.append(zscore[i])
            row.append(ptl[i])
        Values.append(row)
    tt=np.asarray(Values)
    rlt = pd.DataFrame(tt, index=Index, columns =headers )
    cnxn.close()
    return rlt


    
  
    

if __name__ == "__main__":
    #conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                #'DBQ=C:\\Test.accdb;')
    #dbstr='C:\\Test.accdb;'
    #LB='1y'
    #s= calc_historic_vol(dbstr,LB)
    #CarryTable(dbstr, LB, s)
    Plottesting()
    
    #print TRTable(dbstr,LB)

    ##Tables2DF(crsr,LB='2y')
    
