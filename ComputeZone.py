# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 10:57:40 2017

@author: luoying.li
"""
import pandas as pd
import numpy as np
from YieldCurve import YieldCurve
from SpotCurve import SpotCurve
from dateutil.relativedelta import relativedelta


def Tables2DF(crsr,*selected_table_name,**LB):
    """Reformat Tables in DataBase to Pandas DataFrame and Stored in a dictionary with table_names as keys
    Argument:
    crsr                  ---cursor from access
    *selected_table_name  ---table names in string format e.g "Spot", return all tables if ommited
    Output:
    Dictionary of DataFrames with table_names as keys
    """
    db_schema = dict() # used to save table names and table column names of all tables in database
    tbls = crsr.tables(tableType='TABLE').fetchall()  
    for tbl in tbls:
        if tbl.table_name not in db_schema.keys(): 
            db_schema[tbl.table_name] = list()
        for col in crsr.columns(table=tbl.table_name):
            db_schema[tbl.table_name].append(col[3])          
    df_dict=dict()
    if len(LB)==0:
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
    else:
         df_dict=dict()
         for each in selected_table_name:
             idx_last=crsr.execute("select top 1 [Date] from "+str(each)+" order by [Date] desc").fetchone()[0]  # select part of the database     
             dd=idx_last-relativedelta(years=1)  #Compute the begining date of periodgit
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
        key=tbl+"Spreads"
        df=pd.DataFrame(s, index=indx,columns=spreads)
        df.index.name='Date'
        rlt[key]=df
    return rlt

def Flys(df_dict):
    Convert_dict={'2s5s10s':[2,5,10],'5s7s10s':[5,7,10],'1s3s5s':[1,3,5],'3s5s7s':[3,5,7],'1s2s3s':[1,2,3]}
    flys=['2s5s10s','5s7s10s','1s3s5s','3s5s7s','1s2s3s']
    
    tenors=[]
    for each in flys:
        tenors=tenors+Convert_dict[each]
    
    rlt={}
    for tbl in df_dict.keys():
        indx = df_dict[tbl].index.tolist()  # get index
        s=[]
        for each in indx: # for each curve, compute spread between t1 and t2 
            kwarg = df_dict[tbl].loc[each].to_dict()
            yc = YieldCurve(**kwarg)
            ylds=yc.build_curve(tenors)
            s.append([2*y-z-x for x,y,z in zip(ylds[0::3],ylds[1::3],ylds[2::3])])
        key=tbl+"Flys"
        df=pd.DataFrame(s, index=indx,columns=flys)
        df.index.name='Date'
        rlt[key]=df
    return rlt        

def RollDown(df_dict):
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)
    
    for each in df_dict.keys():
        if each.endswith("Spot"):
            spottbl=each
            spot_vals=df_dict[each].values.tolist()
            spot_idx=df_dict[each].index.tolist()
    rlt={}
    for tbl in df_dict.keys():
        if tbl.endswith("Spot"):
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
            
        else:
            f=tbl[-2:]
            roll_down_list = []
            indx=df_dict[tbl].index.tolist()
            dels=[]
            for each in indx:
                if each in spot_idx:
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
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)    
    
    rlt={}
    for each in df_dict.keys():
        if each.endswith("Spot"):
            key=each
            spottbl=df_dict[each]
            spot_idx=df_dict[each].index.tolist()
        if each.endswith("3m"):
            fwdtbl=df_dict[each]
            fwd_idx=df_dict[each].index.tolist()
    
    dels=[]
    carry_list = []
    for each in spot_idx:
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
    tenors=list(df_dict.values()[0])
    prd=['3m'] * (len(tenors)-1)    
    
    rlt={}
    for each in df_dict.keys():
        if each.endswith("Spot"):
            key=each
            spottbl=df_dict[each]
            spot_idx=df_dict[each].index.tolist()
        if each.endswith("3m"):
            fwdtbl=df_dict[each]
            fwd_idx=df_dict[each].index.tolist()
    
    dels=[]
    tr_list = []
    for each in spot_idx:
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
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    tbls1=[x+'Spreads' for x in df_dict.keys()]
    tbls2=[x+'Flys' for x in df_dict.keys()]

    if len(df_dict.values()[0])>60:
        spreads_dict=Tables2DF(crsr,*tbls1)
        flys_dict=Tables2DF(crsr,*tbls2)
    else:
        spreads_dict=Tables2DF(crsr,*tbls1,LB='1y')
        flys_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    
    rlt1={}
    rlt2={}
    for key1,df1 in spreads_dict.items():
        df1.sort_index(inplace=True)
        v1=df1.rolling(window=66).std()*np.sqrt(frequency_dict[frequency])
        v1=v1.dropna()
        key1=key1+'Vol'
        rlt1[key1]=v1
    
    for key2,df2 in flys_dict.items():
        df2.sort_index(inplace=True)
        v2=df2.rolling(window=66).std()*np.sqrt(frequency_dict[frequency])
        v2=v2.dropna()
        key2=key2+'Vol'
        rlt2[key2]=v2
     
    return rlt1,rlt2
    
def YieldsVol(df_dict,crsr,frequency='d'):
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    tbls=df_dict.keys()
    
    if len(df_dict.values()[0])>60:
        ylds_dict=Tables2DF(crsr,*tbls)
    else:
        ylds_dict=Tables2DF(crsr,*tbls,LB='1y')
    
    rlt1={}
    for key,df in ylds_dict.items():
        df.sort_index(inplace=True)
        v=df.rolling(window=66,min_periods=66).std()*np.sqrt(frequency_dict[frequency])
        v=v.dropna()
        key=key+'Vol'
        rlt1[key]=v
    
    return rlt1
    
    
    
def AdjRD(df_dict,crsr):
    tbls1=[x+'RD' for x in df_dict.keys()]
    tbls2=[x+'Vol' for x in df_dict.keys()]
    if len(df_dict.values()[0])>60:
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items():
        key2=key[: -2]+'Vol'
        df2=vols_dict[key2]
        idx2=df2.index.tolist()
        idx1=df.index.tolist()
        idx=list(set(idx1).intersection(idx2))
        idx.sort()
        df=df.loc[idx]
        df2=df2.loc[idx]
        df2.drop('3m',1,inplace=True)
        ard=df.div(df2)
        key3=key[: -2]+'AdjRD'
        rlt[key3]=ard
    return rlt

def AdjCarryTR(df_dict,crsr):
    for each in df_dict.keys():
        if each.endswith('Spot'):
            tbl1=each+'Carry'
            tbl2=each+'Vol'
            tbl3=each+'TR'
    
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
    
    df1=df1.loc[idx_c]
    df2_c=df2.loc[idx_c]
    df2_c.drop('3m',1,inplace=True)
    ac=df1.div(df2_c)
    key3=tbl1[:-5]+'AdjCarry'
    rlt[key3]=ac
    
    df3=df3.loc[idx_tr]
    df2_tr=df2.loc[idx_tr]
    df2_tr.drop('3m',1,inplace=True)
    atr=df3.div(df2_tr)
    key4=tbl3[:-2]+'AdjTR'
    rlt[key4]=atr    
    
    return rlt
    
    
def SpreadsFlysTR(df_dict):
    Convert_dict1={'2s5s':['2y','5y'],'5s10s':['5y','10y'],'2s10s':['2y','10y'],'1s2s':['1y','2y'],'2s3s':['2y','3y'],'1s3s':['1y','3y'],'3s5s':['3y','5y'],'5s7s':['5y','7y']}
    spreads=['2s5s','5s10s','2s10s','1s2s','2s3s','1s3s','3s5s','5s7s']
    
    Convert_dict2={'2s5s10s':['2y','5y','10y'],'5s7s10s':['5y','7y','10y'],'1s3s5s':['1y','3y','5y'],'3s5s7s':['3y','5y','7y'],'1s2s3s':['1y','2y','3y']}
    flys=['2s5s10s','5s7s10s','1s3s5s','3s5s7s','1s2s3s']
    
    
    tenors1=[]
    for each in spreads:
        tenors1=tenors1+Convert_dict1[each]
    prd1 = ['3m'] * len(tenors1)
    
    tenors2=[]
    for each in flys:
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
                    r2.append([2*y-z-x for x,y,z in zip(tr2[0::3],tr2[1::3],tr2[2::3])])
                else: dels.append(each)
            else:
                if each in spot_idx:  # For forward curves, compute spread roll down
                    s_dict = spottbl.loc[each].to_dict()
                    f_dict = df_dict[tbl].loc[each].to_dict()
                    yy = YieldCurve(**f_dict)
                    tr1=yy.calc_roll_down(tenors1,prd1,s_dict,tbl[-2:])
                    tr2=yy.calc_roll_down(tenors2,prd2,s_dict,tbl[-2:])
                    r1.append([x-y for x,y in zip(tr1[1::2],tr1[0::2])])
                    r2.append([2*y-z-x for x,y,z in zip(tr2[0::3],tr2[1::3],tr2[2::3])])
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
    tbls1=[x+'SpreadsRD' for x in df_dict.keys()]
    tbls2=[x+'SpreadsVol' for x in df_dict.keys()]
    if len(df_dict.values()[0])>60:
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items():
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
    tbls1=[x+'FlysRD' for x in df_dict.keys()]
    tbls2=[x+'FlysVol' for x in df_dict.keys()]
    if len(df_dict.values()[0])>60:
        RD_dict=Tables2DF(crsr,*tbls1)
        vols_dict=Tables2DF(crsr,*tbls2)
    else:
        RD_dict=Tables2DF(crsr,*tbls1,LB='1y')
        vols_dict=Tables2DF(crsr,*tbls2,LB='1y')
    
    rlt={}
    for key,df in RD_dict.items():
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
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    