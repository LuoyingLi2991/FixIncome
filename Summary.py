# -*- coding: utf-8 -*-
"""
Created on Fri Nov 10 17:35:51 2017
@author: luoying.li
"""
import xlwings as xw
import pandas as pd
import numpy as np
import InterfaceFs

def ChangeTitle(df):
    """Change column names xxxSpotSpreads/Flys RD to xxxSpotSpreads/Flys TR
    df: Dataframe
    """
    lvl1=list(df.columns.levels[0])
    for i, each in enumerate(lvl1):
        if 'Spot' in each:
            lvl1[i]=each[:-2]+'TR'
    lvl2=list(df.columns.levels[1])
    lvl=[lvl1,lvl2]
    df.columns.set_levels(lvl,inplace=True)
    return df

@xw.func
@xw.arg('Countries', np.array, ndim=2)
@xw.arg('Criteria', np.array, ndim=2)
@xw.ret(expand='table')
def Summary(Countries,path,LookBackWindow,Criteria):
    """Generate a Dataframe that summarizes potential opportunities of all countries
	 Countries: a list of Countries read from Excel
	 path: Database Directory
	 Criteria: a 4x2 array contains four sets of criteria
	 """
    TableList={}
    Curves=['Spot','Fwd3m','Fwd6m','Fwd1y','Fwd2y','Fwd3y','Fwd4y','Fwd5y']
    for each in Countries:  # Construct Yield Table names for each countries
        Temp=[]
        for C in Curves:
            Temp.append(each[0]+C)
        Temp.insert(0,'')
        TableList[each[0]]=[Temp]
    # lists to contains results from each criterion, later will be merged into one dataframe
    list1=pd.DataFrame() 
    list2=pd.DataFrame()
    list3=pd.DataFrame()
    list4=pd.DataFrame()
    for country, tblist in TableList.items():  # Filter opportunities for each countries
        lvl=InterfaceFs.YieldsLvLs(LookBackWindow,tblist,path)  # Get Level Table
        rd=InterfaceFs.RollDownTable(LookBackWindow,tblist,path)  # Get RollDown Table
        S=InterfaceFs.SpreadsTable(LookBackWindow,tblist,path)  # Get Spreads Table
        Srd=InterfaceFs.SpreadsRDTable(LookBackWindow,tblist,path)  # Get Spreads Rolldown Table
        Srd=ChangeTitle(Srd)
        F=InterfaceFs.ButterFlysTable(LookBackWindow,tblist,path) # Get Flys Table
        Frd=InterfaceFs.FlysRDTable(LookBackWindow,tblist,path)  # Get Flys Rolldown Table
        Frd=ChangeTitle(Frd)
        args=[lvl,rd,S,Srd,F,Frd]
		  # Filter based on criteria 1
        t1='Criteria: '+str(int(Criteria[0,0]*100))+'/'+str(int(Criteria[0,1]*100))
        r1=InterfaceFs.Filter(Criteria[0,0],Criteria[0,1],t1,*args)
        if not r1.empty:
            r1.set_index([len(r1.index)*[str(country)],r1.index.tolist()],inplace=True) 
        list1=list1.append(r1)
        # Filter based on criteria 2
        t2='Criteria: '+str(int(Criteria[1,0]*100))+'/'+str(int(Criteria[1,1]*100))
        r2=InterfaceFs.Filter(Criteria[1,0],Criteria[1,1],t2,lvl,rd,S,Srd,F,Frd)
        if not r2.empty:
            r2.set_index([len(r2.index)*[str(country)],r2.index.tolist()],inplace=True)
        list2=list2.append(r2)
        # Filter based on criteria 3
        t3='Criteria: '+str(int(Criteria[2,0]*100))+'/'+str(int(Criteria[2,1]*100))
        r3=InterfaceFs.Filter(Criteria[2,0],Criteria[2,1],t3,lvl,rd,S,Srd,F,Frd)
        if not r3.empty:
            r3.set_index([len(r3.index)*[str(country)],r3.index.tolist()],inplace=True)
        list3=list3.append(r3)
        # Filter based on criteria 4
        t4='Criteria: '+str(int(Criteria[3,0]*100))+'/'+str(int(Criteria[3,1]*100))
        r4=InterfaceFs.Filter(Criteria[3,0],Criteria[3,1],t4,lvl,rd,S,Srd,F,Frd)
        if not r4.empty:
            r4.set_index([len(r4.index)*[str(country)],r4.index.tolist()],inplace=True)
        list4=list4.append(r4)
    # Concate Four Dataframes into one    
    dff = pd.concat([list1,list2], axis=1)
    dff=pd.concat([dff,list3],axis=1)
    dff=pd.concat([dff,list4],axis=1)
    
    return dff

@xw.func
@xw.arg('df', pd.DataFrame, index=False, header=False)
def UltimateSummary(df):
    df=InterfaceFs.Convert2DF(df,5)
    c=list(df.columns.levels[0])
    rlt=pd.DataFrame()
    for each in c:
        rlt=pd.concat([rlt,df[each]])
    rlt.dropna(inplace=True)
    
    rlt=rlt.drop_duplicates(subset=['Asset'])
    rlt.reset_index(drop=True, inplace=True)
    lvl=rlt["Level%"].to_dict()
    rd=rlt["Rolldown%"].to_dict()
    rank=[]
    for each in lvl.keys():
        if lvl[each]<=0.4:
            rank.append(np.sqrt((1-lvl[each])*(1-lvl[each])+rd[each]*rd[each]))
        else:
            rank.append(np.sqrt(lvl[each]*lvl[each]+rd[each]*rd[each]))
    rlt["rank"]=rank
    rlt.sort_values(['rank'], ascending =False, inplace=True)
    rlt=rlt.head(30)
    rlt.drop(['rank'],axis=1,inplace=True)
    rlt.index = pd.RangeIndex(1,1 + len(rlt))
    return rlt
    

































