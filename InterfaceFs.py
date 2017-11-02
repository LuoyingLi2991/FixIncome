
import xlwings as xw
import pandas as pd
import numpy as np
import pyodbc
from dateutil.relativedelta import relativedelta
from YieldCurve import YieldCurve
from SpotCurve import SpotCurve
from UtilityClass import UtilityClass
import datetime
#import os
#import win32com.client


def convertT(t):
    """Function convert tenors to number e.g 3m->0.35 , 1y->1
    t: either a single tenor or a list of tenors
    """
    if isinstance(t,list):  # if t is a list 
        tt=[]
        for i,each in enumerate(t):
            if each[-1]=='m':
                tt.append(float(each[:-1])/12)  # convert month tenor to number
            else:
                tt.append(int(each[:-1]))  # convert year tenor to number
    else:  # Convert single tenor
        if t[-1]=='m':
            tt=float(t[:-1])/12
        else:
            tt=int(t[:-1])
    return tt

def FormatTables(crsrs,tbls,LB):
    """Function both spot and forward tables from database based on table names(tbls) and LookBackWindow(LB) and Format both tables to have same index"""
    if LB=='ALL':
        df=Tables2DF(crsrs,*tbls)
    else:  
        df=Tables2DF(crsrs,*tbls,lb=LB)
    
    spot=df[tbls[0]]  
    fwd=df[tbls[1]]

    idx1=spot.index.tolist()
    idx2=fwd.index.tolist()
    idx=list(set(idx1).intersection(idx2))  # Find same index 
    idx.sort()
    spot=spot.loc[idx]  # Filter Spot to same index
    fwd=fwd.loc[idx]  # Filter Forward to same index
    return spot,fwd,idx


@xw.func
@xw.ret(expand='table')
def CalculatorsDF(Country,LB,AssetType,Curve,tenor1,tenor2,tenor3,path):
    """Calculate Level/Spread, Rolldown, Vol Adjusted Rolldown Based on attributes passed
    Country: Desired Country
    LB: LookBackWindow
    AssetType: Level/Spread/Fly
    Curve: Indicating if the curve is spot curve or forward curve
    tenor1: first tenor
    tenor2: second tenor
    tenor3: third tenor
    path: directory of database
    """
    # Connect to YieldData database
    YldsDB1= 'DBQ='+str(path+ '\\YieldsData.accdb')
    conn1 = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};' + YldsDB1)
    [crsr1,cnxn1]=Build_Access_Connect(conn1)  # Connect to MS Access
    crsrs=[(crsr1,cnxn1)]
        
    lvls=[]
    rd=[]
    if Curve=='Spot': # if curve belongs to spot curve
        tbls=[Country+Curve,Country+'Fwd3m']
        [spot,fwd,idx]=FormatTables(crsrs,tbls,LB)  # Get Spot and Forward 3m Yields tables for this country
        for each in idx:  # Construct YieldCurve and SpotCurve object for each row and Calculate Rolldown 
            k1=spot.loc[each].to_dict()
            k2=fwd.loc[each].to_dict()
            yc=YieldCurve(**k1)
            sc=SpotCurve(k1,k2)
            if AssetType=='Spread':  # Calculate Spread and Spread rolldown
                tenors=[tenor1,tenor2]
                s=yc.build_curve(convertT(tenors))
                lvls.append(s[1]-s[0])
                rs=sc.calc_total_return(tenors,['3m']*len(tenors))
                rd.append(rs[1]-rs[0])
            elif AssetType=='Fly':  # Calculate FLys and Flys rolldown
                tenors=[tenor1,tenor2,tenor3]
                s=yc.build_curve(convertT(tenors))
                lvls.append(-2*s[1]+s[2]+s[0])
                rs=sc.calc_total_return(tenors,['3m']*len(tenors))
                rd.append(-2*rs[1]+rs[2]+rs[0])
            else:  # Calculate yield level and level rolldown 
                tenors=tenor1
                lvls.append(yc.build_curve(convertT(tenors)))
                rd.append(sc.calc_total_return(tenors,'3m'))
    else:  # Curve belongs to forward curve
        tbls=[Country+'Spot',Country+Curve]
        [spot,fwd,idx]=FormatTables(crsrs,tbls,LB)  # Get Desired forward curve and spot curve
        for each in idx: # Construct YieldCurve and SpotCurve object for each row and Calculate Rolldown 
            k1=spot.loc[each].to_dict()
            k2=fwd.loc[each].to_dict()
            yc=YieldCurve(**k2)
            if AssetType=='Spread':  # Calculate Spread and Spread rolldown
                tenors=[tenor1,tenor2]
                s=yc.build_curve(convertT(tenors))
                lvls.append(s[1]-s[0])
                rs=yc.calc_roll_down(tenors,['3m']*len(tenors),k1,tbls[1][-2:])
                rd.append(rs[1]-rs[0])
            elif AssetType=='Fly':  # Calculate FLys and Flys rolldown
                tenors=[tenor1,tenor2,tenor3]
                s=yc.build_curve(convertT(tenors))
                lvls.append(-2*s[1]+s[2]+s[0])
                rs=yc.calc_roll_down(tenors,['3m']*len(tenors),k1,tbls[1][-2:])
                rd.append(-2*rs[1]+rs[2]+rs[0])
            else:  # Calculate yield level and level rolldown 
                tenors=tenor1
                lvls.append(yc.build_curve(convertT(tenors)))
                rd.append(yc.calc_roll_down(tenors,'3m',k1,tbls[1][-2:]))
    rlt=pd.DataFrame(lvls,index=idx,columns=['Level'])  # Construct a dataframe
    rlt['rolldown']=rd  # Add rolldown column
    rlt['dy']=rlt['Level'].diff()  # Get yield difference
    rlt=rlt.dropna() 
    rlt['vol']=rlt['dy'].rolling(window=66).std()*np.sqrt(252)  # 3m rolling window for dyield
    rlt=rlt.dropna()
    rlt['AdjRD']=rlt['rolldown']/rlt['vol']  # Calc Vol Adjusted rolldown
    u=UtilityClass()
    z=u.calc_z_score(rlt['AdjRD'].to_frame(),False,'all')[1]  # Calc Z score for vol adjusted rolldown
    rlt['AdjRDZscore']=z
    AdjRD=rlt['AdjRD'].tolist()
    rlt.drop(['dy','vol','AdjRD'],axis=1,inplace=True)  # Delete unwanted tables
    headers=list(rlt)
    H=['L','R','Z']
    for each, h in zip(headers,H):  # Add columns 'Aver', '+1sd','-1sd', '+2sd', '-2sd' for 'Level', 'rolldown' and 'Zscore'
        rlt['aver'+h]=[np.mean(rlt[each].tolist())]*len(rlt[each].tolist())
        std=np.std(rlt[each].tolist())
        rlt['+1sd'+h]=[rlt['aver'+h].values[0]+std]*len(rlt[each].tolist())  #  1 std above average
        rlt['-1sd'+h]=[rlt['aver'+h].values[0]-std]*len(rlt[each].tolist())  #  1 std below average
        rlt['+2sd'+h]=[rlt['aver'+h].values[0]+2*std]*len(rlt[each].tolist())  #  2 std above average
        rlt['-2sd'+h]=[rlt['aver'+h].values[0]-2*std]*len(rlt[each].tolist()) 
    headers=['Level','averL','+1sdL','-1sdL','+2sdL','-2sdL','rolldown','averR','+1sdR','-1sdR','+2sdR','-2sdR',
             'AdjRDZscore','averZ','+1sdZ','-1sdZ','+2sdZ','-2sdZ']  
    rlt=rlt[headers]  # Format Dataframe to desired column sequence
    rlt['AdjRD']=AdjRD  # Add AdjRD Column
    return rlt

@xw.func
@xw.arg('df', pd.DataFrame, index=True, header=True)  # Get Dataframe from Excel GUI
def CalcTbl(df,Country,Asset,Curve,tenor1,tenor2,tenor3):
    """This Function formulates a table that contains Today, 1 week before and 1 month before's level/Spread, rolldown, vol adjusted rolldown and their zscore and percentile
    df: Dataframe read from Excel GUI. This Dataframe is also the result calculated from 'CalculatorsDF' function
    Asset: Asset Type, possible choices: Level, Spread and Fly
    tenor1: First tenor
    tenor2: second tenor
    tenor3 third tenor
    """
    df1 = df[['Level','rolldown','AdjRD']]  # Select columns to a new dataframe
    L=df1['Level'].to_frame()  # Convert Column to Dataframe
    L.rename(columns={'Level':'TTT'}, inplace=True)  # Change Column name 
    R=df1['rolldown'].to_frame() # Convert Column to Dataframe
    R.rename(columns={'rolldown':'TTT'}, inplace=True) # Change Column name 
    A=df1['AdjRD'].to_frame() # Convert Column to Dataframe
    A.rename(columns={'AdjRD':'TTT'}, inplace=True) # Change Column name 
    df_dict={'L':L,'R':R,'A':A}  # Create a dataframe dictionary
    headers=['TTT']
    tbls=['L','R','A']
    if Asset=='Level':  # If asset type is Level, set corresponding tenor, header, index and calculate result table
        tenor=Curve+tenor1
        rlt_header=[['Level%']*3+['RollDown(bsp)']*3+['AdjRollDown']*3,['Lvl','Z','PCTL']*3]
        rlt_idx=[[tenor]*3,['Today','1W Before','1M Before']]
        rlt=GetRltDF(tbls,df_dict,headers,rlt_header,rlt_idx,True)
        rlt.loc[:,('RollDown(bsp)','Lvl')]=rlt['RollDown(bsp)']['Lvl'].apply(lambda x: x*100)
    elif Asset=='Spread':  # If asset type is Spread, set corresponding tenor, header, index and calculate result table
        tenor=Curve+"/"+tenor1[:-1]+"s"+tenor2[:-1]+"s"
        rlt_header=[['Spread(bsp)']*3+['SpreadRollDown(bsp)']*3+['SpreadAdjRollDown']*3,['Lvl','Z','PCTL']*3]
        rlt_idx=[[tenor]*3,['Today','1W Before','1M Before']]
        rlt=GetRltDF(tbls,df_dict,headers,rlt_header,rlt_idx)
    else:  # If asset type is Fly, set corresponding tenor, header, index and calculate result table
        tenor=Curve+"/"+tenor1[:-1]+"s"+tenor2[:-1]+"s"+tenor3[:-1]+"s"
        rlt_header=[['Fly(bsp)']*3+['FlyRollDown(bsp)']*3+['FlyAdjRollDown']*3,['Lvl','Z','PCTL']*3]
        rlt_idx=[[tenor]*3,['Today','1W Before','1M Before']]
        rlt=GetRltDF(tbls,df_dict,headers,rlt_header,rlt_idx) 
    rlt.index.set_labels([0,-1,-1],level=0,inplace=True)  # Change label for multiIndex
    # Change label for multiColumnHeaders
    l=list(rlt.columns.labels[0])
    for i in range(2):
        l[i*3+2]=-1
        l[i*3+1]=-1
    rlt.columns.set_labels(l,level=0,inplace=True)
    rlt.index.names=(Country,None)
    columns= list(rlt.columns.levels[0])
    for each in columns: # Drop Adjusted Roll down's level
        if each.endswith("AdjRollDown"):
            rlt.drop((each,'Lvl'),axis=1,inplace=True)
    return rlt



def FormatIdx(idx):
    """This funtion formats index into multiIndex"""
    a=list(idx.labels[0])  # Get label from index
    for i in range(len(a)/3):  # Format label
        a[3*i+1]=a[3*i]
        a[3*i+2]=a[3*i]
    b=list(idx.labels[1])
    label=[a,b] 
    idx.set_labels(label,inplace=True)  # Set Labels for index
    idx.names=(None,None)  # Set index names to None
    return idx  # Return index


def Convert2DF(df):
    """This function converts tables in Excel to Dataframe"""
    df.drop(df.index[0],inplace=True)  # Drop the first row due to the format of table read from Excel GUI
    df1=df.T  # Transpose dataframe
    df1.set_index([1,2], inplace=True)  # Set first two columns as index
    h=df1.index
    h=h[2:]  # Get Index and delete first two, as the first two are None
    h=FormatIdx(h)  # Format Index 
    df.set_index([0,1], inplace=True)  # Set first two columns as index for None transposed dataframe
    idx=df.index[2:]  # Get Index and format index
    idx=FormatIdx(idx)
    df=df[2:]  # Delete first two rows
    df=pd.DataFrame(df.values.tolist(),columns=h,index=idx)  # Construct MultiIndex Dataframe
    return df


def Filter(c1,c2,Title,*args):
    """Select data that fullfill the criteria: location %>c1 AND RollDown%>c2
    Title: Title of this criteria
    *args: a list of dataframes with sequence like: location, rolldown,location,rolldown....
    """
    n=len(args)/2  # Get number of combinations
    asset=[]
    level=[]
    RD=[]
    P=[]
    for i in range(n):  # For each combination, go through filter process
        lvl=Convert2DF(args[2*i])
        rd=Convert2DF(args[2*i+1])
        curves=list(lvl.columns.levels[0])
        tenors=list(rd.index.levels[0])
        lvl1=lvl.filter(like='Today', axis=0)  # Filter Out Today's Locations
        lvl1=lvl1.filter(like='PCTL', axis=1)  # Filter Out Percentile of Today's Locations
        rd1=rd.filter(like='Today', axis=0)  # Filter Out Today's Rolldowns         
        rd1=rd1.filter(like='PCTL', axis=1)  # Filter Out Percentile of Today's Rolldowns
        for curve in curves:
            if curve.endswith("Spot"): rdC=curve+'TR'  # RollDown table has different header names
            else: rdC=curve+'RD' 
            for t in tenors:  # Start filter process
                if ((lvl1[curve].loc[t].values[0][0]>c1) or(lvl1[curve].loc[t].values[0][0]<1-c1)) and rd1[rdC].loc[t].values[0][0]>c2:
                    level.append(lvl[curve]['Level'].loc[t]['Today'])
                    RD.append(rd[rdC]['Level'].loc[t]['Today'])
                    P.append(lvl[curve]['PCTL'].loc[t]['Today'])
                    asset.append(curve+'/'+t)
    for i,each in enumerate(P):
        P[i]="{0:.0f}%".format(each * 100)
    vals=[asset,level,RD,P]  
    df=pd.DataFrame(vals,index=[[Title]*4,['Asset','Level','RollDown','Location%']])  # Construct results dataframe
    df.index.set_labels([0,-1,-1,-1],level=0,inplace=True)  
    df=df.T
    if not df.empty:
        df.index = pd.RangeIndex(1,1 + len(df))
    return df
    
    
    

@xw.func
@xw.arg('lvl', pd.DataFrame, index=False, header=False) # Yields Level Table in Excel GUI
@xw.arg('rd', pd.DataFrame, index=False, header=False)  # RollDown Table in Excel GUI
@xw.arg('S', pd.DataFrame, index=False, header=False)  # Spreads Table in Excel GUI
@xw.arg('F', pd.DataFrame, index=False, header=False)  # Flys Table in Excel GUI
@xw.arg('Srd', pd.DataFrame, index=False, header=False)  # Spreads Rolldown Table in Excel GUI
@xw.arg('Frd', pd.DataFrame, index=False, header=False)  # Flys RollDown Table in Excel GUI
@xw.ret(expand='table')
def List1(lvl,rd,S,Srd,F,Frd):
    """Display results for Criteria 1"""
    args=[lvl,rd,S,Srd,F,Frd]
    c1=0.8
    c2=0.8
    Title='Criteria: 80/80'
    return Filter(c1,c2,Title,*args)

@xw.func
@xw.arg('lvl', pd.DataFrame, index=False, header=False) # Yields Level Table in Excel GUI
@xw.arg('rd', pd.DataFrame, index=False, header=False)  # RollDown Table in Excel GUI
@xw.arg('S', pd.DataFrame, index=False, header=False)  # Spreads Table in Excel GUI
@xw.arg('F', pd.DataFrame, index=False, header=False)  # Flys Table in Excel GUI
@xw.arg('Srd', pd.DataFrame, index=False, header=False)  # Spreads Rolldown Table in Excel GUI
@xw.arg('Frd', pd.DataFrame, index=False, header=False)  # Flys RollDown Table in Excel GUI
@xw.ret(expand='table')
def List4(lvl,rd,S,Srd,F,Frd):
    """Display results for Criteria 4"""
    args=[lvl,rd,S,Srd,F,Frd]
    c1=0.95
    c2=0.4
    Title='Criteria:95/40'
    return Filter(c1,c2,Title,*args)


@xw.func
@xw.arg('lvl', pd.DataFrame, index=False, header=False) # Yields Level Table in Excel GUI
@xw.arg('rd', pd.DataFrame, index=False, header=False)  # RollDown Table in Excel GUI
@xw.arg('S', pd.DataFrame, index=False, header=False)  # Spreads Table in Excel GUI
@xw.arg('F', pd.DataFrame, index=False, header=False)  # Flys Table in Excel GUI
@xw.arg('Srd', pd.DataFrame, index=False, header=False)  # Spreads Rolldown Table in Excel GUI
@xw.arg('Frd', pd.DataFrame, index=False, header=False)  # Flys RollDown Table in Excel GUI
@xw.ret(expand='table')
def List2(lvl,rd,S,Srd,F,Frd):
    """Display results for Criteria 2"""
    args=[lvl,rd,S,Srd,F,Frd]
    c1=0.9
    c2=0.6
    Title='Criteria:90/60'
    return Filter(c1,c2,Title,*args)

@xw.func
@xw.arg('lvl', pd.DataFrame, index=False, header=False) # Yields Level Table in Excel GUI
@xw.arg('rd', pd.DataFrame, index=False, header=False)  # RollDown Table in Excel GUI
@xw.arg('S', pd.DataFrame, index=False, header=False)  # Spreads Table in Excel GUI
@xw.arg('F', pd.DataFrame, index=False, header=False)  # Flys Table in Excel GUI
@xw.arg('Srd', pd.DataFrame, index=False, header=False)  # Spreads Rolldown Table in Excel GUI
@xw.arg('Frd', pd.DataFrame, index=False, header=False)  # Flys RollDown Table in Excel GUI
@xw.ret(expand='table')
def List3(lvl,rd,S,Srd,F,Frd):
    """Display results for Criteria 2"""
    args=[lvl,rd,S,Srd,F,Frd]
    c1=0.6
    c2=0.9
    Title='Criteria:60/90'
    return Filter(c1,c2,Title,*args)    
    
    

@xw.func
@xw.ret(expand='table')
def FwdPlot(Country,path):
    """ Return "1y/1y-1y" DataFrame of Country
    Country: Desired Country
    path: Path of the folder where database locates at
    """
    print path
    fwd=str(Country)+"Fwd1y"  # Construct 1 year forward table name
    spot=str(Country)+"Spot"  # Construct spot table name
    YldsDB1= 'DBQ='+str(path+ '\\YieldsData.accdb')
    conn1 = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};' + YldsDB1)
    [crsr1,cnxn1]=Build_Access_Connect(conn1)  # Connect to MS Access
    crsrs=[(crsr1,cnxn1)]
    df=Tables2DF(crsrs,spot,fwd)  # return dictionary of tables from database
    fwd=df[fwd]  # get forward table from dictionary
    spot=df[spot]  # get spot table from dictionary
    tenors=list(spot)  # get tenor headers from spot table
    t='1y'  
    
    indx=spot.index.tolist()  
    indx_fwd=fwd.index.tolist()
    dels=[]
    
    if t in tenors: # no need to interpolate for 1y point
        r=[]
        for each in indx:
            if each in indx_fwd: # match dates between forward and spot
                r.append(fwd.loc[each].to_dict()[t]-spot.loc[each].to_dict()[t])
            else:
                dels.append(each) 
        for each in dels:  # delete spot dates that are not in forward dates
            indx.remove(each)
    else: # interpolation is needed for 1y point
        r=[]
        for each in indx:
            if each in indx_fwd:  # Match dates between spot and forward dates
                f=fwd.loc[each].tolist()
                s=spot.loc[each].tolist()
                f_kwarg=dict(zip(tenors,f))
                s_kwarg=dict(zip(tenors,s))
                y1=YieldCurve(**f_kwarg)  # forward interpolation for 1y point
                y2=YieldCurve(**s_kwarg)  # spot interpolation for 1y point
                r.append(y1.build_curve(1)-y2.build_curve(1)) # calc spread
            else:
                dels.append(each)
            
        for each in dels:
                indx.remove(each)
   
    rlt=pd.DataFrame(r,index=indx,columns=['1y/1y-1y'])  # Construct a dataframe
    #  Add average value column
    rlt['Aver']=[np.mean(rlt['1y/1y-1y'].tolist())]*len(rlt['1y/1y-1y'].tolist())
    sd=np.std(rlt['1y/1y-1y'].tolist())
    rlt['+1sd']=[rlt['Aver'].values[0]+sd]*len(rlt['1y/1y-1y'].tolist())  #  1 std above average
    rlt['-1sd']=[rlt['Aver'].values[0]-sd]*len(rlt['1y/1y-1y'].tolist())  #  1 std below average
    rlt['+2sd']=[rlt['Aver'].values[0]+2*sd]*len(rlt['1y/1y-1y'].tolist())  #  2 std above average
    rlt['-2sd']=[rlt['Aver'].values[0]-2*sd]*len(rlt['1y/1y-1y'].tolist())  #  2 std below average
    return rlt


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

def removeUni(l):
    """convert unicode to string"""
    result=[]
    for each in l:
        each=each.replace(u'\xa0', ' ').encode('utf-8')
        result.append(each)
    return result

def Tables2DF(crsrs,*selected_table_name,**LookBackWindow):
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
    
    crsr=crsrs[0][0]
    cnxn=crsrs[0][1]
    
    # Get table columns and saved in dictionary
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
        cnxn.close() 
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
        cnxn.close()
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
         cnxn.close()
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
         cnxn.close() 
         return df_dict

"""
@xw.func
def Repair_Compact_DB(path):
    oApp = win32com.client.Dispatch("Access.Application")
    srcDB=str(path+'\\TempData.accdb')
    destDB = str(path+'\\TempData_backup.accdb')
    oApp.compactRepair(srcDB,destDB)
    os.remove(destDB)
    oApp = None
"""

def GetTbls(TableList,LookBackWindow,TblSuffix,path):
    """Return Tables extracted from Database
    TableList: List of Tables YieldsTables
    TblSuffix: Suffix Added to the tables later
    path: Path of the folder where database locates at
    """
    TableList=removeUni(np.delete(TableList[0], 0))
    tbls=[]  # Construct tablenames 
    for each in TableList:
        tbls.append(each+TblSuffix)
    # Connect to Database
    tempDB= 'DBQ='+str(path+'\\TempData.accdb')   
    conn = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + tempDB)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn)
    crsrs=[(crsr,cnxn)]
    
    # Get tables from Databse
    if str(LookBackWindow)!="ALL":
        df_dict=Tables2DF(crsrs,*tbls,LB=str(LookBackWindow))
    else: df_dict=Tables2DF(crsrs,*tbls)
    
    headers=list(df_dict.values()[0])  # Get column names
    
    temp1=[]  # Construct result dataframe's column names
    for tbl in TableList:
        temp1=temp1+[tbl]*3
    rlt_cols = [np.array(temp1), np.array(['Level', 'Z', 'PCTL']*len(TableList))]    
    
    temp2=[]  # Construct result dataframe's index
    for header in headers:
        temp2=temp2+[header]*3
    rlt_index = [np.array(temp2), np.array(['Today', '1W Before', '1M Before']*len(headers))]

    return df_dict, rlt_cols, rlt_index, headers, tbls


def GetRltDF(tbls,df_dict,headers,rlt_cols,rlt_index,*ylds):
    """Templete Function that constructs tables for Excel GUI
    tbls: list of table names with desired orders
    df_dict: dictionary contains all tables extracted from database
    headers: Column names of database table
    rlt_cols/rlt_index: column names/index for returned dataframe
    *ylds: an args to indicate if df_dict are yields
    """
    Values=[] 
    u=UtilityClass() 
    for tbl in tbls: 
        df=df_dict[tbl].sort_index()
        df.dropna(inplace=True)
        lvl=[]
        z=[]
        p=[]
        for each in headers: # for each column in dataframe, compute zscore and percentile
            ss=df[each].to_frame()
            [s_lvl,s_zscore]=u.calc_z_score(ss,False,'1w','1m')
            s_ptl=u.calc_percentile(ss,'1w','1m')
            if ylds==(): # Convert to basis points if not yields
                temp=[x*100 for x in s_lvl]
                s_lvl=temp
            lvl=lvl+s_lvl
            z=z+s_zscore
            p=p+s_ptl
        Values.append(lvl)
        Values.append(z)
        Values.append(p)
    tt=np.asarray(Values) 
    rlt = pd.DataFrame(tt, index=rlt_cols, columns =rlt_index) # Construct result dataframe  
    
    return rlt.T
    

@xw.func
@xw.arg('TableList', np.array, ndim=2)
#@xw.ret(expand='table')
def SpreadsTable(LookBackWindow,TableList,path):
    """Return Today, 1week before and 1month before's Spreads Level, Z_score, Percentile
    Arguments:
        path: database file directory
        LookBackWindow: Whether to select part of the table
        TableList: a list of tables needed
    Output: Today, 1week and 1month's spreads level,zscore(asymmetric) and percentile    
    """
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, spreads, spreadstbls]=GetTbls(TableList,LookBackWindow,'Spreads',path)
    rlt=GetRltDF(spreadstbls,df_dict,spreads,rlt_cols,rlt_index)
    
    print datetime.datetime.now()-tttt # print time needed running this function
    return rlt
 
    
@xw.func
@xw.arg('TableList', np.array, ndim=2)
#@xw.ret(expand='table')
def SpreadsRDTable(LookBackWindow,TableList,path):
    """For Spot Curves, return Spread Total Return
    For Forward Curves, return Spread Roll Down
    path: database file directory
    LookBackWindow: Whether to select part of the table
    TableList: a list of tables needed"""
    
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, spreads, spreadsRDtbls]=GetTbls(TableList,LookBackWindow,'SpreadsRD',path)
    rlt=GetRltDF(spreadsRDtbls,df_dict,spreads,rlt_cols,rlt_index)  
    print datetime.datetime.now()-tttt # print time needed running this function
    return rlt


@xw.func
@xw.arg('TableList', np.array, ndim=2)
#@xw.ret(expand='table')
def SpreadsAdjRD(LookBackWindow,TableList,path):
    """For Spot Curves, return Spread Total Return
    For Forward Curves, return Spread Roll Down
    path: database file directory
    LookBackWindow: Whether to select part of the table
    TableList: a list of tables needed"""
    
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, spreads, spreadsRDtbls]=GetTbls(TableList,LookBackWindow,'SpreadsAdjRD',path)
    rlt=GetRltDF(spreadsRDtbls,df_dict,spreads,rlt_cols,rlt_index).drop('Level',axis=1,level=1)  
    print datetime.datetime.now()-tttt # print time needed running this function
    return rlt




@xw.func
@xw.arg('TableList', np.array, ndim=2)
def ButterFlysTable(LookBackWindow,TableList,path):
    """Return Today, 1week before and 1month before's Flys Level, Z_score, Percentile
    Arguments:
        path: database file directory
        LookBackWindow: Whether to select part of the table
        TableList: a list of tables needed
    """
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, flys, flystbls]=GetTbls(TableList,LookBackWindow,'Flys',path)
    rlt=GetRltDF(flystbls,df_dict,flys,rlt_cols,rlt_index)      
    print datetime.datetime.now()-tttt
    return rlt


@xw.func
@xw.arg('TableList', np.array, ndim=2)
#@xw.ret(expand='table')
def FlysRDTable(LookBackWindow,TableList,path):
    """For Spot Curves, return Spread Total Return
    For Forward Curves, return Spread Roll Down
    path: database file directory
    LookBackWindow: Whether to select part of the table
    TableList: a list of tables needed"""
    
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, flys, flysRDtbls]=GetTbls(TableList,LookBackWindow,'FlysRD',path)
    rlt=GetRltDF(flysRDtbls,df_dict,flys,rlt_cols,rlt_index)      
    print datetime.datetime.now()-tttt  # print time needed running the function
    return rlt


@xw.func
@xw.arg('TableList', np.array, ndim=2)
#@xw.ret(expand='table')
def FlysAdjRD(LookBackWindow,TableList,path):
    """For Spot Curves, return Spread Total Return
    For Forward Curves, return Spread Roll Down
    path: database file directory
    LookBackWindow: Whether to select part of the table
    TableList: a list of tables needed"""
    
    tttt=datetime.datetime.now()
    [df_dict, rlt_cols, rlt_index, flys, flysRDtbls]=GetTbls(TableList,LookBackWindow,'FlysAdjRD',path)
    rlt=GetRltDF(flysRDtbls,df_dict,flys,rlt_cols,rlt_index).drop('Level',axis=1,level=1)  
    print datetime.datetime.now()-tttt # print time needed running this function
    return rlt


    
@xw.func
@xw.arg('TableList', np.array, ndim=2)
def AdjRollDownTable(LookBackWindow,TableList,path):
    """Return Today, 1week before and 1month before's Adj_RollDown, Z_score, Percentile
    Arguments:
        path: database file directory
        LookBackWindow: Whether to select part of the table
    """
    tttt=datetime.datetime.now()
    TableList=removeUni(np.delete(TableList[0], 0))
    tbls=[]
    for each in TableList:
        if each.endswith('Spot'):
            tbls.append(each+'AdjTR')
        else: tbls.append(each+'AdjRD')
    
    tempDB= 'DBQ='+str(path+'\\TempData.accdb')
    conn = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + tempDB)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn)
    
    
    crsrs=[(crsr,cnxn)]
    
    if str(LookBackWindow)!="ALL":
        df_dict=Tables2DF(crsrs,*tbls,LB=str(LookBackWindow))
    else: df_dict=Tables2DF(crsrs,*tbls)
   
    tenors=list(df_dict.values()[0])
    
    temp1=[]
    for tbl in tbls:
        temp1=temp1+[tbl]*3
        
    temp2=[]
    for t in tenors:
        temp2=temp2+[t]*3
        
    rlt_cols = [np.array(temp1), np.array(['Level', 'Z', 'PCTL']*len(tbls))]
    rlt_index = [np.array(temp2), np.array(['Today', '1W Before', '1M Before']*len(tenors))]
    
    rlt=GetRltDF(tbls,df_dict,tenors,rlt_cols,rlt_index).drop('Level',axis=1,level=1)

    print datetime.datetime.now()-tttt
    return rlt




@xw.func
@xw.arg('TableList', np.array, ndim=2)
def RollDownTable(LookBackWindow,TableList,path):
    """Return Today, 1week before and 1month before's Total Return Level, Z_score, Percentile
    Arguments:
        path: database file directory
        LookBackWindow: Whether to select part of the table
    """
    tttt=datetime.datetime.now()
    TableList=removeUni(np.delete(TableList[0], 0))
    tbls=[]
    for each in TableList:
        if each.endswith('Spot'):
            tbls.append(each+'TR')
        else: tbls.append(each+'RD')
    
    tempDB= 'DBQ='+str(path+'\\TempData.accdb')
    conn = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + tempDB)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn)
    
    crsrs=[(crsr,cnxn)]
    
    if str(LookBackWindow)!="ALL":
        df_dict=Tables2DF(crsrs,*tbls,LB=str(LookBackWindow))
    else: df_dict=Tables2DF(crsrs,*tbls)
    
    tenors=list(df_dict.values()[0])
    
    temp1=[]
    for tbl in tbls:
        temp1=temp1+[tbl]*3
        
    temp2=[]
    for t in tenors:
        temp2=temp2+[t]*3
        
    rlt_cols = [np.array(temp1), np.array(['Level', 'Z', 'PCTL']*len(tbls))]
    rlt_index = [np.array(temp2), np.array(['Today', '1W Before', '1M Before']*len(tenors))]
    
    rlt=GetRltDF(tbls,df_dict,tenors,rlt_cols,rlt_index)
    print datetime.datetime.now()-tttt
    return rlt
    

@xw.func
@xw.arg('TableList', np.array, ndim=2)
def YieldsLvLs(LookBackWindow,TableList,path):
    """Return YieldsLevels,zscore,percentiles"""
    #Repair_Compact_DB(path)
    TableList=removeUni(np.delete(TableList[0], 0))
    #connect to databse
    YldsDB= 'DBQ='+str(path+'\\YieldsData.accdb')
    conn = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; ' + YldsDB)  #Create database connection string
    [crsr,cnxn]=Build_Access_Connect(conn)    
    crsrs=[(crsr,cnxn)]
    
    if str(LookBackWindow)!="ALL":
        df_dict=Tables2DF(crsrs,*TableList,LB=str(LookBackWindow))
    else: df_dict=Tables2DF(crsrs,*TableList)
    
    headers=list(df_dict.values()[0])
    
    temp1=[]
    for tbl in TableList:
        temp1=temp1+[tbl]*3
        
    temp2=[]
    for header in headers:
        temp2=temp2+[header]*3
        
    rlt_cols = [np.array(temp1), np.array(['Level', 'Z', 'PCTL']*len(TableList))]
    rlt_index = [np.array(temp2), np.array(['Today', '1W Before', '1M Before']*len(headers))]
    
    rlt=GetRltDF(TableList,df_dict,headers,rlt_cols,rlt_index,True)
    return rlt



if __name__ == "__main__":
    LB='ALL'
    path='P:\\Interest Rate Model'
    tbls=[['','EURSpot']]
    C='US'
    AdjRollDownTable(LB,tbls,path)