# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 09:11:39 2017

@author: luoying.li
"""
import numpy as np
import InterfaceFs
import pandas as pd


def calc_historic_vol(timeseries, frequency):
    idx=timeseries.index[1:]
    timeseries=timeseries.values.tolist()
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    dyields=[(timeseries[x+1]-timeseries[x])/timeseries[x] for x in range(len(timeseries)-1)]
    df_dyields=pd.DataFrame(dyields,index=idx,columns=['dy'])
    df_dyields['stdev3m'] = df_dyields['dy'].rolling(window=66).std()*np.sqrt(frequency_dict[frequency])  # annulized three months rolling window std
    df_dyields = df_dyields.dropna()
    df_dyields.drop('dy',1,inplace=True)
    return df_dyields
            
    
if __name__ == "__main__":
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=InterfaceFs.Build_Access_Connect(conn_str)
    df=InterfaceFs.Tables2DF(crsr,'Spot',LB='1y').values()[0]
    s=df['6m']
    print calc_historic_vol(s, 'd')
    
    

    
    
    
    
    
    
    
    
    
    
    
    
   
    