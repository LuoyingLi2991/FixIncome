# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 09:11:39 2017

@author: luoying.li
"""
import numpy as np
import InterfaceFs
import pandas as pd


def calc_historic_vol(timeseries, frequency):
    frequency_dict={'d':252,'w':52, 'm':12, 'a':1}
    temp=[(timeseries[x+1]-timeseries[x])/timeseries[x] for x in range(len(timeseries)-1)]
    sigma_temp = np.std(temp,ddof=1)
    rlt=sigma_temp*np.sqrt(frequency_dict[frequency])
    return rlt



if __name__ == "__main__":
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')
    [crsr,cnxn]=InterfaceFs.Build_Access_Connect(conn_str)
    df=InterfaceFs.Tables2DF(crsr,'Spot',LB='1y').values()[0]
    s=df['6m'].tolist()
    print calc_historic_vol(s, 'd')
    
    

    
    
    
    
    
    
    
    
    
    
    
    
   
    