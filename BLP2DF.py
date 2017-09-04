import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import datetime, date
import pandas as pd
import testAccess
 


def bdh(ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY'):
        """
        Get ticker_list and field_list
        return dictionary of pandas dataframe with ticker as keyname
        """
        host='localhost'
        port=8194 # Create and fill the request for the historical data
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        session = blpapi.Session(sessionOptions)
        if not session.start():
            print("Failed to start session.")
        
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
        
        refDataService = session.getService("//blp/refdata")
        
        if isinstance(ticker_list,str):
            ticker_list = [ticker_list]
        if isinstance(fld_list,str):
            fld_list = [fld_list]
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')
        #print ticker_list,fld_list,start_date, end_date
        request = refDataService.createRequest("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)
        for f in fld_list:
            request.getElement("fields").appendValue(f)
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)

        #print("Sending Request:", request)
        # Send the request
        session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        while (True):
                # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent()
            if ev.eventType() in [5,6]:
                for msg in ev:
                   #print msg
                   ticker = msg.getElement('securityData').getElement('security').getValue()
                   fieldData = msg.getElement("securityData").getElement("fieldData")
                   for i in range(fieldData.numValues()):
                        for j in range(1, fieldData.getValue(i).numElements()):
                            data[(ticker, fld_list[j - 1])][
                                    fieldData.getValue(i).getElement(0).getValue()] = fieldData.getValue(i).getElement(j).getValue()
            if ev.eventType() == blpapi.Event.RESPONSE:
                    # Response completly received, so we could exit
                break
        
        pd_dict=dict()

        if len(data) == 0:
            # security error case
            return DataFrame()
        
        Default_Dict_Keys=data.keys()
        check=[0]*len(ticker_list)
        
        for i in range(len(ticker_list)):
            for each in Default_Dict_Keys:
                if each[0]==ticker_list[i] and check[i]==0:
                    pd_dict[ticker_list[i]]=DataFrame(zip(data[each].values(),data[each].keys()),columns=[each[1],'Date'])
                    pd_dict[ticker_list[i]].set_index('Date',inplace=True)
                    check[i]=1
                if each[0]==ticker_list[i] and check[i]==1:
                   pd_dict[ticker_list[i]].loc[:,each[1]]=pd.Series(data[each].values(),index=pd_dict[ticker_list[i]].index)
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
        key = ''.join(key.split()) # del all spaces in key
        if key not in tbls_names: # if table not created, create a new table
            header=list(df)
            Index=df.index.name
            cols=" (["+Index+"] date"
            for each in header:
                cols=cols+", ["+each+"]"+" double"
            cols=cols+", PRIMARY KEY([Date]))"
            query_ct="CREATE TABLE "+ str(key)+cols
            crsr.execute(query_ct)
            for index, row in df.iterrows():
                row=list(row)
                row.insert(0,index)
                var_string = ', '.join('?' * len(row))
                query_insert="INSERT INTO "+str(key)+" VALUES (%s);" % var_string
                crsr.execute(query_insert,row)
        else:  # write new data to database
            query_last="select top 1 [Date] from "+str(key)+" order by [Date] desc"
            crsr.execute(query_last)    
            Last_Index = datetime.date(crsr.fetchone()[0])
            df=df.loc[Last_Index :]
            if len(df.index)>1:
                count=0
                for index, row in df.iterrows():
                    if count==0:
                        count=1
                    else:
                        row=list(row)
                        row.insert(0,index)
                        var_string = ', '.join('?' * len(row))
                        query_insert="INSERT INTO "+str(key)+" VALUES (%s);" % var_string
                        crsr.execute(query_insert,row)
                        
                
   
if __name__ == "__main__":
    print "HistoryDataExtraction"
    try:
       data=bdh(["IBM US Equity","MSFT US Equity","AAPL US Equity"], ["PX_LAST","OPEN","VWAP_VOLUME"], "20170101","20170120")
       #print data
       conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                   'DBQ=C:\\Test.accdb;')
       
       srcDB = 'C:\\Test.accdb'
       destDB = 'C:\\Test_backup.accdb'
       
       #testAccess.Repair_Compact_DB(srcDB, destDB) # uncomment to repair and compact database 
       [crsr,cnxn]=testAccess.Build_Access_Connect(conn_str) 
      
       pd2DB (data,crsr)
       
       cnxn.commit()
       cnxn.close()
       
       
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."