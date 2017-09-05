import pyodbc
import pandas as pd
import win32com.client
import os
from YieldCurve import YieldCurve
from UtilityClass import UtilityClass
from SpotCurve import SpotCurve



def Repair_Compact_DB(srcDB, destDB):
    """Repair and Compact the DataBase"""
    oApp = win32com.client.Dispatch("Access.Application")
    oApp.compactRepair(srcDB, destDB)
    os.remove(destDB)
    oApp = None

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

def Tables2DF(crsr):
    """Reformat All Tables in DataBase to Pandas DataFrame and Stored in a dictionary with table_names as keys
    Argument:
    crsr ---cursor from access
    Output:
    Dictionary of DataFrames with table_names as keys
    """
    db_schema = dict()
    tbls = crsr.tables(tableType='TABLE').fetchall()

    for tbl in tbls:
        if tbl.table_name not in db_schema.keys():
            db_schema[tbl.table_name] = list()
        for col in crsr.columns(table=tbl.table_name):
            db_schema[tbl.table_name].append(col[3])
    #print(db_schema)
    df_dict=dict()
    for tbl, cols in db_schema.items():
        sql = "SELECT * from %s" % tbl  # Dump data
        crsr.execute(sql)
        val_list = []
        while True:
            row = crsr.fetchone()
            if row is None:
                break
            val_list.append(list(row))
        temp_df = pd.DataFrame(val_list, columns=cols)
        temp_df.set_index(keys=cols[0], inplace=True) # First Column as Key
        df_dict[tbl]=temp_df
    return df_dict

def analyse(df_dict):
    """conduct analyse"""
    header = list(df_dict['Spot'])
    index = df_dict['Spot'].index
    spot_val_list = df_dict['Spot'].values.tolist()
    roll_down_list = []
    prd = ['3m'] * (len(header) - 1)
    for vals in spot_val_list:
        kwarg = dict(zip(header, vals))
        yieldcurve = YieldCurve(**kwarg)
        temp = yieldcurve.calc_roll_down(header[1:], prd)
        temp.insert(0, vals[0])
        roll_down_list.append(temp)
    df_roll_down = pd.DataFrame(roll_down_list, index=index,
                                columns=header)  # create roll down dataframe with all rolldown
    cur_spot = df_dict['Spot'].iloc[-1].to_dict()  # current spot rates of all tenors
    cur_fwd = df_dict['Fwd3m'].iloc[-1].to_dict()  # current forward rates of all tenors
    roll_down = list(df_roll_down.iloc[-1].values)
    spot_curve = SpotCurve(cur_spot, cur_fwd)
    tr = spot_curve.calc_total_return(header[1:],prd)  # calculate total return from current spot rates and current forward rates
    tr.insert(0, 0)  # special case: tenor is 3m
    carry = spot_curve.calc_carry(header[1:], prd)  # calculate carry from current spot rates and current forward rates
    carry.insert(0, -roll_down[0])  # special case: tenor is 3m
    z_score = []
    z_score_rd = []
    u = UtilityClass()
    for name in header:
        z_score.append(u.calc_z_score(list(df_dict['Spot'][name].values), False))
        z_score_rd.append(u.calc_z_score(list(df_roll_down[name].values), False))

    return roll_down, z_score_rd, carry, z_score, tr, df_roll_down


def write2DB(roll_down, z_score_rd, carry, z_score, tr):
    """write all input lists to Access DataBase"""
    header = ['3m', '6m', '1y', '2y', '3y', '5y', '10y'] # Tenor as Key
    sr="Tenor"
    crsr.execute("CREATE TABLE Results(%s varchar(15), rolldown double, zscore double, total_return double, rd_Z_score double, carry double, PRIMARY KEY(Tenor))" %sr)
    for i in range(len(header)):
        sql = "INSERT INTO Results VALUES(%r,%r,%r,%r,%r,%r )" % (
        header[i], roll_down[i], z_score[i], tr[i], z_score_rd[i], carry[i])
        crsr.execute(sql)


if __name__ == "__main__":
    conn_str = ('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; '
                'DBQ=C:\\Test.accdb;')

    #srcDB = 'C:\\Users\\luoying.li\\Documents\\DummySheet.accdb'
    #destDB = 'C:\\Users\\luoying.li\\Documents\\DummySheet_backup.accdb'

    #Repair_Compact_DB(srcDB, destDB) # uncomment to Repair and Compact Database
    [crsr,cnxn]=Build_Access_Connect(conn_str)
    df_dict=Tables2DF(crsr)
    [roll_down, z_score_rd, carry, z_score, tr,df]=analyse(df_dict)
    #write2DB(roll_down, z_score_rd, carry, z_score, tr) # uncomment to write results to DataBase
    
    print roll_down
    print z_score
    
    cnxn.commit()
    cnxn.close()