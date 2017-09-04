
import xlwings as xw
from YieldCurve import YieldCurve
import pandas as pd

@xw.func
@xw.arg('x', pd.DataFrame, index=True, header=True)
@xw.ret(expand='table')
def analyse11(x):
    current=x.iloc[0].to_dict()
    #x = {'3m': 1.3439, '6m': 1.393, '1y': 1.4588, '2y': 1.5749, '3y': 1.6678, '5y': 1.8314, '10y': 2.1435}
    #forward = {'3m': 1.4323, '6m': 1.4539, '1y': 1.5327, '2y': 1.632, '3y': 1.72, '5y': 1.8767, '10y': 2.1782}
    header = ['6m', '1y', '2y', '3y', '5y', '10y']
    r = ['3m', '3m', '3m', '3m', '3m', '3m']
    oj = YieldCurve(**current)
    rd = oj.calc_roll_down(header, r)
    return rd



