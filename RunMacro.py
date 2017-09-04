import win32com.client
import os

if os.path.exists("C:\\Users\\luoying.li\\Downloads\\InternTest_Li Luoying.xlsm"):
    xl = win32com.client.Dispatch("Excel.Application")
    xl.Application.Visible = True
    wb=xl.Workbooks.Open(Filename="C:\\Users\\luoying.li\\Downloads\\InternTest_Li Luoying.xlsm")
    xl.Application.Run("Update")
    wb.Save()
    wb.Close(SaveChanges=True)
    del xl
