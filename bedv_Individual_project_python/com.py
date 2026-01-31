# #!/usr/bin/python
# import psycopg2
# import pandas as pd
# import os
# import shutil
# from datetime import datetime, timedelta

# использование С-DLL
def c_dll():
	import ctypes
	User32 = ctypes.WinDLL('User32.dll')
	x = User32.GetSystemMetrics(1)
	print(x)
	# bCOM = CreateObject("BedvitCOM.BignumArithmeticFloat")
	# bCOM.Help

def BedvitCOM_BignumArithmeticFloat():
	import comtypes.client
	# from comtypes import client
	bCOM = comtypes.client.CreateObject('BedvitCOM.BignumArithmeticFloat')
	bCOM.Help

def Excel_Application():
	# использование СОМ-DLL
	from win32com import client
	app = client.DispatchEx("Excel.Application")
	app.Interactive = True
	app.Visible = True
	input_file = r'Test.xlsb'
	Workbook = app.Workbooks.Open(input_file)

def bedvitCOM_Factorial():
	"""Поделючаем под пользователем COM.DLL из корневой папки"""
	# #регистрируем под пользователем: DllInstall(1) BedvitCOM-DLL - разово
	# import ctypes
	# bCOM = ctypes.WinDLL('BedvitCOM64.dll')
	# bCOM.DllInstall(1) #0-unregister, return==0 - OK
	# # использование BedvitCOM-DLL

	from win32com import client
	print(client.DispatchEx('BedvitCOM.VBA').Version())
	print(client.DispatchEx('BedvitCOM.VBA').FileName())
	bCOMi = client.DispatchEx('BedvitCOM.BignumArithmeticInteger')
	bCOMi.Factorial (0, 100)
	print(f'Factorial 100: '+bCOMi.Bignum(0))
 
def XLLcmdDataLoadFromExcelSheets():
	from win32com import client
	input_xll = r'C:\Users\...\AppData\Roaming\Microsoft\AddIns\BedvitXLL64.xll'
	input_file = r'C:\Users\...\Documents\Test.xlsb'

	app = client.DispatchEx("Excel.Application")
	app.Visible = True
	app.RegisterXLL(input_xll)
	workbook = app.Workbooks.Open(input_file)
	app.Application.Run("XLLcmdDataLoadFromExcelSheets", "1", "Имя моего сохранения")
	app.Application.Quit()

	del app


def bedvitCOM_File():
	"""Поделючаем под пользователем COM.DLL из корневой папки"""
	# #регистрируем под пользователем: DllInstall(1) BedvitCOM-DLL - разово
	# import ctypes
	# bCOM = ctypes.WinDLL('BedvitCOM64.dll')
	# bCOM.DllInstall(1) #0-unregister, return==0 - OK
	# # использование BedvitCOM-DLL

	from win32com import client

	varFromFile=['', 5, [4, 5, 6]]
	# bCOMi = client.DispatchEx('BedvitCOM.VBA')
	# bCOMi = client.DispatchEx('BedvitCOM.Сollection')
	bCOMi = client.DispatchEx('BedvitCOM.Functions')
	# bCOMi = client.DispatchEx('BedvitCOM.Methods')

	res = bCOMi.VariantToFile (varFromFile)
	res = bCOMi.VariantFromFile (varFromFile)
	print(res[1][0])


if __name__ == '__main__':
	#num1=round(3.5)
	#num2=round(4.5)
	#print(num1,num2)
	#main()
	bedvitCOM_File()
	# print('Hello')
