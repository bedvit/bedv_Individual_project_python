#!/usr/bin/python
import psycopg2
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta
# from win32com import client

DIR_SOURCE = os.path.join(os.path.dirname(__file__),'source','')# f'/home/de11an/bedv/project/source/'#
DIR_ARCHIVE = os.path.join(os.path.dirname(__file__),'archive','')#f'/home/de11an/bedv/project/archive/'#

def scd2(cursor_dwh, table, id_dwh, select_dwh, select_stg_dwh, select_tgt_dwh, where_dwh):
	#Загрузка в приемник "вставок" на источнике (формат SCD2).
	cursor_dwh.execute(	f"""
						insert into de11an.bedv_dwh_dim_{table}_hist({select_dwh}, effective_from, effective_to, deleted_flg)
						select 
							{select_stg_dwh}, 
							stg.update_dt,
							to_date( '9999-12-31', 'YYYY-MM-DD' ),
							'N'
						from de11an.bedv_stg_{table} stg
						left join de11an.bedv_dwh_dim_{table}_hist tgt
							on stg.{id_dwh} = tgt.{id_dwh}
							and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
							and tgt.deleted_flg = 'N'
						where tgt.{id_dwh} is null
						""")
	#conn_dwh.commit()
	#Обновление в приемнике "обновлений" на источнике (формат SCD2).
	cursor_dwh.execute(f"""
						update de11an.bedv_dwh_dim_{table}_hist
						set 
							effective_to = tmp.update_dt - interval '1 second'
						from (
							select 
								stg.{id_dwh}, 
								stg.update_dt
							from de11an.bedv_stg_{table} stg
							inner join de11an.bedv_dwh_dim_{table}_hist tgt
								on stg.{id_dwh} = tgt.{id_dwh}
								and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
								and tgt.deleted_flg = 'N'
							{where_dwh}
							) tmp
						where de11an.bedv_dwh_dim_{table}_hist.{id_dwh} = tmp.{id_dwh} 
							and de11an.bedv_dwh_dim_{table}_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) 
							and de11an.bedv_dwh_dim_{table}_hist.deleted_flg = 'N' 
						""")
	#conn_dwh.commit()
	cursor_dwh.execute(f"""
						insert into de11an.bedv_dwh_dim_{table}_hist({select_dwh}, effective_from, effective_to, deleted_flg)
						select 
							{select_stg_dwh}, 
							stg.update_dt,
							to_date( '9999-12-31', 'YYYY-MM-DD' ),
							'N'
						from de11an.bedv_stg_{table} stg
						inner join de11an.bedv_dwh_dim_{table}_hist tgt
						on stg.{id_dwh} = tgt.{id_dwh}
							and tgt.effective_to = stg.update_dt - interval '1 second'
							and tgt.deleted_flg = 'N'
						{where_dwh}
						""")

	#Удаление в приемнике удаленных в источнике записей (формат SCD2).
	cursor_dwh.execute(f"""
						insert into de11an.bedv_dwh_dim_{table}_hist({select_dwh}, effective_from, effective_to, deleted_flg)
						select 
							{select_tgt_dwh}, 
							now(),
							to_date( '9999-12-31', 'YYYY-MM-DD' ),
							'Y'
						from de11an.bedv_dwh_dim_{table}_hist tgt
						where tgt.{id_dwh} in (
							select tgt.{id_dwh}
							from de11an.bedv_dwh_dim_{table}_hist tgt
							left join de11an.bedv_stg_del_{table} stg
							on stg.{id_dwh} = tgt.{id_dwh}
							where stg.{id_dwh} is null
								and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
								and tgt.deleted_flg = 'N'
						)
						and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
						and tgt.deleted_flg = 'N'
						""")
	cursor_dwh.execute(f"""
						update de11an.bedv_dwh_dim_{table}_hist
						set 
							effective_to = now() - interval '1 second'
						where de11an.bedv_dwh_dim_{table}_hist.{id_dwh} in (
							select tgt.{id_dwh}
							from de11an.bedv_dwh_dim_{table}_hist tgt
							left join de11an.bedv_stg_del_{table} stg
							on stg.{id_dwh} = tgt.{id_dwh}
							where stg.{id_dwh} is null
								and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
								and tgt.deleted_flg = 'N'
						)
						and de11an.bedv_dwh_dim_{table}_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
						and de11an.bedv_dwh_dim_{table}_hist.deleted_flg = 'N'
						""")

	# update meta
	cursor_dwh.execute(f"""update de11an.bedv_meta
							set max_update_dt = coalesce(
								( select max( update_dt ) from de11an.bedv_stg_{table}  ),
								( select max_update_dt from de11an.bedv_meta
								  where schema_name='de11an' and table_name='bedv_stg_{table}' )
							)
							where schema_name='de11an' and table_name = 'bedv_stg_{table}'""")

def main():	
	#делаем коннект
	conn_src = psycopg2.connect(database = "bank",
							host =     "host",
							user =     "user",
							password = "password",
							port =     "5432")
	conn_dwh= psycopg2.connect(database = "edu",
							host =     "host",
							user =     "user",
							password = "password",
							port =     "5432")
	conn_src.autocommit = False
	conn_dwh.autocommit = False
	cursor_src = conn_src.cursor()
	cursor_dwh= conn_dwh.cursor()

	# Обрабатываем источник transactions
	#Получаем дату из меты - последние изменения
	cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_transactions'")
	last_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	src_file =  DIR_SOURCE + 'transactions_'+ last_date.strftime("%d%m%Y")+'.txt'
	arch_file =  DIR_ARCHIVE + 'transactions_'+ last_date.strftime("%d%m%Y")+'.txt.backup'
	#Находим и загружаем новые файлы (фильтруем по дате из меты), все, которые есть в папке
	while os.path.isfile(src_file):
		cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_transactions" )
		#Открываем файл (должен быть 1 файл за новую дату) и загружаем в stg_
		df = pd.read_table( f'{src_file}',sep=';', header=0, index_col=None )
		df['parsed_date'] = df['transaction_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date())
		#фильтруем по дате
		df = df[df['parsed_date'] == last_date.date()]
		#заменяем запятую на точку, для корректной загрузки чисел
		df['amt_fixed'] = df.amount.apply(lambda x: x.replace(',', '.'))
		df = df[['transaction_id','transaction_date','card_num','oper_type','amt_fixed','oper_result','terminal']]
		cursor_dwh.executemany( """ INSERT INTO de11an.bedv_stg_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
									VALUES( %s, %s, %s, %s, %s, %s , %s) """, df.values.tolist() )
		#Загрузка в приемник "вставок" на источнике (формат fact).
		cursor_dwh.execute("""insert into de11an.bedv_dwh_fact_transactions( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal )
								select trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal 
								from de11an.bedv_stg_transactions""")
		# update meta
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{last_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
								where schema_name='de11an' and table_name='bedv_stg_transactions'""")
		# переместить обработанные файлы
		shutil.move(src_file,arch_file)
		#сохраняем все изменения на сервере
		conn_dwh.commit()
		#формируем новую дату для файла
		last_date+=timedelta(days=1) 
		src_file =  DIR_SOURCE + 'transactions_'+ last_date.strftime("%d%m%Y")+'.txt'
		arch_file =  DIR_ARCHIVE + 'transactions_'+ last_date.strftime("%d%m%Y")+'.txt.backup'
	######################################################################################################

	#Обрабатываем источник passport_blacklist
	cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_passport_blacklist'")
	last_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	src_file =  DIR_SOURCE + 'passport_blacklist_'+ last_date.strftime("%d%m%Y")+'.xlsx'
	arch_file =  DIR_ARCHIVE + 'passport_blacklist_'+ last_date.strftime("%d%m%Y")+'.xlsx.backup'
	#Находим и загружаем новые файлы (фильтруем по дате из меты), все, которые есть в папке
	while os.path.isfile(src_file):
		cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_passport_blacklist" )
		#Открываем файл (должен быть 1 файл за новую дату) и загружаем в stg_
		df = pd.read_excel( f'{src_file}', sheet_name='blacklist', header=0, index_col=None )
		df['parsed_date'] = df['date'].apply(lambda x: x.to_pydatetime().date())
		#фильтруем по дате
		df = df[df['parsed_date'] == last_date.date()]
		df = df[['passport','date']]
		cursor_dwh.executemany( """ INSERT INTO de11an.bedv_stg_passport_blacklist(passport_num, entry_dt) 
									VALUES( %s, %s) """, df.values.tolist() )
		#Загрузка в приемник "вставок" на источнике (формат fact).
		cursor_dwh.execute("""insert into de11an.bedv_dwh_fact_passport_blacklist( passport_num, entry_dt)
								select passport_num, entry_dt
								from de11an.bedv_stg_passport_blacklist""")
		# update meta
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{last_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
								where schema_name='de11an' and table_name='bedv_stg_passport_blacklist'""")
		# переместить обработанные файлы
		shutil.move(src_file,arch_file)
		#сохраняем все изменения на сервере
		conn_dwh.commit()
		#формируем новую дату для файла
		last_date+=timedelta(days=1) 
		src_file =  DIR_SOURCE + 'passport_blacklist_'+ last_date.strftime("%d%m%Y")+'.xlsx'
		arch_file =  DIR_ARCHIVE + 'passport_blacklist_'+ last_date.strftime("%d%m%Y")+'.xlsx.backup'
	######################################################################################################

	#Обрабатываем SCD2
	# имя таблицы, ключ, другие поля необходимые для кодогенерации
	tbl_src=(('terminals','terminal_id','terminal_type', 'terminal_city','terminal_address'),
			 ('cards','card_num','account'),
			 ('accounts','account','valid_to','client'),
			 ('clients','client_id','last_name','first_name','patronymic','date_of_birth','passport_num','passport_valid_to','phone')
			)
	tbl_dwh=(('terminals','terminal_id','terminal_type', 'terminal_city','terminal_address'),
			 ('cards','card_num','account_num'),
			 ('accounts','account_num','valid_to','client'),
			 ('clients','client_id','last_name','first_name','patronymic','date_of_birth','passport_num','passport_valid_to','phone')
			)

	for i in range(len(tbl_src)):
		#формирование SQL-вставок
		table=tbl_dwh[i][0]#имена таблиц одинаковые в источнике и таргете
		id_src=tbl_src[i][1]
		id_dwh=tbl_dwh[i][1]
		select_src = ', '.join(tbl_src[i][1:])
		select_dwh = ', '.join(tbl_dwh[i][1:])
		select_stg_dwh ='stg.' + ', stg.'.join(tbl_dwh[i][1:])
		select_tgt_dwh ='tgt.' + ', tgt.'.join(tbl_dwh[i][1:])
		where_dwh = f'where 1=0'
		for p in tbl_dwh[i][1:]:
			where_dwh += f' or stg.{p} <> tgt.{p} or (stg.{p} is null and tgt.{p} is not null) or (stg.{p} is not null and tgt.{p} is null)'
		#формирование SQL-вставок

		#ищем нужную дату
		cursor_dwh.execute(f"select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_{table}'")
		last_date=cursor_dwh.fetchone()[0]

		if i==0:
			#Обрабатываем источник в файле xlsx - terminals
			last_date+=timedelta(days=1) 
			src_file =  DIR_SOURCE + f'{table}_'+ last_date.strftime("%d%m%Y")+'.xlsx'
			arch_file =  DIR_ARCHIVE + f'{table}_'+ last_date.strftime("%d%m%Y")+'.xlsx.backup'
			while os.path.isfile(src_file):
				cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_{table}" )
				cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_del_{table}" )
				df = pd.read_excel( f'{src_file}', sheet_name=f'{table}', header=0, index_col=None )
				df['update_dt'] = last_date.strftime('%Y-%m-%d')
				df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]
				cursor_dwh.executemany(	f"""
										INSERT INTO de11an.bedv_stg_{table}(
											{select_dwh},
											update_dt 
										) VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() )

				#Загружаем список id в stg_terminals_del
				df = df[[f'{id_dwh}']]
				cursor_dwh.executemany( f"INSERT INTO de11an.bedv_stg_del_{table}({id_dwh}) VALUES(%s) ", df.values.tolist() )

				#Инкрементальная загрузка(scd2)
				scd2(cursor_dwh, table, id_dwh, select_dwh, select_stg_dwh, select_tgt_dwh, where_dwh)

				# переместить обработанные файлы
				shutil.move(src_file,arch_file)
				#сохраняем все изменения на сервере
				conn_dwh.commit()
				#формируем новую дату для файла
				last_date+=timedelta(days=1) 
				src_file =  DIR_SOURCE + f'{table}_'+ last_date.strftime("%d%m%Y")+'.xlsx'
				arch_file =  DIR_ARCHIVE + f'{table}_'+ last_date.strftime("%d%m%Y")+'.xlsx.backup'
			continue

		#SQL-сервер
		#Очистка стейджинговых таблиц
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_{table}" )
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_del_{table}" )

		#Захват данных из источника (измененных с момента последней загрузки) в стейджинг
		cursor_src.execute( f"""
							select {select_src},  coalesce(update_dt, create_dt) as update_dt from info.{table}
							where coalesce(update_dt, create_dt) > to_date('{last_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
							""")
		for row in cursor_src:
			cursor_dwh.execute( f"""INSERT INTO de11an.bedv_stg_{table}({select_dwh}, update_dt) VALUES ({'%s'+',%s'*(len(tbl_dwh[i])-1)})""", row) #заливаем данные построчно, экономия памяти, не упадет при больших таблицах
		#cursor_dwh.executemany( f"""INSERT INTO de11an.bedv_stg_{table}({select_dwh}, update_dt) VALUES ({'%s'+',%s'*(len(tbl_dwh[i])-1)})""", cursor_src.fetchall()) #заливаем массивом, при больших таблицах может не хватить памяти
		cursor_dwh.execute( f"""update de11an.bedv_stg_{table} set {id_dwh} = trim({id_dwh})""")
		#Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
		cursor_src.execute( f"SELECT trim({id_src}) FROM info.{table}" )
		cursor_dwh.executemany( f"""INSERT INTO de11an.bedv_stg_del_{table}({id_dwh}) VALUES (%s)""", cursor_src.fetchall())

		#Инкрементальная загрузка(scd2)
		scd2(cursor_dwh, table, id_dwh, select_dwh, select_stg_dwh, select_tgt_dwh, where_dwh)
		
		#сохраняем все изменения на сервере
		conn_dwh.commit()
	######################################################################################################


	#Формируем отчет по мошенническим операциям
	#1. Совершение операции при просроченном или заблокированном паспорте.
	cursor_dwh.execute( f"""
						insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
						select distinct
							tr.trans_date as event_dt, 
							cli.passport_num as passport, 
							(cli.last_name || ' ' || cli.first_name || ' ' || cli.patronymic) as fio, 
							cli.phone as phone,
							1 as event_type,
							tr.trans_date::date as report_dt
						from de11an.bedv_dwh_fact_transactions tr
						inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
						inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
						inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
						left join de11an.bedv_dwh_fact_passport_blacklist pbl on pbl.passport_num = cli.passport_num
						where (tr.trans_date::date > coalesce(cli.passport_valid_to, now())	or tr.trans_date>pbl.entry_dt)
							and tr.trans_date::date> coalesce((select max(report_dt) from de11an.bedv_rep_fraud where event_type=1), tr.trans_date::date-1)
						""")
	#2. Совершение операции при недействующем договоре.
	cursor_dwh.execute( f"""
						insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
						select distinct
							tr.trans_date as event_dt, 
							cli.passport_num as passport, 
							(cli.last_name || ' ' || cli.first_name || ' ' || cli.patronymic) as fio, 
							cli.phone as phone,
							2 as event_type,
							tr.trans_date::date as report_dt
						from de11an.bedv_dwh_fact_transactions tr
						inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
						inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
						inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
						where tr.trans_date::date > acc.valid_to 
							and tr.trans_date::date> coalesce((select max(report_dt) from de11an.bedv_rep_fraud where event_type=2), tr.trans_date::date-1)
						""")
		######################################################################################################
	conn_dwh.commit()
	cursor_src.close()
	cursor_dwh.close()
	conn_src.close()
	conn_dwh.close()



def bedvitCOM():
	"""Поделючаем под пользователем COM.DLL из корневой папки"""
	#регистрируем под пользователем: DllInstall(1) BedvitCOM-DLL - разово
	import ctypes
	bCOM = ctypes.WinDLL('BedvitCOM64.dll')
	bCOM.DllInstall(1) #0-unregister, return==0 - OK
	# использование BedvitCOM-DLL
	from win32com import client
	print(client.DispatchEx('BedvitCOM.VBA').Version())
	print(client.DispatchEx('BedvitCOM.VBA').FileName())
	bCOMi = client.DispatchEx('BedvitCOM.BignumArithmeticInteger')
	bCOMi.Factorial (0, 100)
	print(f'Factorial 100: '+bCOMi.Bignum(0))

def graf():
	"""вывод графика"""
	#вывод графика
	import numpy as np
	import pandas as pd
	import matplotlib.pyplot as plt
	plt.close("all")
	ts = pd.Series(np.random.randn(1000), index=pd.date_range("1/1/2000", periods=1000))
	ts = ts.cumsum()
	#ts.plot()
	#plt.show()
	df = pd.DataFrame(np.random.randn(1000, 4), index=ts.index, columns=["A", "B", "C", "D"])
	df = df.cumsum()
	#plt.figure()
	df.plot()
	plt.legend(loc='best')
	plt.show()

def form():
	"""вывод формы"""
	# вывод формы
	from PyQt5 import QtWidgets
	import sys
	app = QtWidgets.QApplication(sys.argv)
	window = QtWidgets.QWidget()
	window.setWindowTitle("Hello World!")
	window.resize(300,700)
	label = QtWidgets.QLabel("<center>Hello World!</*center>")
	btnQuit = QtWidgets.QPushButton("&Закрыть окно")
	vbox = QtWidgets.QVBoxLayout()
	vbox.addWidget(label)
	vbox.addWidget(btnQuit)
	window.setLayout(vbox)
	btnQuit.clicked.connect(app.quit)
	window.show()
	sys.exit(app.exec_())


# использование С-DLL
# import ctypes
# User32 = ctypes.WinDLL('User32.dll')
# x=User32.GetSystemMetrics(1)
# print(x)
# # bCOM = CreateObject("BedvitCOM.BignumArithmeticFloat")
# # bCOM.Help

# import comtypes.client
# bCOM = comtypes.client.CreateObject('BedvitCOM.BignumArithmeticFloat')
# bCOM.Help

# испоьзование СОМ-DLL
# from win32com import client
# app = client.DispatchEx("Excel.Application")
# app.Interactive = True
# app.Visible = True
# input_file = r'Test.xlsb'
# Workbook = app.Workbooks.Open(input_file)



# import os, os.path


def ExcelApplicationRun():
	# from win32com import client
	# # if os.path.exists("excelsheet.xlsm"):
	# xl=client.Dispatch("Excel.Application")
	from win32com import client
	app = client.DispatchEx("Excel.Application")
	# app.Interactive = True
	app.Visible = True

	input_xll = r'C:\Users\vit\AppData\Roaming\Microsoft\AddIns\BedvitXLL64.xll' 
	input_file = r'C:\Users\vit\Documents\Visual Studio 2022\BedvitDLL\Tests\Test.xlsb'

	app.RegisterXLL(input_xll) #xl.RegisterXLL('C:/path/addin.xll')
	# Workbook = app.Workbooks.Add

	Workbook = app.Workbooks.Open(input_file)

	# for xla, xlam add-in
	# # # Need to load the addins before opening the workbook
	# # addin_path = r'C:\path\addin.xla'
	# # xl.Workbooks.Open(addin_path)
	# # xl.AddIns.Add(addin_path).Installed = True

	# Workbook.visible = True
	# import pythoncom
	# pythoncom.PumpWaitingMessages()
	# import time
	# time.sleep(1)
	app.Application.Run("XLLcmdSetSheetFormat","1")
	# app.Application.Run("m_VBARun.test") #app.Application.Run("XLLcmdSetSheetFormat","1")
	#xl.Application.Save() # if you want to save then uncomment this line and change delete the ", ReadOnly=1" part from the open function.
	app.Application.Quit() # Comment this out if your excel script closes
	del app


if __name__ == '__main__':
	#main()
	#bedvitCOM()
	# graf()
	#form()
	ExcelApplicationRun()