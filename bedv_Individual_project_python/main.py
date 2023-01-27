#from pickle import FALSE
import psycopg2
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta

DIR_SOURCE = 'source/'
DIR_ARCHIVE = 'archive/'

def main():	
	#делаем коннект
	conn_src = psycopg2.connect(database = "bank",
							host =     "de-edu-db.chronosavant.ru",
							user =     "bank_etl",
							password = "bank_etl_password",
							port =     "5432")
	conn_dwh= psycopg2.connect(database = "edu",
							host =     "de-edu-db.chronosavant.ru",
							user =     "de11an",
							password = "peregrintook",
							port =     "5432")
	conn_src.autocommit = False
	conn_dwh.autocommit = False
	cursor_src = conn_src.cursor()
	cursor_dwh= conn_dwh.cursor()

	##1. Очистка стейджинговых таблиц
	##Очистим весь стейджинг 
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_transactions" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_passport_blacklist" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_terminals" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_cards" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_accounts" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_clients" )

	##Очистим весь stg_del 
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_del_terminals" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_del_cards" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_del_accounts" )
	#cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_del_clients" )


	#загружаем фактические данные из файлов dwh_fact

	# stg_terminals
	# 1. Получаем дату из меты - последние изменения
	# 2. Находим новый файл (фильтруем по дате из меты)
	# 3. Открываем файл (должен быть 1 файл за новую дату) и загружаем в stg_terminals
	# 4. Загружаем список id в stg_terminals_del
	# 5. SCD2 загрузка из stg:
	# 5.1. insert from stg_terminals to dim_terminals
	#   добавление новых записей
	# 5.2. (update, insert) from stg_terminals to dim_terminals
	#   изменение старых (открытие новой версии, закрытие старой версии)
	# 5.3. (update, insert) from stg_terminals_del -> dim_terminals
	#   логическое удаление (открытие новой версии с deleted_flg = 1, закрытие старой версии)
	# 6. Обновить meta

	cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_dwh_dim_terminals_hist'")
	find_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	terminals_file = DIR_SOURCE + 'terminals_'+ find_date.strftime("%d%m%Y")+'.xlsx'#while 'terminals_'DDMMYYYY.xlsx:

	#загрузка всех файлов из папки по очередности: +1 к дате в META
	while os.path.isfile(terminals_file):
		print(terminals_file)
		df = pd.read_excel( f'{terminals_file}', sheet_name='terminals', header=0, index_col=None )
		df['update_dt'] = find_date.strftime('%Y-%m-%d')
		df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]
		cursor_dwh.executemany( """ INSERT INTO de11an.bedv_stg_terminals(
										terminal_id,
										terminal_type,
										terminal_city,
										terminal_address,
										update_dt 
									) VALUES( %s, %s, %s, %s, %s ) """, df.values.tolist() )

		cursor_dwh.executemany( "INSERT INTO de11an.bedv_stg_del_terminals(terminal_id) VALUES(%s)", df['terminal_id'].values.tolist())

		### Загрузка dim
		# dim_terminals
		# SCD2 ->
		#cursor_dwh.execute( """ insert into dwh.dim_terminals (
		#						)""")
		## ...

		# update meta
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
									where schema_name='de11an' and source_name='bedv_dwh_dim_terminals_hist'""")
		conn_dwh.commit()
		#явно берем из META, вдруг не прогрузилось (сбой в сети и т.д.), ранее было #find_date+=timedelta(days=1) 
		cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_dwh_dim_terminals_hist'")

		#перемещаем файл
		tmp=f'{DIR_ARCHIVE}{terminals_file}'+'.backup'
		# переместить обработанные файлы
		shutil.move(
			f'{DIR_SOURCE}{terminals_file}',
			f'{DIR_ARCHIVE}{terminals_file}'+'.backup'
		)


		find_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
		terminals_file = DIR_SOURCE + 'terminals_'+ find_date.strftime("%d%m%Y")+'.xlsx'#while 'terminals_'DDMMYYYY.xlsx:

	######################################################################################################

  
	cursor_src.close()
	cursor_dwh.close()
	conn_src.close()
	conn_dwh.close()








	#2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг

	#3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.

	#4. Загрузка в приемник "вставок" на источнике (формат SCD2).

	#5. Обновление в приемнике "обновлений" на источнике (формат SCD2).

	#6. Удаление в приемнике удаленных в источнике записей (формат SCD2).

	#7. Обновление метаданных.

	#8. Фиксация транзакции.

	#* Напишите скрипт, соединяющий нужные таблицы для поиска операций, совершенных при недействующем договоре (это самый простой случай мошенничества). Отладьте ваш скрипт для одной даты в DBeaver, он должен выдавать результат. В простейшем варианте допустимо использовать «хардкод» для задания дня отчета.
	#• Результат выполнения скрипта загружайте в таблицу xxxx_rep_fraud. Не забывайте сформировать поле report_dt.
	#• Зафиксируйте изменения. Отключитесь от баз.
	#• Переименуйте обработанные файлы и перенесите их в другой каталог.
	#* Заполните файл main.cron

	#cursor_src.execute( "SELECT card_num, account, create_dt, update_dt FROM info.cards" )
	#for row in cursor_dwh:
	#	cursor_dwh.execute( """INSERT INTO de11an.bedv_stg_cards(card_num,account_num,create_dt,update_dt) VALUES (%s,%s,%s,%s)""", row) #построчно
	#conn_dwh.commit()

	#cursor_src.close()
	#cursor_dwh.close()
	#conn_src.close()
	#conn_dwh.close()

	#select max(coalesce(update_dt, create_dt)) from info.clients;
	#select create_dt > max_update_dt or update_dt > max_update_dt from info.clients;
	
	#переносим файлы в архив
	#os.rename(r'sourse/transactions_01032021.txt', r'archive/transactions_01032021.txt.backup') 
	#https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_varchar.28n.29_by_default

def f0():	
	conn = psycopg2.connect(database = "edu",
							host =     "de-edu-db.chronosavant.ru",
							user =     "de11an",
							password = "peregrintook",
							port =     "5432")
	
	conn.autocommit = False
	cursor = conn.cursor()

	cursor.execute("CREATE TABLE IF NOT EXISTS de11an.bedv_med_results (telephone text, patient_name text, analysis_name text, conclusion text )" )
	cursor.execute("TRUNCATE TABLE de11an.bedv_med_results" )
	conn.commit()

	cursor.execute( "SELECT * FROM de.med_an_name" )
	records2 = cursor.fetchall()

	cursor.execute( "SELECT * FROM de.med_name" )
	records3 = cursor.fetchall()

	# Формирование DataFrame вариант: easy
	df1 = pd.read_excel( r'medicine.xlsx', sheet_name='easy', header=0, index_col=None )
	records1=df1.values.tolist()
	records = []
	for row1 in records1:
		for row2 in records2:
			if row1[1]==row2[0] and (row1[2]<row2[3] or row1[2]>row2[4]):
				for row3 in records3:
					if row1[0]==row3[0]:
						records.append([row3[2],row3[1],row2[1],'Понижен' if row1[2]<row2[3] else 'Повышен'])


	df = pd.DataFrame(records)
	df.to_excel( 'ResultEasy.xlsx', sheet_name='easy', header=False, index=False )
	cursor.executemany( """INSERT INTO de11an.bedv_med_results(telephone, patient_name,analysis_name,conclusion) VALUES (%s,%s,%s,%s)""", df.values.tolist())
	conn.commit()

	# Формирование DataFrame вариант: hard
	cursor.execute("TRUNCATE TABLE de11an.bedv_med_results" )
	records.clear()
	records4 = []
	df1 = pd.read_excel( r'medicine.xlsx', sheet_name='hard', header=0, index_col=None )
	records1=df1.values.tolist()

	for row1 in records1:

		if type(row1[2])==str and row1[2].replace('.', '').isdigit():
			row1[2]=float(row1[2])

		if type(row1[2])==str:
			if row1[2]=='+' or 'Пол' in row1[2]:
				row1[2]=1

		for row2 in records2:
			if ((type(row1[2])==float or type(row1[2])==int) and row1[1]==row2[0] and (row1[2]==1 or row1[2]<row2[3] or row1[2]>row2[4])):
				for row3 in records3:
					if row1[0]==row3[0]:
						records4.append([row3[2],row3[1],row2[1],'Положительный' if row1[2]==1 else ('Понижен' if row1[2]<row2[3] else 'Повышен')])


	records4.sort()
	i=0
	while i<len(records4)-1:
		if records4[i][0]==records4[i+1][0]:
			records.append(records4[i])
			records.append(records4[i+1])
			i+=2
		i+=1
	df = pd.DataFrame(records)
	df.to_excel( 'ResultHard.xlsx', sheet_name='hard', header=False, index=False )
	cursor.executemany( """INSERT INTO de11an.bedv_med_results(telephone, patient_name,analysis_name,conclusion) VALUES (%s,%s,%s,%s)""", df.values.tolist())
	conn.commit()

	cursor.close()
	conn.close()



def f1():
	conn1 = psycopg2.connect(database = "edu",
							host =     "de-edu-db.chronosavant.ru",
							user =     "de11an",
							password = "peregrintook",
							port =     "5432")
	conn2= psycopg2.connect(database = "edu",
							host =     "de-edu-db.chronosavant.ru",
							user =     "de11an",
							password = "peregrintook",
							port =     "5432")
  
	conn1.autocommit = False
	conn2.autocommit = False
	cursor1 = conn1.cursor()
	cursor2 = conn2.cursor()

	cursor1.execute( "SELECT * FROM de11an.bedv_source" )
	#cursor2.executemany( """INSERT INTO de11an.bedv_source2 VALUES (%s,%s,%s)""", cursor1.fetchall()) #первый способ через список
	for result in cursor1:
		cursor2.execute( """INSERT INTO de11an.bedv_source2 VALUES (%s,%s,%s)""", result) #второй список построчно, экономия памяти, проверить на скорость
	#conn2.commit()

	cursor1.close()
	cursor2.close()
	conn1.close()
	conn2.close()

def f2():
	# получим объект файла
	file1 = open(r"...txt", "r")
	while True:
		# считываем строку
		line = file1.readline()
		# прерываем цикл, если строка пустая
		if not line:
			break
		# выводим строку
		print(line.strip())
	# закрываем файл
	file1.close


def f3():
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
	print(bCOMi.Bignum(0))







if __name__ == '__main__':
    #main()
	#print(f3.__doc__)
	#f3()
	main()




#import psycopg2
#import pandas as pd

#def main():	
#	conn = psycopg2.connect(database = "edu",
#							host =     "de-edu-db.chronosavant.ru",
#							user =     "de11an",
#							password = "peregrintook",
#							port =     "5432")
	
#	conn.autocommit = False
#	cursor = conn.cursor()

#	cursor.execute("CREATE TABLE IF NOT EXISTS de11an.bedv_med_results (telephone text, patient_name text, analysis_name text, conclusion text )" )
#	cursor.execute("TRUNCATE TABLE de11an.bedv_med_results" )
#	conn.commit()

#	cursor.execute( "SELECT * FROM de.med_an_name" )
#	records2 = cursor.fetchall()

#	cursor.execute( "SELECT * FROM de.med_name" )
#	records3 = cursor.fetchall()

#	# Формирование DataFrame вариант: easy
#	df1 = pd.read_excel( r'medicine.xlsx', sheet_name='easy', header=0, index_col=None )
#	records1=df1.values.tolist()
#	records = []
#	for row1 in records1:
#		for row2 in records2:
#			if row1[1]==row2[0] and (row1[2]<row2[3] or row1[2]>row2[4]):
#				for row3 in records3:
#					if row1[0]==row3[0]:
#						records.append([row3[2],row3[1],row2[1],'Понижен' if row1[2]<row2[3] else 'Повышен'])


#	df = pd.DataFrame(records)
#	df.to_excel( 'ResultEasy.xlsx', sheet_name='easy', header=False, index=False )
#	cursor.executemany( """INSERT INTO de11an.bedv_med_results(telephone, patient_name,analysis_name,conclusion) VALUES (%s,%s,%s,%s)""", df.values.tolist())
#	conn.commit()

#	# Формирование DataFrame вариант: hard
#	cursor.execute("TRUNCATE TABLE de11an.bedv_med_results" )
#	records.clear()
#	records4 = []
#	df1 = pd.read_excel( r'medicine.xlsx', sheet_name='hard', header=0, index_col=None )
#	records1=df1.values.tolist()

#	for row1 in records1:

#		if type(row1[2])==str and row1[2].replace('.', '').isdigit():
#			row1[2]=float(row1[2])

#		if type(row1[2])==str:
#			if row1[2]=='+' or 'Пол' in row1[2]:
#				row1[2]=1

#		for row2 in records2:
#			if ((type(row1[2])==float or type(row1[2])==int) and row1[1]==row2[0] and (row1[2]==1 or row1[2]<row2[3] or row1[2]>row2[4])):
#				for row3 in records3:
#					if row1[0]==row3[0]:
#						records4.append([row3[2],row3[1],row2[1],'Положительный' if row1[2]==1 else ('Понижен' if row1[2]<row2[3] else 'Повышен')])


#	records4.sort()
#	i=0
#	while i<len(records4)-1:
#		if records4[i][0]==records4[i+1][0]:
#			records.append(records4[i])
#			records.append(records4[i+1])
#			i+=2
#		i+=1
#	df = pd.DataFrame(records)
#	df.to_excel( 'ResultHard.xlsx', sheet_name='hard', header=False, index=False )
#	cursor.executemany( """INSERT INTO de11an.bedv_med_results(telephone, patient_name,analysis_name,conclusion) VALUES (%s,%s,%s,%s)""", df.values.tolist())
#	conn.commit()

#	cursor.close()
#	conn.close()

#if __name__ == '__main__':
#    main()


#тренировочный модуль
	#def main():	
	#conn = psycopg2.connect(database = "edu",
	#						host =     "de-edu-db.chronosavant.ru",
	#						user =     "de11an",
	#						password = "peregrintook",
	#						port =     "5432")
	
	#conn.autocommit = False
	#cursor = conn.cursor()

	## Удаление таблицы - отладка
	##cursor.execute( "DROP TABLE de11an.bedv_med_results" )
	## Создание таблицы
	#cursor.execute("CREATE TABLE IF NOT EXISTS de11an.bedv_med_results (telephone text, patient_name text, analysis_name text, conclusion text )" )
	#cursor.execute("TRUNCATE TABLE de11an.bedv_med_results" )
	##try:
	##	cursor.execute("CREATE TABLE de11an.bedv_med_results (telephone text, patient_name text, analysis_name text, conclusion text )" )
	##except Exception:
		

	##сохраняем коннект (изменения)
	#conn.commit()

	## Выполнение SQL кода в базе данных с возвратом результата
	#cursor.execute( "SELECT * FROM de.med_an_name" )
	#records2 = cursor.fetchall()
	#for row in records2:
	#	print( row )
	##names = [ x[0] for x in cursor.description ]
	##df2 = pd.DataFrame( records2, columns = names )# Формирование DataFrame

	#cursor.execute( "SELECT * FROM de.med_name" )
	#records3 = cursor.fetchall()
	#for row in records3:
	#	print( row )
	##names = [ x[0] for x in cursor.description ]
	##df3 = pd.DataFrame( records3, columns = names )# Формирование DataFrame

	## Чтение из файла
	#df2 = pd.read_excel( r'medicine.xlsx', sheet_name='easy', header=0, index_col=None )
	#records1=df2.values.tolist()
	#print(records1)

	## Формирование DataFrame - в итоги
	#records = [['0','0','0','0']]

	## запись list в DataFrame
	#df = pd.DataFrame(records)
	## Запись в файл
	#df.to_excel( 'result.xlsx', sheet_name='sheet1', header=False, index=False )

	## Выполнение SQL кода вставки в базу данных
	#cursor.executemany( """INSERT INTO de11an.bedv_med_results(telephone, patient_name,analysis_name,conclusion) VALUES (%s,%s,%s,%s)""", df.values.tolist())#df.values.tolist()
	##cursor.execute( "INSERT INTO de11an.bedv_testtable( id, val ) VALUES ( 1, 'ABC' )" )
	##сохраняем коннект (изменения)
	#conn.commit()

	##смотрим что записали
	#cursor.execute( "SELECT * FROM de11an.bedv_med_results" )#
	#records = cursor.fetchall()
	#for row in records:
	#	print( row )

	## Закрываем соединение
	#cursor.close()
	#conn.close()
