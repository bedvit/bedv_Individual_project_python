#from pickle import FALSE
import psycopg2
import pandas as pd

def main():	
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
	#регистрируем под пользователем: DllInstall(1) BedvitCOM-DLL
	import ctypes
	bCOM = ctypes.WinDLL('BedvitCOM64.dll')
	res=bCOM.DllInstall(1) #0-unregister, return==0 - OK
	# использование BedvitCOM-DLL
	from win32com import client
	print(client.DispatchEx('BedvitCOM.VBA').Version())
	bCOMvba = client.DispatchEx('BedvitCOM.VBA')
	print(bCOMvba.FileName())

	#bCOM = client.DispatchEx('BedvitCOM.BignumArithmeticInteger')
	#bCOM.Factorial (0, 1024)
	#bCOM = client.DispatchEx('BedvitCOM.VBA')


if __name__ == '__main__':
    #main()
	#print(f3.__doc__)
	f3()




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
