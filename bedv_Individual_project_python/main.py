#from pickle import FALSE
import psycopg2
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta


DIR_SOURCE = os.path.join(os.path.dirname(__file__),'source','')
DIR_ARCHIVE = os.path.join(os.path.dirname(__file__),'archive','')

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

	# Обрабатываем источник transactions
	#Получаем дату из меты - последние изменения
	cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_transactions'")
	find_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	src_file =  DIR_SOURCE + 'transactions_'+ find_date.strftime("%d%m%Y")+'.txt'
	arch_file =  DIR_ARCHIVE + 'transactions_'+ find_date.strftime("%d%m%Y")+'.txt.backup'
	#Находим и загружаем новые файлы (фильтруем по дате из меты), все, которые есть в папке
	while os.path.isfile(src_file):
		cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_transactions" )
		#Открываем файл (должен быть 1 файл за новую дату) и загружаем в stg_
		df = pd.read_table( f'{src_file}',sep=';', header=0, index_col=None )
		df['parsed_date'] = df['transaction_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date())
		#фильтруем по дате
		df = df[df['parsed_date'] == find_date.date()]
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
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
								where schema_name='de11an' and table_name='bedv_stg_transactions'""")
		# переместить обработанные файлы
		shutil.move(src_file,arch_file)
		#сохраняем все изменения на сервере
		conn_dwh.commit()
		#формируем новую дату для файла
		find_date+=timedelta(days=1) 
		src_file =  DIR_SOURCE + 'transactions_'+ find_date.strftime("%d%m%Y")+'.txt'
		arch_file =  DIR_ARCHIVE + 'transactions_'+ find_date.strftime("%d%m%Y")+'.txt.backup'
	######################################################################################################

	#Обрабатываем источник passport_blacklist
	cursor_dwh.execute("select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_passport_blacklist'")
	find_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	src_file =  DIR_SOURCE + 'passport_blacklist_'+ find_date.strftime("%d%m%Y")+'.xlsx'
	arch_file =  DIR_ARCHIVE + 'passport_blacklist_'+ find_date.strftime("%d%m%Y")+'.xlsx.backup'
	#Находим и загружаем новые файлы (фильтруем по дате из меты), все, которые есть в папке
	while os.path.isfile(src_file):
		cursor_dwh.execute( "DELETE FROM de11an.bedv_stg_passport_blacklist" )
		#Открываем файл (должен быть 1 файл за новую дату) и загружаем в stg_
		df = pd.read_excel( f'{src_file}', sheet_name='blacklist', header=0, index_col=None )
		df['parsed_date'] = df['date'].apply(lambda x: x.to_pydatetime().date())
		#фильтруем по дате
		df = df[df['parsed_date'] == find_date.date()]
		df = df[['passport','date']]
		cursor_dwh.executemany( """ INSERT INTO de11an.bedv_stg_passport_blacklist(passport_num, entry_dt) 
									VALUES( %s, %s) """, df.values.tolist() )
		#Загрузка в приемник "вставок" на источнике (формат fact).
		cursor_dwh.execute("""insert into de11an.bedv_dwh_fact_passport_blacklist( passport_num, entry_dt)
								select passport_num, entry_dt
								from de11an.bedv_stg_passport_blacklist""")
		# update meta
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
								where schema_name='de11an' and table_name='bedv_stg_passport_blacklist'""")
		# переместить обработанные файлы
		shutil.move(src_file,arch_file)
		#сохраняем все изменения на сервере
		conn_dwh.commit()
		#формируем новую дату для файла
		find_date+=timedelta(days=1) 
		src_file =  DIR_SOURCE + 'passport_blacklist_'+ find_date.strftime("%d%m%Y")+'.xlsx'
		arch_file =  DIR_ARCHIVE + 'passport_blacklist_'+ find_date.strftime("%d%m%Y")+'.xlsx.backup'
	######################################################################################################

	#Обрабатываем источник terminals
	table='terminals'
	cursor_dwh.execute(f"select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_{table}'")
	find_date=cursor_dwh.fetchone()[0]+timedelta(days=1)
	src_file =  DIR_SOURCE + f'{table}_'+ find_date.strftime("%d%m%Y")+'.xlsx'
	arch_file =  DIR_ARCHIVE + f'{table}_'+ find_date.strftime("%d%m%Y")+'.xlsx.backup'
	while os.path.isfile(src_file):
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_{table}" )
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_del_{table}" )
		df = pd.read_excel( f'{src_file}', sheet_name=f'{table}', header=0, index_col=None )
		df['update_dt'] = find_date.strftime('%Y-%m-%d')
		df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]
		cursor_dwh.executemany(	f"""
								INSERT INTO de11an.bedv_stg_{table}(
									terminal_id,
									terminal_type,
									terminal_city,
									terminal_address,
									update_dt 
								) VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() )

		#Загружаем список id в stg_terminals_del
		df = df[['terminal_id']]
		cursor_dwh.executemany( f"INSERT INTO de11an.bedv_stg_del_{table}(terminal_id) VALUES( %s) ", df.values.tolist() )

		#Загрузка в приемник "вставок" на источнике (формат SCD2).
		cursor_dwh.execute(	f"""
							insert into de11an.bedv_dwh_dim_{table}_hist( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
							select 
								stg.terminal_id, 
								stg.terminal_type, 
								stg.terminal_city, 
								stg.terminal_address, 
								stg.update_dt,
								to_date( '9999-12-31', 'YYYY-MM-DD' ),
								'N'
							from de11an.bedv_stg_{table} stg
							left join de11an.bedv_dwh_dim_{table}_hist tgt
								on stg.terminal_id = tgt.terminal_id
								and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
								and tgt.deleted_flg = 'N'
							where tgt.terminal_id is null
							""")

		#Обновление в приемнике "обновлений" на источнике (формат SCD2).
		cursor_dwh.execute(f"""
							update de11an.bedv_dwh_dim_{table}_hist
							set 
								effective_to = tmp.update_dt - interval '1 second'
							from (
								select 
									stg.terminal_id, 
									stg.update_dt
								from de11an.bedv_stg_{table} stg
								inner join de11an.bedv_dwh_dim_{table}_hist tgt
									on stg.terminal_id = tgt.terminal_id
									and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
									and tgt.deleted_flg = 'N'
								where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
									or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
									or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
							) tmp
							where de11an.bedv_dwh_dim_{table}_hist.terminal_id = tmp.terminal_id
							""")
		cursor_dwh.execute(f"""
							insert into de11an.bedv_dwh_dim_{table}_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
							select 
								stg.terminal_id, 
								stg.terminal_type, 
								stg.terminal_city, 
								stg.terminal_address, 
								stg.update_dt,
								to_date( '9999-12-31', 'YYYY-MM-DD' ),
								'N'
							from de11an.bedv_stg_{table} stg
							inner join de11an.bedv_dwh_dim_{table}_hist tgt
							on stg.terminal_id = tgt.terminal_id
								and tgt.effective_to = stg.update_dt - interval '1 second'
								and tgt.deleted_flg = 'N'
								where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
									or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
									or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
							""")

		#Удаление в приемнике удаленных в источнике записей (формат SCD2).
		cursor_dwh.execute(f"""
							insert into de11an.bedv_dwh_dim_{table}_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
							select 
								tgt.terminal_id, 
								tgt.terminal_type, 
								tgt.terminal_city, 
								tgt.terminal_address, 
								now(),
								to_date( '9999-12-31', 'YYYY-MM-DD' ),
								'Y'
							from de11an.bedv_dwh_dim_{table}_hist tgt
							where tgt.terminal_id in (
								select tgt.terminal_id
								from de11an.bedv_dwh_dim_{table}_hist tgt
								left join de11an.bedv_stg_del_{table} stg
								on stg.terminal_id = tgt.terminal_id
								where stg.terminal_id is null
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
							where de11an.bedv_dwh_dim_{table}_hist.terminal_id in (
								select tgt.terminal_id
								from de11an.bedv_dwh_dim_{table}_hist tgt
								left join de11an.bedv_stg_del_{table} stg
								on stg.terminal_id = tgt.terminal_id
								where stg.terminal_id is null
									and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
									and tgt.deleted_flg = 'N'
							)
							and de11an.bedv_dwh_dim_{table}_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
							and de11an.bedv_dwh_dim_{table}_hist.deleted_flg = 'N'
							""")

		# update meta
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
									where schema_name='de11an' and table_name='bedv_stg_{table}'""")

		# переместить обработанные файлы
		shutil.move(src_file,arch_file)
		#сохраняем все изменения на сервере
		conn_dwh.commit()
		#формируем новую дату для файла
		find_date+=timedelta(days=1) 
		src_file =  DIR_SOURCE + f'{table}_'+ find_date.strftime("%d%m%Y")+'.xlsx'
		arch_file =  DIR_ARCHIVE + f'{table}_'+ find_date.strftime("%d%m%Y")+'.xlsx.backup'
	######################################################################################################

	#Обрабатываем источник sql-сервер
	# имя таблицы, ключ, другие поля необходимые для кодогенерации
	tbl_src=(('cards','card_num','account'),
			 ('accounts','account','valid_to','client'),
			 ('clients','client_id','last_name','first_name','patronymic','date_of_birth','passport_num','passport_valid_to','phone')
			)
	tbl_dwh=(('cards','card_num','account_num'),
			 ('accounts','account_num','valid_to','client'),
			 ('clients','client_id','last_name','first_name','patronymic','date_of_birth','passport_num','passport_valid_to','phone')
			)

	for i in range(len(tbl_src)):
		#формирование SQL-вставок
		table=tbl_dwh[i][0]#таблицы одинаковые
		id_src=tbl_src[i][1]
		id_dwh=tbl_dwh[i][1]
		select_src = ', '.join(tbl_src[i][1:])
		select_dwh = ', '.join(tbl_dwh[i][1:])
		select_stg_dwh ='stg.' + ', stg.'.join(tbl_dwh[i][1:])
		select_tgt_dwh ='tgt.' + ', tgt.'.join(tbl_dwh[i][1:])
		where_dwh = f'where 1=1'
		for p in tbl_dwh[i][1:]:
			where_dwh += f' or stg.{p} <> tgt.{p} or (stg.{p} is null and tgt.{p} is not null) or (stg.{p} is not null and tgt.{p} is null)'
		#формирование SQL-вставок

		#Очистка стейджинговых таблиц
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_{table}" )
		cursor_dwh.execute( f"DELETE FROM de11an.bedv_stg_del_{table}" )

		#Захват данных из источника (измененных с момента последней загрузки) в стейджинг
		cursor_dwh.execute(f"select max_update_dt from de11an.bedv_meta where schema_name='de11an' and table_name='bedv_stg_{table}'")
		find_date=cursor_dwh.fetchone()[0]
		cursor_src.execute( f"""
							select {select_src},  max(coalesce(update_dt, create_dt)) as update_dt from info.{table}
							where max(coalesce(update_dt, create_dt)) > to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
							""")
		cursor_dwh.executemany( f"""INSERT INTO de11an.bedv_stg_{table}({select_dwh}, update_dt) VALUES (%s)""", cursor_src.fetchall())

		#Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
		cursor_src.execute( f"SELECT {id_src} FROM info.{table}" )
		cursor_dwh.executemany( f"""INSERT INTO de11an.bedv_stg_del_{table}({id_dwh}) VALUES (%s)""", cursor_src.fetchall())

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
							""")
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
		cursor_dwh.execute(f"""update de11an.bedv_meta set max_update_dt = to_date('{find_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') 
									where schema_name='de11an' and table_name='bedv_stg_{table}'""")

		#сохраняем все изменения на сервере
		conn_dwh.commit()
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
