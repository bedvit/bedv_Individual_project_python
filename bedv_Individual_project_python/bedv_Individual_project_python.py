import psycopg2
import pandas as pd
	
conn = psycopg2.connect(database = "edu",
	                    host =     "de-edu-db.chronosavant.ru",
	                    user =     "de11an",
	                    password = "peregrintook",
	                    port =     "5432")
	
conn.autocommit = False
	
cursor = conn.cursor()

# Чтение из файла
df = pd.read_excel( r'D:\Visual Studio 2017\Projects\PythonApplication1\pandas_in.xlsx', sheet_name='sheet1', header=1, index_col=None )
# Выполнение SQL кода вставки в базу данных
cursor.executemany( """INSERT INTO de11an.bedv_testtable(id, val) VALUES (%i, %s)""", df.values.tolist())
#сохраняем коннект (изменения)
conn.commit()
# Закрываем соединение
cursor.close()
conn.close()
