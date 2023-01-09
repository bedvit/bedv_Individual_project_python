import psycopg2
import pandas as pd
	
conn = psycopg2.connect(database = "edu",
	                    host =     "de-edu-db.chronosavant.ru",
	                    user =     "de11an",
	                    password = "peregrintook",
	                    port =     "5432")
	
conn.autocommit = False
	
cursor = conn.cursor()

# ������ �� �����
df = pd.read_excel( r'D:\Visual Studio 2017\Projects\PythonApplication1\pandas_in.xlsx', sheet_name='sheet1', header=1, index_col=None )
# ���������� SQL ���� ������� � ���� ������
cursor.executemany( """INSERT INTO de11an.bedv_testtable(id, val) VALUES (%i, %s)""", df.values.tolist())
#��������� ������� (���������)
conn.commit()
# ��������� ����������
cursor.close()
conn.close()
