########20240805_Ковалев_Алексей############
import pymssql
import pandas as pd
import sqlalchemy.types
from sqlalchemy import create_engine
import datetime

import os
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

DB_LOGIN = 'test_MyDB'#os.getenv('LOGIN')
DB_PASSWORD = 'test_MyDB'#os.getenv('PASSWORD')
DB_HOST = 'MOW03-SQL64'
# DB_HOST = 'localhost'
DB_NAME = 'test_MyDB'
DB_SCHEME = 'dbo'
DB_TABLE = 'sales'
DB_CONNECTION_STRING = f'mssql+pymssql://{DB_LOGIN}:{DB_PASSWORD}' \
                       f'@{DB_HOST}/{DB_NAME}?charset=utf8'
DB_STORED_PROCEDURE = 'ExportDataByDateRange'

import logging
logger = logging.getLogger(__name__)


import tkinter as tk
from tkinter import messagebox
from tkcalendar import Calendar


# ------------------------------------------------------------------------------
# bad path
# ------------------------------------------------------------------------------
from math import ceil


def drop_table(conn: pymssql.Connection, cursor: pymssql.Cursor):
    SQL_STATEMENT = f"""
    TRUNCATE TABLE {DB_NAME}.{DB_SCHEME}.{DB_TABLE};
    """
    logger.debug(f'{SQL_STATEMENT}')
    cursor.execute(SQL_STATEMENT)

    conn.commit()
    pass


def insert_values(conn: pymssql.Connection, cursor: pymssql.Cursor,
                  df: pd.DataFrame):
    places = ('%s', ) * len(df.columns)
    SQL_STATEMENT = f"""
    INSERT {DB_SCHEME}.{DB_TABLE} (dt, article, kg) 
    OUTPUT INSERTED.rowid 
    values ({','.join(places)});
    """
    # VALUES ({','.join(places)});

    logger.debug(f'-df -> list')
    # replace <NA> to None(NULL)
    data = [[None if pd.isna(x) else x for x in row.values.tolist()]
            for index, row in df.iterrows()]
    # data = df.values.tolist()
    logger.debug(f'{SQL_STATEMENT}, {len(data)}')
    logger.debug(f'+df -> list')

    logger.debug(f'-executemany')
    # cursor.executemany(SQL_STATEMENT, data) # 40+ min :(

    chunk_size = 10000  # 60+ min :(
    start_index, end_index = 0, 0
    for x in range(1, int(ceil(len(data)/chunk_size)) + 1):
        end_index = x * chunk_size
        chunk = data[start_index: end_index]
        cursor.executemany(SQL_STATEMENT, chunk)
        # 'VALUE': 5min - 10000 row
        # 'value': 2min - 10000 row

        # result = cursor.fetchall()
        logger.debug(f'{x}: {[start_index, end_index]} -> {len(chunk)}')
        conn.commit()

        start_index = end_index
        if end_index > len(data):
            break
        pass

    logger.debug(f'+executemany')
    pass


def load_manual():  #
    conn = pymssql.connect(
        server=DB_HOST,
        user=DB_LOGIN,
        password=DB_PASSWORD,
        database=DB_NAME,
        as_dict=True
    )
    cursor = conn.cursor()

    drop_table(conn, cursor)

    df = read_excel('./_data.xlsb')  # small data
    # df = read_excel('./data.xlsb')  # original: 4min

    insert_values(conn, cursor, df)

    cursor.close()
    conn.close()
    pass
# ------------------------------------------------------------------------------


def load_pandas(df: pd.DataFrame):
    logger.debug(f'-sqlalchemy')
    engine = create_engine(DB_CONNECTION_STRING)
    count_row = df.to_sql(con=engine, name=DB_TABLE, schema=DB_SCHEME,
                          if_exists='replace', index=False,
                          # index_label='rowid'
                          dtype={
                              'dt': sqlalchemy.types.DATE,
                              'article': sqlalchemy.types.BIGINT,
                              'kg': sqlalchemy.types.SMALLINT,
                          }
                          )
    logger.debug(f'+sqlalchemy')

    msg = f'Data loaded to "{DB_NAME}.{DB_SCHEME}.{DB_TABLE}"'
    tk.messagebox.showinfo('Message', msg)
    pass


def convert_date(value):
    return pd.to_datetime(value, unit='D', origin='1899-12-30')
    pass


def convert_int(value, default=None):
    try:
        result = int(value)
        if result < 0:
            raise ValueError
    except (TypeError, ValueError) as inst:
        logger.warning(f'{value} -> {default}')
        result = default
    return result
    pass


def read_excel(filename: str):
    logger.debug(f'-read excel')

    df = []
    with pd.ExcelFile(filename) as xls:
        df = pd.read_excel(xls, engine='pyxlsb', sheet_name='sales',
                           # comment='#', # 160444(2): 23.03.2021 | #РѕС€РёР±РєР° | -1
                           converters={
                               'dt': convert_int,
                               'article': convert_int,
                               'kg': convert_int},
                           )
    df['dt'] = pd.to_datetime(df['dt'], unit='D', origin='1899-12-30')
    df['article'] = df['article'].astype('Int64')
    df['kg'] = df['kg'].astype('Int64')

    logger.debug(f'+read excel')
    return df
    pass


# ------------------------------------------------------------------------------


def export_data_by_date_range(_from: str, _to: str):
    logger.debug(f'-get data')
    engine = create_engine(DB_CONNECTION_STRING)
    sql = f"""
    DECLARE @return_value int
    
    EXEC @return_value = [{DB_SCHEME}].[{DB_STORED_PROCEDURE}]
         @from = '{_from}',
         @to = '{_to}'

    SELECT 'Return Value' = @return_value
    """

    df = pd.read_sql(sql, engine)
    df.columns = list((*df.columns[:-1], f'[{_from}:{_to}]'))
    df.info()
    logger.debug(f'+get data')

    logger.debug(f'-export')
    dt_now = datetime.datetime.now()
    filename = f'./report_{dt_now.strftime("%Y-%m-%d %H%M%S.%f")}.xlsx'
    sheet = 'report'
    with pd.ExcelWriter(filename) as writer:
        df.to_excel(writer, sheet_name=sheet, header=True,
                    freeze_panes=(1, 0), index=False)
        workbook = writer.book
        worksheet = writer.sheets[sheet]
        #worksheet.autofit()

        format_int = workbook.add_format()
        format_int.set_num_format('0')

        # format_year = workbook.add_format()
        # format_year.set_num_format('YYYY')
        # format_month = workbook.add_format()
        # format_month.set_num_format('MMMM')

        format_decimal = workbook.add_format()
        format_decimal.set_num_format('0.00')

        worksheet.set_column(0, 2, None, format_int)
        worksheet.set_column(2, 2, 12, format_int)
        worksheet.set_column(3, 5, None, format_decimal)
        worksheet.set_column(4, 4, 12, format_decimal)
        worksheet.set_column(5, 5, 21, format_decimal)

        msg = f'Report "{filename}" is done'
        tk.messagebox.showinfo('Message', msg)
        pass

    logger.debug(f'+export')
    pass


# ------------------------------------------------------------------------------


def main():
    def load_data():
        # df = read_excel('./_data.xlsb')  # small test data
        df = read_excel('./data.xlsb')
        df.info()
        load_pandas(df)
        pass

    def export_data():
        export_data_by_date_range(
            _from=calendar_from.selection_get(),
            _to=calendar_to.selection_get())
        pass

    # --------------------------------------------------------------------------
    logging.basicConfig(level=logging.DEBUG)
    logger.propagate = False
    logFormatter = logging.Formatter("%(asctime)s [%(filename)s:%(lineno)s - "
                                     "%(funcName)20s() ] %(message)s")
    fileHandler = logging.FileHandler(filename='./_log.txt',
                                      mode='w', encoding='utf-8')
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

    logger.info('=== start')
    logger.info(f'{DB_CONNECTION_STRING}')
    # --------------------------------------------------------------------------
    window = tk.Tk()
    window.title("'Cherkizovo' report")
    window.geometry('640x480')

    frame = tk.Frame(window, padx=10, pady=10)
    frame.pack(fill=tk.X, expand=True)

    # load_data
    btn_load_data = tk.Button(frame, text='1. load data',
                              command=load_data)
    btn_load_data.grid(row=1, column=1, sticky='we')

    # export_data
    calendar_from = Calendar(frame, selectmode='day',
                             year=2021, month=2, day=2)
    calendar_from.grid(row=2, column=1)

    calendar_to = Calendar(frame, selectmode='day',
                           year=2021, month=5, day=13)
    calendar_to.grid(row=2, column=2)

    btn_export_data = tk.Button(frame, text="2. create report",
                                command=export_data)
    btn_export_data.grid(row=3, column=2, sticky='we')
    # --------------------------------------------------------------------------
    window.mainloop()

    # load_data()
    # export_data_by_date_range(
    #     _from='2021-05-01',
    #     _to='2021-05-10'
    # )
    logger.info('=== stop')
    pass


if __name__ == '__main__':
    main()
    pass
#####################################
