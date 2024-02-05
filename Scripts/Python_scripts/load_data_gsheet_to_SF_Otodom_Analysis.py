import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time 
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

start_time = time.time()

engine = create_engine(URL(
                    account = 'ceiknnc-keb24333',
                    user = 'thoufiq',
                    password = 'Admin$1234',
                    database = 'demo',
                    schema = 'public',
                    warehouse = 'demo_wh'))
                
with engine.connect() as conn:
    try:
        query = """ SELECT ID, SPREADSHEET_NAME FROM otodom_data_log """
        df = pd.read_sql(query,conn)
        df.columns = map(lambda x: str(x).upper(), df.columns)

        gc = gspread.service_account()
        loop_counter = 0

        for index, row in df.iterrows():
            loop_counter += 1
            locals()['sh'+str(loop_counter)] = gc.open(row['SPREADSHEET_NAME'])
            wks = locals()['sh'+str(loop_counter)].get_worksheet(0)
            df_out = get_as_dataframe(wks, usecols=[0,1,2], nrows=wks.row_count, header=None, skiprows=1, evaluate_formulas=True)
            print('Spreadsheet '+row['SPREADSHEET_NAME']+' loaded back to DataFrame!')
            
            df_out.columns = ['RN', 'TITLE', 'TITLE_ENG']
            df_out.to_sql('otodom_data_flatten_translate', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)

    except Exception as e:
        print('--- Error --- ',e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))