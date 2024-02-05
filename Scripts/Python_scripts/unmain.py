import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time
import dask.dataframe as dd

def geocode_with_retry(location, geolocator, max_retries=5, delay_factor=2):
    retries = 0
    while retries < max_retries:
        try:
            return geolocator.reverse(location).raw['address']
        except GeocoderTimedOut as e:
            print(f"Geocoding timed out. Retrying... ({retries + 1}/{max_retries})")
            retries += 1
            time.sleep(delay_factor ** retries)
    raise Exception("Geocoding failed after multiple retries.")

start_time = time.time()

geolocator = Nominatim(user_agent="otodomprojectanalysis")

engine = create_engine(URL(
    account='cw56065.ca-central-1.aws',
    user='mnishantkumar',
    password='Rockandroll12@',
    database='PROJECT_1',
    schema='public',
    warehouse='PROJECT1_WH'))

with engine.connect() as conn:
    try:
        query = """ SELECT RN, concat(latitude,',',longitude) as LOCATION
                    FROM (SELECT RN
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,4)+1) AS LATITUDE 
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,1)+1, (REGEXP_INSTR(location,' ',1,2) - REGEXP_INSTR(location,' ',1,1) - 1) ) AS LONGITUDE
                        FROM otodom_data_short_flatten WHERE rn between 1 and 100
                        ORDER BY rn  ) """
        print("--- %s seconds ---" % (time.time() - start_time))

        df = pd.read_sql(query, conn)

        df.columns = map(lambda x: str(x).upper(), df.columns)

        ddf = dd.from_pandas(df, npartitions=10)
        print(ddf.head(5, npartitions=-1))

        # Apply geocoding with retry mechanism
        ddf['ADDRESS'] = ddf['LOCATION'].apply(
            lambda x: geocode_with_retry(x, geolocator),
            meta=(None, 'str')
        )
        print("--- %s seconds ---" % (time.time() - start_time))

        pandas_df = ddf.compute()
        print(pandas_df.head())
        print("--- %s seconds ---" % (time.time() - start_time))

        pandas_df.to_sql('otodom_data_flatten_address', con=engine, if_exists='append', index=False,
                         chunksize=16000, method=pd_writer)
    except Exception as e:
        print('--- Error --- ', e)
    finally:
        conn.close()

engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))
