import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine,Table, MetaData, insert,text
import pandas as pd
import psycopg2
import urllib

load_dotenv()

server = os.getenv("SERVER")
db = os.getenv("DATABASE")
db2= os.getenv("DATABASE2")
driver = os.getenv("DRIVER")
uid = os.getenv("USER")
user=os.getenv("USER2")
pwd = os.getenv("PASSWORD")
port = os.getenv("PORT")
host=os.getenv("HOST")


CREATE_ROOM = text(" CREATE TABLE  myooms (id SERIAL PRIMARY KEY, name TEXT);")
CREATE_TEMP = text(" CREATE TABLE  tempuuuu(room_id INTEGER, tempreture REAL ,date TIMESTAMP, FOREIGN KEY(room_id) REFERENCES myooms(id) ON DELETE CASCADE );")

q = ("""
SELECT 
    s.name AS schema_name, 
    t.name AS table_name
FROM 
    sys.tables t
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    s.name IN ('HumanResources', 'Person', 'Production', 'Purchasing', 'Sales')
ORDER BY 
    s.name, t.name
""")

def map_dtype(dtype):
    if dtype == 'int64':
        return 'INTEGER'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'object':
        return 'TEXT'
    elif dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'TEXT'  # Default to TEXT for unknown types


def generate_create_table_sql(df, table_name):
    columns = []
    for column, dtype in df.dtypes.items():
        sql_type = map_dtype( str( dtype ) )
        columns.append( f'"{column}" {sql_type}' )

    columns_sql = ",".join( columns )
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
    return create_table_sql

def ext():
    try:
        connection = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection = yes; ' )
        print(connection)

        cursor = connection.cursor()
        cursor.execute(q)
        tables=cursor.fetchall()
        print(tables)
        cursor.close()

        for t in tables:
            tname=f"{t[0]}.{t[1]}"
            df = pd.read_sql_query(f'select * from {t[0]}.{t[1]};', connection)
            # print(df)
            load(df , t ,tname)
    except Exception as e:
        print("process failed")
    finally:
        connection.close()


def load(df,t,tname):
     try:
            s = create_schema_sql = f'''CREATE SCHEMA IF NOT EXISTS "{t[0]}";'''
            sql = generate_create_table_sql( df, tname )
            # print( s + sql )
            # print( df )
            s.lower()
            engine2 = create_engine( f"postgresql://{user}:{pwd}@{host}:{port}/{db2}" )
            connection=engine2.connect()
            connection.execute(text(s.lower()+sql))
            df.to_sql(name = tname,con=engine2  , index=False, if_exists='append')
            print(f'table{ t} imported')
            connection.commit()
            connection.close()

     except Exception as e:
        print(e)



ext()

#debugg statements :)
# print( s + sql )
# print( df )
# except Exception as e:
#     print("error " + str(e))
# metadata = MetaData( )
# table = Table( 'rooms', metadata, autoload_with=engine2, schema='dbo' )
# # connection.execute( CREATE_ROOM)
# # connection.execute( CREATE_TEMP )