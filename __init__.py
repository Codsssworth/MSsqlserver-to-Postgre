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

def ext():
    # try:
        connection = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection = yes; ' )
        print(connection)

        cursor = connection.cursor()
        cursor.execute(q)
        tables=cursor.fetchall()
        print(tables)
        cursor.close()

        for t in tables:
            print(t[0] ,t[1] )
            df = pd.read_sql_query(f'select * from {t[0]}.{t[1]};', connection)
            print(df)
            load(df , t)
    # except Exception as e:
    #     print("process failed")
    # finally:
    #     connection.close()


def load(df,t):
    r = 1
    print("ree")

    # try:
    engine2 = create_engine( f"postgresql://{user}:{pwd}@{host}:{port}/{db2}" )
    connection=engine2.connect()
    metadata = MetaData( )
    # connection.execute( CREATE_ROOM)
    # connection.execute( CREATE_TEMP )
    connection.commit()
    connection.close()

    table = Table( 'rooms', metadata, autoload_with=engine2, schema='dbo' )
    print(df)
    # df.to_sql(name = 'rooms',con=engine  , if_exists='append')
    print(f"table{ t} imported")

    # except Exception as e:
    #     print(e)


# try:
ext()
# except Exception as e:
#     print("error " + str(e))