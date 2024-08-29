import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import urllib

load_dotenv()

server = os.getenv("SERVER")
db = os.getenv("DATABASE")
driver = os.getenv("DRIVER")
uid = os.getenv("USER")
user=os.getenv("USER2")
pwd = os.getenv("PASSWORD")
port = os.getenv("PORT")
host=os.getenv("HOST")


q = ("""select t.name from table_name from sys.tables t where t.name in ('[HumanResources].[Department]','[Person].[Address]','[Person].[Person]','[Production].[Culture]','[Production].[Culture]','[Production].[ProductListPriceHistory]','[Person].[BusinessEntity]','[Person].[StateProvince]');""")
print(q)
def ext():
    try:
        connection = pyodbc.connect('DRIVER' +driver+ ';SERVER'+server+';DATABASE'+db+';Trusted_Connection = yes; ')

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(q)
                tables=cursor.fetchall()
                print(tables)
                cursor.close()

        for t in tables:
            df = pd.read_sql_query(f'select * from {t[0]}', connection)
            load(df , t[0])
    except Exception as e:
        print("process failed")
    finally:
        connection.close()


def load(df,t):
    r = 1

    try:
        engine = create_engine( f"postgresql://{user}:{pwd}@{host}:{port}/{db}" )
        df.to_sql(f'stg_{t}',engine,if_exists='replace',index=False)
        print(f"table{ t} imported")

    except Exception as e:
        print(e)



