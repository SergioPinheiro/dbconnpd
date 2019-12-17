import mysql
import pandas as pd
from sqlalchemy import create_engine
from math import ceil

class DBConnect:
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = create_engine("mysql+pymysql://{user}:{password}@{host}/{database}".format(
                user = self.user,
                password = self.password,
                host = self.host,
                database = self.database
            ))
        except Exception as ex:
            print("Falha ao conectar: " + str(ex)) 

    def get_DataFrame(self, tableName):
        if not self.connection:
            self.connect()
        try:
            df = pd.read_sql(f"select * from {tableName}", self.connection)
            return df
        except Exception as ex:
            print("Falha ao receber dados: " + str(ex))
            return None 
    
    def set_Dataframe(self, tableName, dataframe, chunksize='auto', if_exists='replace'):
        if not self.connection:
            self.connect()
        try:
            if chunksize == 'auto':
                chunksize = self.calc_chunksize(len(dataframe))
            elif chunksize == 'all':
                chunksize = None
                
            dataframe.to_sql(tableName, self.connection, index=False, if_exists=if_exists, chunksize=chunksize)
        except Exception as ex:
            print("Falha ao enviar dados: " + str(ex))

    def calc_chunksize(self, size, division=10, minimum=10, maximum=1000):
        size = ceil(size/division)
        size = max([size, minimum])
        size = min([size, maximum])
        return size

    def close_conection(self):
        # self.connection.close()
        self.connection = None