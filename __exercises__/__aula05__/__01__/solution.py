import mysql.connector as ms
import pandas as pd
import os, sys
from dotenv import load_dotenv
load_dotenv()


class MyMySQLOperations:
    def __init__(self, host, user, psswrd):
        self.connection = self.__connect_to_mysql(host, user, psswrd)
        self.cursor = self.__create_mysql_cursor()

    def __connect_to_mysql(self, host, user, psswrd):
        try:
            conn = ms.connect(
                host=host,
                user=user,
                password=psswrd
            )
            print('conexão com mysql efetuada com sucesso!!!\n')
            return conn
        except:
            print('!! não foi possivel se conectar ao mysql, revise as credenciais informadas !!')   
            sys.exit(1) # <-- stops the program if the connection fails

    def __create_mysql_cursor(self):
        return self.connection.cursor()

    def create_db(self, db_name):
        self.cursor.execute(f'DROP DATABASE {db_name}')
        self.cursor.execute(f'CREATE DATABASE {db_name}')
        self.cursor.execute(f'USE {db_name}')
        print(f'\n>> banco de dados {db_name} criado com sucesso!')

    def create_tb(self, tb_name):
        sql = f'''
            CREATE TABLE {tb_name} (
                id              VARCHAR(50) PRIMARY KEY,
                nome            VARCHAR(100),
                idade           TINYINT,
                email           VARCHAR(100),
                data_cadastro   DATE,
                ativo           BOOLEAN
            )
        '''
        self.cursor.execute(sql)
        print(f'\n>> tabela de dados {tb_name} criado com sucesso!')

    # reads a csv file using pandas and inserts it into mysql
    def insert_csv_data_into_mysql(self, tb_name, csv_path):
        df = pd.read_csv(csv_path)
        print(f'\n>> qtd linhas e colunas do csv: {df.shape}')
        
        data = [tuple(row) for i, row in df.iterrows()]
        sql = f'INSERT INTO {tb_name} VALUES (%s, %s, %s, %s, %s, %s)'
        
        self.cursor.executemany(sql, data)
        self.connection.commit()
        self.cursor.execute(f'SELECT id FROM {tb_name}')
        
        sql_count_data = len([row for row in self.cursor])
        print(f'>> qtd de linhas na tabela do sql: {sql_count_data}')
        
    def show_tb_data(self, tb_name):
        self.cursor.execute(f'SELECT * FROM {tb_name}')
        print([row for row in self.cursor])
        
    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print('\n\nconexão com mysql fechada com sucesso!')

# gets the credentials from .env
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
psswrd = os.getenv('MYSQL_PASSWORD')

# creates the connection, database, and table in mysql
db = MyMySQLOperations(host, user, psswrd)
db.create_db('db_clientes')
tb_name = 'clientes'
db.create_tb(tb_name)

# reads and inserts data from a csv file into a mysql table
csv_path = '__exercises__/__aula05__/__01__/dataset.csv'
db.insert_csv_data_into_mysql(tb_name, csv_path)
db.show_tb_data(tb_name)

db.close_connection()