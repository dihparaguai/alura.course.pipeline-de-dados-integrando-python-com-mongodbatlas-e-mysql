import pandas as pd
import mysql.connector as ms
import os
# uses the dotenv file to get the sensitive data
from dotenv import load_dotenv
load_dotenv()


class MyMySQL():
    def __init__(self, host, user, password):
        self.conn = self.__connect_to_mysql(host, user, password)
        self.cursor = self.conn.cursor()  # creates a cursor to execute sql commands

    def __connect_to_mysql(self, host, user, password):
        try:
            # creates a connection to the mysql server
            conn = ms.connect(
                host=host,
                user=user,
                password=password
            )
            print('\nA conexão com o MySQL foi realizada com sucesso!')
            return conn
        except:
            print('\nNão foi possivel estabelecer uma conexão com o MySQL, reveja as credenciais informadas')
            return None

    def create_db(self, db_name):
        self.cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
        self.cursor.execute(f'USE {db_name}')
        self.cursor.execute('SHOW DATABASES;')

        # must be run each item in the cursor
        print(f'\n\n--> Bancos de dados disponivels: {[db for db in self.cursor]}')

    def create_table(self, tb_name):
        # drops the table if it already exists
        self.cursor.execute(f'DROP TABLE IF EXISTS {tb_name};')

        # sql command to create a table
        sql = f"""    
            CREATE TABLE {tb_name} (
                id              VARCHAR(100),
                produto         VARCHAR(100),
                categoria       VARCHAR(100),
                preco           DECIMAL(10,2),
                frete           DECIMAL(10,2),
                data_compra     DATE,
                vendedor        VARCHAR(100),
                local_compra    VARCHAR(100),
                avaliacao       TINYINT,
                tipo_pagamento  VARCHAR(100),
                qtd_parcelas    TINYINT,
                lat             FLOAT,
                lon             FLOAT,
                
                PRIMARY KEY (id)
            );
        """
        self.cursor.execute(sql)

        self.cursor.execute('SHOW TABLES;')
        print(f'\n--> Tabelas disponiveis: {[tb for tb in self.cursor]}')

    def insert_csv_data_into_mysql_table(self, csv_data_path, tb_name):
        # creates a dataframe using a csv file
        df = pd.read_csv(csv_data_path)
        print(f'\n--> Nome das colunas no arquivo CSV:\n{df.columns}')
        print(f'\n--> Quantidade de linhas e colunas no arquivo CSV:\n{df.shape}')

        # returns each row as a "tuple"
        data_sql = [tuple(row) for i, row in df.iterrows()]

        sql = f'INSERT INTO {tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        # to execute multiples commands, a connection commit is needed
        self.cursor.executemany(sql, data_sql)
        self.conn.commit()

        self.cursor.execute(f'SELECT * FROM {tb_name}')
        print(f'--> Quantidade de tuplas na tabela "{tb_name}" no MySQL:\n{len([row for row in self.cursor])}')

    def show_table_data(self, tb_name):
        print(f'\n--> 10 primeiros dados da tabela "{tb_name}":')
        self.cursor.execute(f'SELECT * FROM {tb_name} LIMIT 10')
        for row in self.cursor:
            print(row)

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
        print('\n\nA conexão com o MySQL foi fechada com sucesso!')


# obtains credential data
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')

# creates a connection and a database in mysql
db = MyMySQL(host, user, password)
db.create_db('db_produtos')

# creates a table and inserts data into it from a csv file
file_path = 'data/produtos_categoria_livros.csv'
tb_name = 'livros'
db.create_table(tb_name)
db.insert_csv_data_into_mysql_table(file_path, tb_name)
db.show_table_data(tb_name)

# creates a table and inserts data into it from another csv file
file_path = 'data/produtos_data_compra_maior_que_2020.csv'
tb_name = 'produtos_data_compra_maior_que_2020'
db.create_table(tb_name)
db.insert_csv_data_into_mysql_table(file_path, tb_name)
db.show_table_data(tb_name)

db.close_connection()
