import mysql.connector as ms
import os
from dotenv import load_dotenv
import pandas as pd

os.environ.clear()
load_dotenv(override=True)

# FUNCTIONS
# Creates a connection to MySQL
def ms_connect_cursor(host, user, psswrd):
    try:
        connection = ms.connect(
            host=host,
            user=user,
            password=psswrd
        )
        cursor = connection.cursor()
        return (connection, cursor)
    except:
        print("Não foi possivel se conectar ao MySQL")
        return None

# Creates and uses a database in MySQL
def create_db(conn, db_name):
    cursor = conn[1]
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    cursor.execute(f'USE {db_name}')
    cursor.execute('SHOW DATABASES')
    print(f'\nbases de dados disponiveis: \n{list(db for db in cursor)}\n')

# Creates a table in the database in use
def create_tb(conn, tb_name):
    cursor = conn[1]
    cursor.execute(f'DROP TABLE IF EXISTS {tb_name}')
    create_table = f"""
        CREATE TABLE {tb_name} (
            id_produto VARCHAR(50) PRIMARY KEY,
            nome_produto VARCHAR(100) NOT NULL,
            categoria VARCHAR(50) NOT NULL,
            preco DECIMAL(10,2) NOT NULL,
            quantidade_em_estoque INT NOT NULL,
            data_adicao DATE NOT NULL,
            ativo BOOLEAN )
        """
    cursor.execute(create_table)
    cursor.execute('SHOW TABLES')
    print(f'\nbases de dados disponiveis: \n{list(db for db in cursor)}\n')

# Inserts data from a CSV file into the MySQL table
def insert_data_into_mysql(conn, csv_path, tb_name):
    connection = conn[0]
    cursor = conn[1]
    data = [tuple(row) for i, row in pd.read_csv(
        csv_path, delimiter=",").iterrows()]
    cursor.executemany(
        f'INSERT INTO {tb_name} VALUES(%s, %s, %s, %s, %s, %s, %s)', data)
    connection.commit()

    cursor.execute(f'SELECT * FROM {tb_name}')
    print(f'\ndados adicionados na tabela:\n{[tb for tb in cursor]}\n')

# Closes both the connection and cursor
def close_connection(conn):
    conn[0].close()
    conn[1].close()

# CALLS
# Initiates a MySQL connection, returning a tuple with the connection and the cursor
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
psswrd = os.getenv('MYSQL_PSSWRD')
conn = ms_connect_cursor(host, user, psswrd)

# Creates both the database and the table
db_name = 'db_produtos'
tb_name = 'produtos'
create_db(conn, db_name)
create_tb(conn, tb_name)

# inserts csv data into mysql table
csv_path = '__exercises__/aula05/ex02/dataset.csv'
insert_data_into_mysql(conn, csv_path, tb_name)

close_connection(conn)