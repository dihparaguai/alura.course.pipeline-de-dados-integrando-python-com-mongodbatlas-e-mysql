import mysql.connector as ms
import os
from dotenv import load_dotenv
load_dotenv()

# limpa as variaveis de ambiente e recarrega os dados na pasta '.env'
os.environ.clear()
load_dotenv(override=True)

connection = ms.connect(
    host=os.getenv("MYSQL_AULA_04_EX_02_HOST"),
    user=os.getenv("MYSQL_AULA_04_EX_02_USER"),
    password=os.getenv("MYSQL_AULA_04_EX_02_PSSWRD")

)

cursor = connection.cursor()

cursor.execute('CREATE DATABASE IF NOT EXISTS db_teste2;')
cursor.execute('SHOW DATABASES;')
print([db for db in cursor])
