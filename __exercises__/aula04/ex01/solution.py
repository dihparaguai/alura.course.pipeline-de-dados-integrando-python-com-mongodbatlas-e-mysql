import mysql.connector as ms
import os
from dotenv import load_dotenv
load_dotenv()

host = os.getenv('MYSQL_AULA_04_EX_01_HOST')
user = os.getenv('MYSQL_AULA_04_EX_01_USER')
psswrd = os.getenv('MYSQL_AULA_04_EX_01_PSSWRD')

conn = ms.connect(
    host=host,
    user=user,
    password=psswrd
)

cursor = conn.cursor()

cursor.execute('CREATE DATABASE IF NOT EXISTS db_teste')
cursor.execute('SHOW DATABASES')

print([db for db in cursor])
