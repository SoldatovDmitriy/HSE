import json
import getpass
import psycopg2

# Считываем настройки подключения
with open("config.json") as f:
    config = json.load(f)

# Получаем логин и пароль от пользователя
user = input("Введите логин: ")
password = getpass.getpass("Введите пароль: ")

# Соединяем безопасно
conn = psycopg2.connect(
    dbname=config["dbname"],
    user=user,
    password=password,
    host=config["host"],
    port=config["port"]
)

cur = conn.cursor()
cur.execute("SELECT VERSION();")
version = cur.fetchone()
print("PostgreSQL version:", version[0])

cur.close()
conn.close()
