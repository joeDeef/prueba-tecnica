import sqlite3

conn = sqlite3.connect("datawarehouse.db")

with open("init_sqlite.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())

conn.close()
print("Base de datos creada correctamente")