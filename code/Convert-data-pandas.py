import sqlite3
import pandas as pd

# เชื่อมต่อฐานข้อมูล
conn = sqlite3.connect('your_new_data01.db')

# ดึงข้อมูลทั้งหมดจากตาราง 'product' มาใส่ใน DataFrame
product = pd.read_sql_query("SELECT * FROM product", conn)
product = product.set_index('productNo') 
customers = pd.read_sql_query("SELECT * FROM customers", conn)
transaction = pd.read_sql_query('SELECT * FROM "transaction"', conn)
customers = customers.set_index('CustomerNo')  
transaction = transaction.set_index('TransactionNo')
# แสดงข้อมูลใน DataFrame
print(df)

# ปิดการเชื่อมต่อ
conn.close()