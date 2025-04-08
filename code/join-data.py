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
#Join tables T-T 
merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
print(merged_transaction)

# ปิดการเชื่อมต่อ
conn.close()