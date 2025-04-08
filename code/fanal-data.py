import sqlite3
import pandas as pd
import requests
from fastparquet import write, ParquetFile

# เชื่อมต่อฐานข้อมูล SQLite
conn = sqlite3.connect('your_new_data01.db')

# ดึงข้อมูลทั้งหมดจากตาราง 'customer', 'product', และ 'transaction'
customer = pd.read_sql_query("SELECT * FROM customers", conn)
product = pd.read_sql_query("SELECT * FROM product", conn)
transaction = pd.read_sql_query('SELECT * FROM "transaction"', conn)

# ปิดการเชื่อมต่อฐานข้อมูล
conn.close()

# รวมข้อมูลจากหลาย DataFrame
merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")

# แปลงคอลัมน์ 'Date' ใน merged_transaction ให้เป็น datetime
merged_transaction['Date'] = pd.to_datetime(merged_transaction['Date'])

# แสดงข้อมูลใน DataFrame ที่รวมแล้ว
print("Merged Transaction DataFrame:")
print(merged_transaction.head())

# ดึงข้อมูลจาก API และแปลงเป็น DataFrame
import requests
url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
r = requests.get(url)
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.drop(columns=['id'])

# ตรวจสอบข้อมูลใน conversion_rate
print("Conversion Rate DataFrame:")
print(conversion_rate.head())

# แปลงคอลัมน์ 'date' ใน conversion_rate และ 'Date' ใน merged_transaction ให้เป็น datetime
conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
merged_transaction['Date'] = pd.to_datetime(merged_transaction['Date'])

# รวมข้อมูลกับ conversion_rate โดยใช้คอลัมน์ 'Date' และ 'date'
final_df = merged_transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")

# แสดงเฉพาะส่วนหัว (head) ของ DataFrame ที่รวมกันแล้ว
print("Head of Final DataFrame:")
print(final_df.head())
print(final_df.columns)

# คำนวณ total_amount และ thb_amount
final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]
final_df = final_df.drop(["date", "gbp_thb"], axis=1)
print(final_df.head())
# บันทึกข้อมูลลง CSV
final_df.to_parquet("final001.parquet", index=False)
print("Data saved to output.parquet"