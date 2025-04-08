import requests
import pandas as pd
import sqlite3
url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
r = requests.get(url)
r.json()
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)

#Drop (ลบ) column id ที่ไม่ได้ใช้ หรือใครจะใช้ก็ไม่ต้องลบ
#conversion_rate = conversion_rate.drop(columns=['id'])
conversion_rate
#เอาไว้เช็ค type
#print(type(result_conversion_rate))  
# assert isinstance(result_conversion_rate, list)
แปลงคอลัมน์ 'date' ใน conversion_rate และ 'Date' ใน merged_transaction ให้เป็น datetime
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
print("Data saved to output.parquet")