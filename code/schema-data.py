import sqlite3



database_path = r'/path/to/your/directory/your_new_data01.db'
conn = sqlite3.connect('your_new_data01.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

 with sqlite3.connect(database_path) as conn:
        with conn.cursor() as cursor:
            # ค้นหารายการตารางทั้งหมด
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            tables = ['customer', 'product', 'transaction']
            
        for table_name in tables:
            print(f"\nSchema ของตาราง '{table_name}':")
            cursor.execute(f"PRAGMA table_info({table_name});")
            schema = cursor.fetchall()
            for column in schema:
                print(f"Column: {column[1]}, Type: {column[2]}, Not Null: {column[3]}, Default Value: {column[4]}, Primary Key: {column[5]}")

        cursor.close()
        conn.close()
        cursor.execute("SELECT * FROM product")
        product_data = cursor.fetchall()
        for row in product_data:
            print(row)
      