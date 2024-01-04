import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)


conn = pymysql.connect(**connection)
cursor = conn.cursor()
c = 0
with os.scandir('/mnt/data/o365/tweets') as it:
    for entry in it:
        if entry.is_file:
            update_query = "UPDATE files SET downloaded = %s WHERE name = %s;"
            cursor.execute(update_query, (1, entry.name))        
            print(c, entry.name)
            c+=1
conn.commit()
conn.close()
