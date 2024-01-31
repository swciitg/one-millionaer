# a script to do a task on data files

import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)


def the_task(path, query, params):
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()
    c = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file:
                cursor.execute(query, params)        
                print(c, entry.name)
                c+=1
    conn.commit()
    conn.close()


if __name__=="__main__":
    

    # mark files in /mnt/data/o365/tweets as downloaded
    path = '/mnt/data/o365/tweets'
    query = "UPDATE files SET downloaded = %s WHERE name = %s;"
    params = (1, entry.name)
    the_task(path, query, params)
