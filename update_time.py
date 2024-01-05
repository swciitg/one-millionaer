import os
import pymysql
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)


conn = pymysql.connect(**connection)
cursor = conn.cursor()
processed = list()
for path in ['/mnt/data/onedrive/tweets','/mnt/data/o365/tweets', '/media/o365/tweets']:
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file and entry.name not in processed:
                processed.append(entry.name)
                #s = os.stat(entry.path)                
                #t = min(s.st_atime, s.st_mtime, s.st_ctime)
                #t = datetime.fromtimestamp(t)
                #q = 'update files set downloaded_on=%s,downloaded_date=%s,download_path=%s where name = %s'
                #p = (str(t).split('.')[0], str(t.date()), path, entry.name)
                q = 'update files set download_path=%s where name = %s';p = (path, entry.name)
                print(p)
                cursor.execute(q, p)
            


conn.commit();conn.close()
exit()

for i,r in enumerate(rows):
    try:
        print(c-i, r)
        s = os.stat(f"/mnt/data/o365/tweets/{r['name']}")
        t = min(s.st_atime, s.st_mtime, s.st_ctime)
        t = datetime.fromtimestamp(t)
        q = 'update files set downloaded_on=%s,downloaded_date=%s where id = %s'
        p = (str(t).split('.')[0], str(t.date()), r['id'])
        print(p)
        cursor.execute(q, p)
    except FileNotFoundError as e:
        if r['name'].startswith('~tmp'):
            pass
        else:
            print(e)

conn.commit()
conn.close()
