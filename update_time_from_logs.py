import os
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from glob import glob
load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)


conn = pymysql.connect(**connection)
cursor = conn.cursor()
log_dir = '/var/log/onedrive/*'
processed = list()
c = 0
for log in glob(log_dir):
    print('opening log file', log)
    with open(log) as log_file:
        for line in log_file:            
            if 'done' in line:
                line=line.replace('[', '')
                tokens = line.split()
                time = ' '.join(tokens[0:2]).strip().split('.')[0]
                time = datetime.strptime(time, '%Y-%b-%d %H:%M:%S')
                filename = tokens[4].replace('tweets/', '')
                q = 'update files set downloaded_on=%s,downloaded_date=%s where name = %s'
                p = (time, time.date(), filename)
                cursor.execute(q, p)
                c += 1
                if c % 1000 == 0:
                    conn.commit()                    
                print(c,time, filename)

conn.commit();conn.close()
exit()              
'''
if True:
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file and entry.name not in processed:
                processed.append(entry.name)
                #s = os.stat(entry.path)                
                #t = min(s.st_atime, s.st_mtime, s.st_ctime)
                #t = datetime.fromtimestamp(t)
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
'''
