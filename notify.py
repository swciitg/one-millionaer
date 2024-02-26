# With this script you can download millions of files from a onedrive folder.
# This script was copied from https://github.com/O365/python-o365/blame/master/examples/onedrive_example.py
#
# Original Authors:
#   * https://github.com/lamductan
#   * https://github.com/eshifflett
# 
# Modifications By:
#   * https://github.com/iamtalhaasghar
#
# 2024-01-04

import os
import sqlite3, pymysql
import argparse
from O365 import Account
from O365 import FileSystemTokenBackend
import logging
from dotenv import load_dotenv
import multiprocessing, threading
from concurrent.futures import ThreadPoolExecutor
import time
import requests
import functools
from datetime import datetime 
from redis import Redis
import psutil

load_dotenv()

client_id = os.getenv('CLIENT_ID')  # Your client_id
client_secret = os.getenv('CLIENT_SECRET')  # Your client_secret, create an (id, secret) at https://apps.dev.microsoft.com
scopes = ['basic', 'https://graph.microsoft.com/Files.ReadWrite.All']
CHUNK_SIZE = 1024 * 1024 * 5
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)

rdb = Redis()

def files_download():
    conn = pymysql.connect(
        **connection
    )
    cursor = conn.cursor()
    logging.info(f"counting files downloaded")        
    q = "select count(*) as count from files where downloaded = 1"
    cursor.execute(q)
    c = cursor.fetchone()
    conn.close()
    return int(c['count'])


def ntfy(msg):
    # send a post request to a url
    try:
        url = os.getenv('NTFY_URL')
        requests.post(url, data=msg, headers={'Priority': 'high'})
    except Exception as e:
        logging.exception(e)
        pass




def get_battery_level():
    try:
        battery = psutil.sensors_battery()
        percent = battery.percent
        return percent
    except AttributeError:
        # Handle the case where the 'sensors_battery' attribute is not available
        return None




if __name__ == '__main__':
    while True:
        #if not rdb.exists('/millionaer/ping'):
        #    ntfy(f'Stuck! {files_download()}')
        #    time.sleep(5*60)
        #else:
        #    print('all fine')
        battery_level = get_battery_level()
        if battery_level < 15:
            print(f"Battery Level: {battery_level}%")
            ntfy(f'Battery! {battery_level}')
            time.sleep(3*60)

        time.sleep(5)

