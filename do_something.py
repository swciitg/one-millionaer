#!/mnt/data/projects/million-downloader/.venv/bin/python
# a script to do a task on data files

import os
import pymysql
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from time import time
import re

load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)

logging_format ='%(asctime)s - %(levelname)s - %(message)s' 
logging.basicConfig(filename='/var/log/millionaer/do_something.txt', level=logging.INFO, format=logging_format)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(logging_format)
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

total_files = 5328689

def sync_download_path(path):
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()
    c = 0
    data = list()
    t0 = time()
    q = "UPDATE files SET download_path = %s WHERE name = %s;"
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file:
                c+=1
                data.append((path, entry.name))
                if c%100000 == 0:
                    cursor.executemany(q, data)
                    conn.commit()
                    data.clear()
                    avg=int(c/(time()-t0+0.0001))
                    time_left = (total_files / avg)/3600
                    logging.info(f'{c} files done @ {avg} files/sec Time left: {time_left:.02f} hrs')
    conn.commit()
    conn.close()


def extract_tweets():

    # todo:extract parent tweet of quoted & replied tweets
    # todo:de-dup replied tweets with their parent tweets if they already exist in db
    # todo:extract user info
    

    def read_file(path):    
        fh = open(path, 'r') 
        try:
            d = json.load(fh)
            return d
        except Exception as e:
            logging.exception(e)
        finally:
            fh.close()

    conn = pymysql.connect(**connection)
    cursor = conn.cursor()
    timeformat_string = '%Y-%m-%dT%H:%M:%S.%fZ'
    while True:
        q = "select * from files where isnull(lang) limit 1000"
        cursor.execute(q)
        rows = cursor.fetchall()
        if not rows:
            break
        params = list()
        t0 = time()
        c = 0
        for r in rows:
            try:
                d = read_file(os.path.join(r['download_path'], r['name']))
                referenced_tweets = d['data'].get('referenced_tweets') 
                rule = d['matching_rules'][0]
                is_retweet = True if referenced_tweets and referenced_tweets[0]['type']=='retweeted' else False
                is_reply = True if referenced_tweets and referenced_tweets[0]['type']=='replied_to' else False
                is_quote = True if referenced_tweets and referenced_tweets[0]['type']=='quoted' else False
                if is_retweet: # unpack original tweet if current file is repost of an existing tweet
                    try:
                        tweet = d['includes']['tweets'][0]
                    except Exception as e:
                        if d['errors'][0]['parameter'] == 'referenced_tweets.id':
                            tweet = d['data']
                        else:
                            raise 

                else:
                    tweet = d['data']
                
                lang = tweet['lang']
                text = re.sub(r'\s+', ' ', tweet['text']) # remove multiple spaces with one
                created_at = datetime.strptime(tweet['created_at'], timeformat_string)
                metrics = tweet['public_metrics']
                retweet_count = metrics['retweet_count']
                reply_count = metrics['reply_count']
                like_count = metrics['like_count']
                quote_count = metrics['quote_count']
                source = tweet.get('source')
                geo = tweet['geo']
                is_sensitive = True if tweet['possibly_sensitive'] == 'true' else False
                reply_settings = tweet['reply_settings']

    
                params.append((is_retweet, is_reply,is_quote,str(referenced_tweets), lang, text, created_at, retweet_count, reply_count,
                like_count, quote_count, source, str(geo), is_sensitive, reply_settings, str(rule), datetime.now(),
                'do_something.extract_tweets', r['name']))
                c+=1
            except Exception as e:
                logging.exception((r, e))
                exit()
        query = "UPDATE files SET is_retweet=%s,is_reply=%s,is_quote=%s,referenced_tweets=%s,lang=%s,text=%s,created_at=%s,retweet_count=%s,reply_count=%s,like_count=%s,quote_count=%s,source=%s,geo=%s,is_sensitive=%s,reply_settings=%s,rule=%s,modified_on=%s,modified_by=%s WHERE name = %s;" 
        cursor.executemany(query, params)
        conn.commit()
        avg=int(c/(time()-t0+0.0001))
        time_left = (total_files / avg)/3600
        logging.info(f'{c} files done @ {avg} files/sec Time left: {time_left:.02f} hrs')
    conn.commit()
    conn.close()



def is_retweet(path):    
    try:
        fh = open(path, 'r') 
        d = json.load(fh)['data']
        if len(d['referenced_tweets']):
            return (True,str(d['referenced_tweets']))
    except KeyError as e:
        return (False,None)
    except Exception as e:
        logging.exception(e)
    finally:
        fh.close()
    

if __name__=="__main__":
    extract_tweets()
    #sync_download_path('/mnt/data/o365/tweets')
