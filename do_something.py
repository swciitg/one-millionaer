#!/mnt/data/projects/million-downloader/.venv/bin/python
# a script to do a task on data files

import os
import pymysql
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from time import time, sleep
import re
from functools import partial
from multiprocessing import Pool, current_process

load_dotenv()

connection = dict(host="localhost",
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'), cursorclass=pymysql.cursors.DictCursor)

logging_format ='%(asctime)s - %(levelname)s - %(message)s' 
logging.basicConfig(filename='/var/log/millionaer/do_something.txt', level=logging.INFO, format=logging_format)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter(logging_format)
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)


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

def read_json(path):    
    '''
    Reads json file from the given path and returns a dict object.
    '''
    f = open(path, 'r')
    d = json.load(f)
    f.close()
    return d

def get_features(r):
    '''
    Extract required features from dict object after reading the desired json file
    '''
    logging.debug((current_process(), r['name']))
    features = dict(name=r['name'])
    try:
        d = read_json(os.path.join(r['download_path'], r['name']))

        referenced_tweets = d['data'].get('referenced_tweets') 
        features['referenced_tweets'] = str(referenced_tweets)
        rule = d['matching_rules'][0]
        features['rule'] = str(rule)
        is_retweet = True if referenced_tweets and referenced_tweets[0]['type']=='retweeted' else False
        features['is_retweet'] = is_retweet
        features['is_reply'] = True if referenced_tweets and referenced_tweets[0]['type']=='replied_to' else False
        features['is_quote'] = True if referenced_tweets and referenced_tweets[0]['type']=='quoted' else False
        if is_retweet: # unpack original tweet if current file is repost of an existing tweet
            try:
                tweet = d['includes']['tweets'][0]
            except Exception as e:
                if d['errors'][0]['parameter'] == 'referenced_tweets.id':
                    tweet = d['data']
                else:
                    raise # this should not happen

        else:
            tweet = d['data']
        
        features['lang'] = tweet['lang']
        features['text'] = re.sub(r'\s+', ' ', tweet['text']) # remove multiple spaces with one
        timeformat_string = '%Y-%m-%dT%H:%M:%S.%fZ'
        features['created_at'] = datetime.strptime(tweet['created_at'], timeformat_string)
        metrics = tweet['public_metrics']
        features['retweet_count'] = metrics['retweet_count']
        features['reply_count'] = metrics['reply_count']
        features['like_count'] = metrics['like_count']
        features['quote_count'] = metrics['quote_count']
        features['source'] = tweet.get('source')
        geo = tweet['geo']
        features['geo'] = str(geo)
        features['is_sensitive'] = True if tweet['possibly_sensitive'] == 'true' else False
        features['reply_settings'] = tweet['reply_settings']
        features['modified_on']  = datetime.now()
        features['modified_by'] = 'do_something.extract_tweets'
        
        #return (is_retweet, is_reply,is_quote,str(referenced_tweets), lang, text, created_at, retweet_count,reply_count, like_count, quote_count, source, str(geo), is_sensitive, reply_settings, str(rule), datetime.now(), 'do_something.extract_tweets', r['name'])

    except FileNotFoundError as e:
        features['download_path'] = None
        features['downloaded'] = 0
        logging.exception(e)
        #q = f'update files set download_path=null, downloaded=0 where id={r["id"]}'        
        #logging.warning(q)
        #cursor.execute(q)                
    except Exception as e:
        logging.exception(e)
        features['skip'] = 1
        #logging.warning((r, str(e)))
        #q = f'update files set skip=1 where id={r["id"]}'        
        #cursor.execute(q)                
    return features


def unpack_replies_and_quotes(r):
    '''
    Extract required features from dict object after reading the desired json file
    '''
    logging.debug((current_process(), r['name']))
    tweets = list()
    try:
        d = read_json(os.path.join(r['download_path'], r['name']))
        for i,tweet in enumerate(d['includes']['tweets']):        
            features = dict(name=r['name']+str(i))
            features['tweet_id'] = tweet['id']
            features['referenced_tweets_type'] = 'unpacked'
            features['referenced_tweets'] = r['name']
            features['is_retweet'] = False
            features['is_reply'] = False
            features['is_quote'] = False
            features['lang'] = tweet['lang']
            features['text'] = re.sub(r'\s+', ' ', tweet['text']) # remove multiple spaces with one
            timeformat_string = '%Y-%m-%dT%H:%M:%S.%fZ'
            features['created_at'] = datetime.strptime(tweet['created_at'], timeformat_string)
            metrics = tweet['public_metrics']
            features['retweet_count'] = metrics['retweet_count']
            features['reply_count'] = metrics['reply_count']
            features['like_count'] = metrics['like_count']
            features['quote_count'] = metrics['quote_count']
            features['source'] = tweet.get('source')
            geo = tweet['geo']
            features['geo'] = str(geo)
            features['is_sensitive'] = True if tweet['possibly_sensitive'] == 'true' else False
            features['reply_settings'] = tweet['reply_settings']
            features['modified_on']  = datetime.now()
            features['modified_by'] = 'do_something.unpack_replies_and_quotes'
            tweets.append(features)
        
        return tweets
    except Exception as e:
        conn = pymysql.connect(**connection)
        cursor = conn.cursor()
        q = f'update files set skip=1 where id={r["id"]}'        
        cursor.execute(q)                
        conn.commit()
        conn.close()
        return list()
 


def insert_into_db(data):
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()

    for row in data:
        for tweet in row:
            k = ', '.join([key for key in tweet.keys()])
            v = ', '.join(['%s' for v in tweet.values()])
            q = f"insert into files ({k}) values ({v})"
            # Executing dynamic update query
            cursor.execute(q, tuple(tweet.get(c) for c in tweet.keys()))
            q = f'update files set processed=1 where name=%s'        
            cursor.execute(q, row[0]['referenced_tweets'])

    conn.commit()
    conn.close()
    return len(data)


def commit_batch(data):
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()

    for row in data:
        columns = ', '.join([f"{key}=%s" for key in row.keys() if key != 'name'])
        q = f"UPDATE files SET {columns} WHERE name=%s"
        # Executing dynamic update query
        cursor.execute(q, tuple(row.get(col) for col in row.keys() if col != 'name') + (row['name'],))

    conn.commit()
    conn.close()
    return len(data)

def remaining_files():
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()
    threads=int(os.getenv('THREADS'))
    batch_size=int(os.getenv('BATCH_SIZE'))
    t0 = time()
    c = 0
    cursor.execute("select count(*) as count from files where processed=0")
    files_left = int(cursor.fetchone()['count'])    
    conn.close()
    return files_left

def files_to_be_processed(q):
    conn = pymysql.connect(**connection)
    cursor = conn.cursor()
    cursor.execute(q)
    rows = cursor.fetchall()
    conn.close()
    return rows



def extract_data_from_files():

    # todo:de-dup replied tweets with their parent tweets if they already exist in db
    # todo:extract user info
 
    threads=int(os.getenv('THREADS'))
    batch_size=int(os.getenv('BATCH_SIZE'))
    t0 = time()
    c = 0
    files_left = remaining_files()

    with Pool(processes=threads) as pool:
        while True:
            try:
                logging.info('fetching files list...')
                #q = f"select * from files where isnull(lang) and skip=0 limit {batch_size}"
                q = f"select * from files where processed=0 and (is_reply=1 or is_quote=1) and skip=0 limit {batch_size}"
                rows = files_to_be_processed(q)
                if not rows:
                    logging.info('no more files to process')
                    break

                #features = pool.map(partial(get_features, cursor), rows)
                logging.info('working on files...')
                features = pool.map(unpack_replies_and_quotes, rows)
                u = insert_into_db(features)
                c += u

                avg=int(c/(time()-t0+0.0001))
                time_left = (files_left / (avg+0.0001))/3600
                logging.info(f'{files_left-c} files left @ {avg} files/sec Time left: {time_left:.02f} hrs')

            except Exception as e:
                logging.exception(e)
                sleep(60*5)
                


if __name__=="__main__":
    extract_data_from_files()
    #sync_download_path('/mnt/data/o365/tweets')
