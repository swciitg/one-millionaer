# Credits:
#   https://github.com/AkmalAbbas/Roman_Urdu_Hate_Speech_Classification

import tensorflow as tf
import pandas as pd
from gensim.parsing.preprocessing import STOPWORDS
import nltk
from nltk.tokenize import word_tokenize

import re # Regular Expression
from bs4 import BeautifulSoup # To remove html tags
import unicodedata # To remove accented characters

from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences

from fastapi import FastAPI

model = tf.keras.models.load_model('roman_urdu_hate_speeh_lstm_akmal_abbas.keras')
nltk.download('punkt')

stopwords_roman_urdu = { "roman_urdu_stop_words":
    [
        "ai", "ayi", "hy", "hai", "main", "ki", "tha", "koi", "ko", "sy", "woh",
        "bhi", "aur", "wo", "yeh", "rha", "hota", "ho", "ga", "ka", "le", "lye",
        "kr", "kar", "lye", "liye", "hotay", "waisay", "gya", "gaya", "kch", "ab",
        "thy", "thay", "houn", "hain", "han", "to", "is", "hi", "jo", "kya", "thi",
        "se", "pe", "phr", "wala", "waisay", "us", "na", "ny", "hun", "rha", "raha",
        "ja", "rahay", "abi", "uski", "ne", "haan", "acha", "nai", "sent", "photo",
        "you", "kafi", "gai", "rhy", "kuch", "jata", "aye", "ya", "dono", "hoa",
        "aese", "de", "wohi", "jati", "jb", "krta", "lg", "rahi", "hui", "karna",
        "krna", "gi", "hova", "yehi", "jana", "jye", "chal", "mil", "tu", "hum", "par",
        "hay", "kis", "sb", "gy", "dain", "krny", "tou"
    ]
}

ls_stopwords = stopwords_roman_urdu['roman_urdu_stop_words']
all_stopwords_gensim = STOPWORDS.union(set(ls_stopwords))

df_for_eda = pd.read_csv("Cleaned_data.csv")
text = df_for_eda['clean_tweet'].astype(str)
token = Tokenizer()
token.fit_on_texts(text)

def remove_stop_words(text):
  text_tokens = word_tokenize(text)
  tokens_without_sw = [word for word in text_tokens if not word in all_stopwords_gensim]
  tokens_without_sw = ' '.join(tokens_without_sw)
  return tokens_without_sw


def remove_emails(x):
	return re.sub(r'([a-z0-9+._-]+@[a-z0-9+._-]+\.[a-z0-9+_-]+)',"", x)

def remove_urls(x):
	return re.sub(r'(http|https|ftp|ssh)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?', '' , x)

def remove_html_tags(x):
	return BeautifulSoup(x, 'lxml').get_text().strip()

def remove_rt(x):
	return re.sub(r'\brt\b', '', x).strip()

def remove_accented_chars(x):
	x = unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8', 'ignore')
	return x

def remove_special_chars(x):
	x = re.sub(r'[^\w ]+', "", x)
	x = ' '.join(x.split())
	return x

def get_clean(x):
    x = str(x).lower().replace('\\', '').replace('_', ' ')
    x = remove_emails(x)
    x = remove_urls(x)
    x = remove_html_tags(x)
    x = remove_rt(x)
    x = remove_accented_chars(x)
    x = remove_special_chars(x)
    x = re.sub("(.)\\1{2,}", "\\1", x)
    return x


def custom_data_preprocess(x):
  custom_x = remove_stop_words(x)
  custom_x = get_clean(custom_x)
  custom_x = token.texts_to_sequences([custom_x])
  max_length=40
  custom_x = pad_sequences(custom_x,maxlen=max_length,padding="post")
  return custom_x

def predict_tweet_sentiment(x):
  vec = custom_data_preprocess(x)
  result = model.predict(vec)
  return result.flatten()[0]

'''
# tweet_id 155: ecnmc pwr
app = FastAPI()

@app.get('/')
def index():
    return {'status': 'ok'}

@app.get('/predict')
def predict(text):
    return {'probability' : str(predict_tweet_sentiment(text))}

'''
