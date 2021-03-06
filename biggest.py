import pymongo
import json
import redis
import pandas as pd
from pymongo import MongoClient
import sched, time
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import redis

def find_btc(sc):
    df = pd.DataFrame(columns=['Hash', 'Time', 'Amount (BTC)', 'Amount (USD)'])
    url = 'https://www.blockchain.com/btc/unconfirmed-transactions'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="html.parser")
    divs = soup.find_all('div',{ "class" : "sc-1g6z4xm-0 arCxa" })

    for div in divs:
        div = div.getText()
        split1 = div.split("Hash")
        split2 = split1[1].split("Time")
        has = split2[0]
        split3 = split2[1].split("Amount (BTC)")
        time = split3[0]
        split4 = split3[1].split("Amount (USD)")
        btc = split4[0]
        usd = split4[1]
        for i in range(i+4):
            df.loc[i] = [has]+[time]+[btc]+[usd]
        i += 4

    redis.mset({"Hash": df["Hash"], "Time": df["Time"], "Amount (BTC)": df["Amount (BTC)"], "Amount (USD)": df["Amount (USD)"]})
    s.enter(60, 1, find_biggest, (sc,))
    
redis = redis.Redis(host='redis', port=6379)
s = sched.scheduler(time.time, time.sleep)
s.enter(60, 1, find_btc, (s,))
s.run()

has = redis.get("Hash")
time = redis.get("Time")
btc = redis.get("Amount (BTC)")
usd = redis.get("Amount (USD)")
df = pd.DataFrame({"Hash": has, "Time": time, "Amount (BTC)": btc, "Amount (USD)": usd})
id = df["Amount (BTC)"].idxmax()
hasmongo = df.loc[id, "Hash"]
timemongo = df.loc[id, "Time"]
btcmongo = df.loc[id, "Amount (BTC)"]
usdmongo = df.loc[id, "Amount (USD)"]

client = MongoClient("mongodb://localhost:27017/")
db = client["bitcoin"]
col_hash = local_database["hash"]
myhash = {"hash": hasmongo, "time": timemongo, "btc": btcmongo, "usd": usdmongo}
col_hash.insert_one(myhash)