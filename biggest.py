import pymongo
import json
import redis
import pandas as pd
from pymongo import MongoClient

r = redis.Redis()
has = r.get("Hash")
time = r.get("Time")
btc = r.get("Amount (BTC)")
usd = r.get("Amount (USD)")
df = pd.DataFrame(["Hash": has, "Time": time, "Amount (BTC)": btc, "Amount (USD)": usd])
id = df["Amount (BTC)"].idxmax()
hasmongo = df.loc[id, "Hash"]
timemongo = df.loc[id, "Time"]
btcmongo = df.loc[id, "Amount (BTC)"]
usdmongo = df.loc[id, "Amount (USD)"]

client = MongoClient("mongodb://localhost:27018/")
db = client["bitcoin"]
col_hash = local_database["hash"]
myhash = {"hash": hasmongo, "time": timemongo, "btc": btcmongo, "usd": usdmongo}
col_hash.insert_one(myhash)