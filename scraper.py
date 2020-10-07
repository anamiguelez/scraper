from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import requests
import re
import sched, time
import pandas as pd
s = sched.scheduler(time.time, time.sleep)

def find_biggest(sc):
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

    print(df)

    s.enter(60, 1, find_biggest, (sc,))

s.enter(60, 1, find_biggest, (s,))
s.run()
