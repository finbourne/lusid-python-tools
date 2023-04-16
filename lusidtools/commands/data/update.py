import sys
import requests
import pandas as pd
import numpy as np
import pickle
import shutil

# This script will lookup new fx rates and save them
# in a file called new_rates.csv
# Manually validate the file, then replace the existing
# rates.csv file

CCY = "ccy"
DATE = "date"
RATE = "rate"


def get_rates(code, ccy, start):
    response = requests.get(
        "https://www.quandl.com/api/v3/datasets/BOE/{}?start_date={}&api_key=6dzRn58ssvhXUErUHaYA".format(
            code, start
        )
    )
    try:
        data = response.json()

        df = pd.DataFrame.from_records(data["dataset"]["data"], columns=[DATE, RATE])

        df = df[df[DATE] > start].sort_values(DATE)
        df[RATE] = 1 / df[RATE]
        df[CCY] = ccy
        return df
    except:
        print(response.text)
        print("WHILE ATTEMPTING", code, ccy, start)
        exit()


codes = pd.read_csv("codes.csv")
rates = pd.read_csv("rates.csv")
last_date = rates[DATE].max()

last_rates = rates[rates[DATE] == last_date]

l = []
for idx, row in codes.iterrows():
    l.append((row[CCY], get_rates(row["code"], row[CCY], last_date)))

dates = (
    pd.concat([v[1] for v in l], ignore_index=True, sort=True)[[DATE]]
    .drop_duplicates()
    .sort_values(DATE)
    .reset_index(drop=True)
)


def expand(ccy, f):
    m = dates.merge(f, how="left", on=DATE)
    if np.isnan(m.at[0, RATE]):
        rt = last_rates[last_rates[CCY] == ccy][RATE].values[0]
        m.at[0, RATE] = rt
        m.at[0, CCY] = ccy
    return m.fillna(method="ffill")


full = [expand(ccy, df) for ccy, df in l]

# Create USD records

dates[CCY] = "USD"
dates[RATE] = 1.0
full.append(dates)

new_rates = pd.concat(full, ignore_index=True, sort=True).sort_values([DATE, CCY])


shutil.copyfile("rates.csv", "new_rates.csv")
new_rates.to_csv("new_rates.csv", mode="a", header=False, index=False)
