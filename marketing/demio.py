import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
import numpy
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "/fabled-sorter-289010-9aef64e16ce3.json")
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"

headers = {
    'Api-Key': 'omRofLaicCGhMs93337TaMd1lZgukrPY',
    'Api-Secret': 'vjn0sul05V5q'
}

try:
    res = requests.get('https://my.demio.com/api/v1/events', headers=headers, timeout=5)
except requests.exceptions.HTTPError as errh:
    print("Http Error:", errh)
except requests.exceptions.ConnectionError as errc:
    print("Error Connecting:", errc)
except requests.exceptions.Timeout as errt:
    print("Timeout Error:", errt)
except requests.exceptions.RequestException as err:
    raise SystemExit(err)

x = res.json()
df = pd.DataFrame(x)

df['timestamp'] = df.iloc[:, 6].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
df = df.assign(timestamp=pd.to_datetime(df.timestamp, format=('%Y-%m-%d %H:%M:%S')) - timedelta(hours=3))
df = df.query('status == "finished"')[['date_id', 'name', 'status', 'timestamp']]
df = df.assign(event_type=[*map(lambda x: 'lesson' if 'FL' in x else 'webinar', df['name'])])


def dictionary(condition):
    result = {}
    for i in df.date_id[:]:
        res = requests.get('https://my.demio.com/api/v1/report/' + str(i) + '/participants?status=' + condition, \
                           headers=headers, timeout=5)
        t = res.json()
        result[i] = len(list(t.values())[0])
    return result


non_participating = dictionary('did-not-attend')
participants = dictionary('attended')


def col_creating(column):
    return df['date_id'].apply(lambda x: column[x])


df = df.assign(non_participating=col_creating(non_participating))
df = df.assign(participants=col_creating(participants))

df = df.assign(total=df.non_participating + df.participants)
df['cond'] = df.name.apply(lambda x: 1 if str(x)[-2:].isnumeric() & (str(x)[-3:].isnumeric() == False) else 0)
df = df[df.cond == 1]
df = df[df.timestamp >= '2021-11-01']
df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7]]

client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.demio'
# job_config.write_disposition = 'WRITE_APPEND'

# Load data to BQ
job = client.load_table_from_dataframe(df, table, job_config=job_config)