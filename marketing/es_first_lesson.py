import bs4
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import gspread as gs
import numpy

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
gc = gs.service_account(filename=
                        "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json")
credentials = service_account.Credentials.from_service_account_file("/fabled-sorter-289010-9aef64e16ce3.json")

soup = BeautifulSoup(requests.get("https://api7.esv2.com/v2/Api/SubscriberStatistics?"
                                  "apiKey=p5qnbI5yxR2n7aGOf5yB&grouping=List").content, "html.parser")


def stats(list_name, parameter):
    list_name = [i.text for i in soup.find_all(parameter)]
    return list_name


list_id = stats('list_id', 'listid')
list_name = stats('list_name', 'listname')
list_size = stats('list_size', 'listsize')[1:]


def table_preparation():
    df = pd.DataFrame([*zip(list_id, list_name, list_size)], columns=['id', 'name', 'size'])
    df = df.query('name.str.contains("first")')
    df['web_date'] = df.apply(lambda x: str(x['name'])[-6:], axis=1)
    df = df[df.web_date.str.isnumeric()]
    df['web_date'] = df.web_date.apply(lambda x: datetime.strptime(x, '%d%m%y'))
    df = df.assign(course=[*map(lambda x: x.strip().split('_')[0], df['name'])])
    df['size'] = df['size'].astype('int')
    df = df.groupby(['course', 'web_date'])['size'].sum().reset_index().sort_values(by='web_date', ascending=False)
    df['web_date'] = df.web_date.astype('str')
    return df


df = table_preparation()

client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.expertsender_firstlesson'
# job_config.write_disposition = 'WRITE_APPEND'

# Load data to BQ
job = client.load_table_from_dataframe(df, table, job_config=job_config)
