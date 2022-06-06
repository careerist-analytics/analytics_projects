import bs4
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import gspread as gs
import numpy 


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
gc = gs.service_account(filename="/fabled-sorter-289010-9aef64e16ce3.json")
credentials = service_account.Credentials.from_service_account_file("/fabled-sorter-289010-9aef64e16ce3.json")

today = date.today().strftime("%Y-%m-%d")

soup = BeautifulSoup(requests.get("https://api7.esv2.com/v2/Api/SummaryStatistics?apiKey=p5qnbI5yxR2n7aGOf5yB &startDate=2021-01-01&endDate="+today+"&grouping=Date").content, "html.parser")

def stats(list_name):
    list_name = [i.text for i in soup.find_all(list_name)]
    return list_name

date = stats('date')
sent = stats('sent')
bounced = stats('bounced')
delivered = stats('delivered')
opens = stats('opens')
uniqueopens = stats('uniqueopens')
clicks = stats('clicks')
uniqueclicks = stats('uniqueclicks')
clickers = stats('clickers')
complaints = stats('complaints')
unsubscribes = stats('unsubscribes')
goals = stats('goals')
goalsvalue = stats('goalsvalue')

df = pd.DataFrame(list(zip(date, sent[1:], bounced[1:], delivered[1:], opens[1:], uniqueopens[1:], clicks[1:],                     uniqueclicks[1:], clickers[1:], complaints[1:], unsubscribes[1:], goals[1:], goalsvalue[1:])),
               columns =['date', 'sent', 'bounced','delivered','opens','uniqueopens','clicks', \
                        'uniqueclicks', 'clickers', 'complaints', 'unsubscribes', 'goals', 'goalsvalue'])

df = df.assign(date = [*map(lambda x : pd.to_datetime(x, format = '%Y-%m-%d'), df['date'])] )


client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.expertsender_stats'
#job_config.write_disposition = 'WRITE_APPEND' 

# Load data to BQ
job = client.load_table_from_dataframe(df, table, job_config = job_config)





