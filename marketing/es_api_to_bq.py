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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
gc = gs.service_account(filename="/fabled-sorter-289010-9aef64e16ce3.json")
credentials = service_account.Credentials.from_service_account_file("/fabled-sorter-289010-9aef64e16ce3.json")

soup = BeautifulSoup(requests.get("https://api7.esv2.com/v2/Api/SubscriberStatistics?apiKey=p5qnbI5yxR2n7aGOf5yB&grouping=List").content, "html.parser")

list_id = []
for i in soup.find_all('listid'):
    list_id.append(i.text)
    
list_name = []
for i in soup.find_all('listname'):
    list_name.append(i.text)    
    
    
list_size = []
for i in soup.find_all('listsize')[1:]:
    list_size.append(i.text)    

df = pd.DataFrame(list(zip(list_id, list_name, list_size)),
               columns =['id', 'name', 'size'])

df = df.query('name.str.contains("webinar")').query('~name.str.contains("old")')
df['cond'] = df.name.apply(lambda x : 1 if  str(x)[-6:].isnumeric() & (str(x)[-7:].isnumeric() == False)  else 0 )
df = df[df.cond == 1]
df['name'] = df.name.apply(lambda x : x.replace('webinar_mqa_291121','webinar_mqa_301121'))
df['web_date'] = df.name.apply(lambda x : str(x)[-6:])
df['date'] = df.web_date.apply(lambda x: datetime.strptime(x, '%d%m%y'))
df = df[df.date >= '2021-10-01']


client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.expertsender'
#job_config.write_disposition = 'WRITE_APPEND' 

# Load data to BQ
job = client.load_table_from_dataframe(df, table, job_config = job_config)
