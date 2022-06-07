# bigdataapi@fabled-sorter-289010.iam.gserviceaccount.com
import gspread as gs
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
import matplotlib.pyplot as plt
from google.cloud import bigquery
import os
import numpy

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
gc = gs.service_account(filename=
                        "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
                        )
credentials = service_account.Credentials.from_service_account_file(
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
)

links = ['https://docs.google.com/spreadsheets/d/1cDWT9rodZMmIFaezb7fowdrIT-q2Sy8vylzb28hU-_8/edit#gid=585272677',
         'https://docs.google.com/spreadsheets/d/1gXmJeKKFEVG4QisALAMuU9bopoEZcABN55ikQnCssnw/edit#gid=43497635',
         'https://docs.google.com/spreadsheets/d/1cf0uf9q03D0kuEvBYALItSi8OJR6ssQKLfQReLsF2v0/edit#gid=1378333417',
         'https://docs.google.com/spreadsheets/d/19LXa7wQHzmsnU0KjVycOAr2fNFJIyLImD-n0v82vFaQ/edit#gid=167502289',
         'https://docs.google.com/spreadsheets/d/1XLb8mg_Dbh8HIo3G7NXNXBnxanmKqdqjsWSWbdWsHm4/edit?usp=sharing',
         'https://docs.google.com/spreadsheets/d/1lasAiaakFhGPSoTYQqerPqctZLKACrnMot1xoRA1wv8/edit#gid=703122150',
         'https://docs.google.com/spreadsheets/d/1dI38g3LXJ2seWYvj2QfTT1rut3a47ux7tahOcun_8Js/edit#gid=2039768836',
         'https://docs.google.com/spreadsheets/d/15fAstVcvLbJaTiZOVXO296THY0iYJF459C4uglR26_Y/edit?pli=1#gid=8993578',
         'https://docs.google.com/spreadsheets/d/1NQrrgEwa-bJjuMBo9XZlu-PnyjqXy8tadp-rJ5FbXPg/edit#gid=1762494399',
         'https://docs.google.com/spreadsheets/d/1CpJ6vF4IXjxkrg5tg8Gv_TW3-wG1K6zYvyxUXrXCEJA/edit#gid=1089125699',
         'https://docs.google.com/spreadsheets/d/1T2wsJvNFr52A5uXsI0eCe3rX_v8wR7CJIl5x08LNo6I/edit?usp=sharing',
         'https://docs.google.com/spreadsheets/d/1ABgmNkbX1NlUSm8wuFH0IaMOgDbM6Jrpug09l846r7M/edit?usp=sharing',
         'https://docs.google.com/spreadsheets/d/1ghhJYzitT0vVf5bjwuvbyFnzn7mboOJ70ETiOKgsVsQ/edit?usp=sharing']

df_new = pd.DataFrame(columns=['webinar-email', 'sent', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_term'])
for i in links:
    sh = gc.open_by_url(i)
    ws = sh.worksheet('LeadsFromTilda')
    df = pd.DataFrame(ws.get_all_records())
    try:
        df = df[['webinar-email', 'sent', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_term']]
    except KeyError:
        continue
    df_new = df_new.append(df, ignore_index=True)
df_new = df_new.rename(columns={'webinar-email': 'email'})
df_new = df_new.query('utm_source == "facebook"')
# для заполнения пропусков 
# df_new[df_new.sent == ''] = df_new[df_new.sent == ''].replace('', df_new[df_new.sent != ''].sent[1])
df_new['sent'] = df_new.sent.apply(lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S')))
df_new = df_new.drop_duplicates(['email'.lower(), 'utm_campaign'.lower()], keep="first")

query = '''
            select max(sent) as sent from `fabled-sorter-289010.Renta_dataset.tilda`
        '''
project_id = 'fabled-sorter-289010'
df = pd.read_gbq(query, project_id=project_id, credentials=credentials)
df_max = pd.Timestamp(numpy.datetime64(df.sent.max()))

df_new = df_new[df_new.sent > df_max]

client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.tilda'
# job_config.write_disposition = 'WRITE_APPEND'

# Load data to BQ
job = client.load_table_from_dataframe(df_new, table, job_config=job_config)
