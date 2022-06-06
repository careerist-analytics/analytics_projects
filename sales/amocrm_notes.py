import os
import time
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd


project_id = 'fabled-sorter-289010'
credentials = service_account.Credentials.from_service_account_file("/fabled-sorter-289010-9aef64e16ce3.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"
client = bigquery.Client(project='fabled-sorter-289010')

now = round(time.time(),)

query = ' select max(updated_at) as max_update from `fabled-sorter-289010.Renta_dataset.amoCRM_note` ' \
        'where cast(updated_at as decimal) < {}'.format(now)
df = pd.read_gbq(query, project_id=project_id, credentials=credentials)
max_bq = df.max_update[0]

engine = create_engine('mysql+pymysql://readonly:readonly@167.172.135.185:3306/careerist_careerist')
created_new = pd.read_sql('select * from amocrmreplica_Note where created_at > {}  and created_at< {}'.format(max_bq, now), engine)

df = created_new.astype({'message_uuid':str , 'params': str, 'from_user_id': int}, errors= 'ignore' )

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",)
table = 'Renta_dataset.amoCRM_note'
job = client.load_table_from_dataframe(df, table, job_config = job_config)