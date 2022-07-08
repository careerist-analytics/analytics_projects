import pymssql
import pandas as pd
from google.cloud import bigquery
import os
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file \
    ("/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"

Server = 'hscdbserver757.database.windows.net'
Database = 'Artem3010'
Username = 'Artem0899'
Password = 'mDyqLMj9&IhiAOU9'

conn = pymssql.connect(Server, Username, Password, Database)
cursor = conn.cursor(as_dict=True)


def delete_rows():
    query = '''DELETE FROM Renta_dataset.pipelines  WHERE crm = 'hs' '''
    project_id = 'fabled-sorter-289010'
    pd.read_gbq(query, project_id=project_id, credentials=credentials)


def query():
    cursor.execute('''
  select
    'hs' as crm,
    pipes.PipelineId as pipeline_id,
    pipes.PipelineLabel as pipeline_name,
    stages.DealStage as stage_id,
    stages.StageLabel as stage_name
  from DealPipeline pipes
  left join DealPipelineStage stages on pipes.PipelineId = stages.PipelineId ''')

    data = cursor.fetchall()
    df = pd.DataFrame(data)
    conn.close()
    return df


def types(arg):
    df = arg.astype({'crm': str, 'pipeline_id': int, 'pipeline_name': str, 'stage_id': int, 'stage_name': str})
    return df


def load(arg):
    client = bigquery.Client(project='fabled-sorter-289010')
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", )
    table = 'Renta_dataset.pipelines'
    job = client.load_table_from_dataframe(arg, table, job_config=job_config)


if __name__ == "__main__":
    delete_rows()
    load(types(query()))
