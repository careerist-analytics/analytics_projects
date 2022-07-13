import pandas as pd
import pymssql
import re
import sys

from google.oauth2 import service_account
from google.cloud import bigquery

PROJECT_ID = 'fabled-sorter-289010'
CREDENTIALS = service_account.Credentials.from_service_account_file(
    'fabled-sorter-289010-9aef64e16ce3.json')

SERVER = 'hscdbserver757.database.windows.net'
DB = 'Artem3010'
USERNAME = 'Artem0899'
PASSWORD = 'mDyqLMj9&IhiAOU9'

def get_data(query, index_col=None, project_id=PROJECT_ID, credentials=CREDENTIALS, rename_cols={}):
    return pd.read_gbq(query, project_id=project_id, credentials=credentials, index_col=index_col).rename(
        columns=rename_cols)

def get_hs_data(query):
    conn = pymssql.connect(SERVER, USERNAME, PASSWORD, DB)
    cursor = conn.cursor(as_dict=True)

    cursor.execute(query)

    data = cursor.fetchall()
    df = pd.DataFrame(data)

    conn.close()
    return df

def has_digits(inputString):
    return any(char.isdigit() for char in inputString)

def format_col(col, pipelines):
    formatted_col = []
    x = re.split('(\d+)', col.strip())[:2]
    pipe_name = pipelines[pipelines['stage_id'] == int(x[-1])]['pipeline_name'].values[0]

    if pipe_name == 'Main pipeline #1':
        pipe_name = "main_"
    elif pipe_name == 'Webinar pipeline':
        pipe_name = "webinar_"
    elif pipe_name == 'Job Application Service':
        pipe_name = "jas_"

    formatted_col.append(x[0])
    formatted_col.append(pipe_name)
    formatted_col.append(
        pipelines[pipelines['stage_id'] == int(x[-1])]['stage_name'].values[0].replace(" ", "_").lower())

    return "".join(formatted_col)

def upload_table_bigquery(df, table_location):
    try:
        credentials = service_account.Credentials.from_service_account_file('fabled-sorter-289010-9aef64e16ce3.json')
        client = bigquery.Client(project='fabled-sorter-289010', credentials=CREDENTIALS)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        job = client.load_table_from_dataframe(df, table_location, job_config=job_config)

        print("✅ Successfuly uploaded")
    except Exception as error:
        print('⛔️ Error: ' + repr(error))
        sys.exit()

COLS_TO_SKIP = [
    'hs_date_entered_appointmentscheduled', 'hs_date_entered_closedlost', 'hs_date_entered_closedwon',
    'hs_date_entered_contractsent', 'hs_date_entered_decisionmakerboughtin', 'hs_date_entered_presentationscheduled',
    'hs_date_entered_qualifiedtobuy', 'hs_date_exited_appointmentscheduled', 'hs_date_exited_closedlost',
    'hs_date_exited_closedwon', 'hs_date_exited_contractsent', 'hs_date_exited_decisionmakerboughtin',
    'hs_date_exited_presentationscheduled', 'hs_date_exited_qualifiedtobuy', 'hs_lastmodifieddate',
    'hs_time_in_appointmentscheduled', 'hs_time_in_closedlost', 'hs_time_in_closedwon', 'hs_time_in_contractsent',
    'hs_time_in_decisionmakerboughtin', 'hs_time_in_presentationscheduled', 'hs_time_in_qualifiedtobuy'
]

if __name__ == "__main__":
    notes = get_hs_data('''select * from DealHistory''')
    pipes = get_data('''
        select pipeline_name, stage_id, stage_name from Renta_dataset.pipelines
        where crm = 'hs'
    ''')

    notes.columns = [format_col(col, pipes) if has_digits(col) else col.lower() for col in notes.columns]
    notes = notes.loc[:, ~notes.columns.isin(COLS_TO_SKIP)]

    for col in notes.columns:
        if "time_in" in col:
            notes.loc[:, col] = notes.loc[:, col].apply(lambda x: float(round(x)) if x else -1)

    notes.loc[:, 'dealid'] = notes.loc[:, 'dealid'].apply(lambda x: float(round(x)) if x else x)
    upload_table_bigquery(notes, "Renta_dataset.hs_deal_history")
