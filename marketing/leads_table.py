import datetime as datetime
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


def query():
    cursor.execute('''
    select
    d.createdate as date_created,
    d.closedate as date_closed,
    d.DealId as element_id,
    eda.VId  as contact_id,
    concat(c.firstname,' ', c.lastname) as name , --- тож самое что и deal в amo?
    o.hubspot_owner_id as responsible_user_id,
    pipeline,
    course,
    utm_source_lead as lead_source,
    utm_medium_lead as lead_medium,
    utm_campaign_lead as lead_campaign,
    utm_content_lead as lead_content,
    utm_term_lead as lead_term,
    source_key as source_key,
    dealstage as status_id,
    amount as sale,
    closed_lost_reason as loss_reason,
    contract_type as 'contract_type', --- или contract_type - тип контракта
    course_month as course_month,
    'hs' as crm
    from Deal d
    left join Owner o on o.hubspot_owner_id = d.hubspot_owner_id
    left join DealContactAssociations eda on eda.DealId = d.DealId
    left join Contact c on c.VId = eda.VId
    where d.createdate >= '2022-07-06 13:00:00'  ''')

    data = cursor.fetchall()
    df = pd.DataFrame(data)
    conn.close()
    return df


def delete_rows():
    query = '''
    DELETE FROM Renta_dataset.full_leads_data  WHERE date_created >= '2022-07-06 13:00:00' '''
    project_id = 'fabled-sorter-289010'
    pd.read_gbq(query, project_id=project_id, credentials=credentials)


def fill():
    df = query()
    df = df.fillna({'element_id': -1, 'contact_id': -1, 'name': -1,
                    'responsible_user_id': -1, 'pipeline': -1, 'course': -1, 'lead_source': -1,
                    'lead_medium': -1, 'lead_campaign': -1, 'lead_content': -1, 'lead_term': -1,
                    'source_key': -1, 'status_id': -1, 'sale': -1, 'loss_reason': -1, 'contract_type': -1,
                    'crm': -1})
    return df


def types():
    df = fill()
    df = df.astype({'element_id': float, 'contact_id': int, 'name': str, 'responsible_user_id': int, 'pipeline': str,
                    'course': str, 'lead_source': str, 'lead_medium': str, 'lead_campaign': str, 'lead_content': str,
                    'lead_term': str, 'source_key': str, 'status_id': int, 'sale': int, 'loss_reason': str,
                    'contract_type': str,
                    'crm': str})
    df = df.astype({'element_id': int})
    df['course_month'] = pd.to_datetime(df['course_month'], errors='coerce')
    return df


def load():
    df = types()
    client = bigquery.Client(project='fabled-sorter-289010')
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", )
    table = 'Renta_dataset.full_leads_data'
    job = client.load_table_from_dataframe(df, table, job_config=job_config)


if __name__ == "__main__":
    delete_rows()
    load()
