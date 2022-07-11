import pymssql
import pandas as pd
from google.cloud import bigquery
import os
from google.oauth2 import service_account
import re

credentials = service_account.Credentials.from_service_account_file(
    "cred.json")
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "cred.json"

Server = 'hscdbserver757.database.windows.net'
Database = 'Artem3010'
Username = 'Artem0899'
Password = 'mDyqLMj9&IhiAOU9'

conn = pymssql.connect(Server, Username, Password, Database)
cursor = conn.cursor(as_dict=True)


def query():
    cursor.execute('''
    SELECT amo_id,
           VId,
           cast(createdate  AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time'  as datetime) AS created_at,
           cast(lastmodifieddate  AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time'  as datetime) AS updated_at,
           o.hubspot_owner_id AS responsible_user_id,
           concat(c.firstname, ' ', c.lastname) AS name,
           phone,
           c.email AS primary_email,
           timezone,
           country -- (США / Unites States)
    region,
                                           state AS city,
                                           utm_source AS contact_source,
                                           utm_medium AS contact_medium,
                                           utm_campaign AS contact_campaign,
                                           utm_content AS contact_content,
                                           utm_term AS contact_term
    FROM Contact c
    LEFT JOIN OWNER o ON o.hubspot_owner_id = c.hubspot_owner_id  ''')

    data = cursor.fetchall()
    df = pd.DataFrame(data)

    df_hs = df[~df.VId.isna()].drop(['amo_id'], axis = 1)
    df_hs = df_hs.rename(columns={'VId':'contact_id'})
    df_hs['crm'] = 'hs'

    df_amo = df[~df.amo_id.isna()].drop(['VId'], axis = 1)
    df_amo = df_amo.rename(columns={'amo_id':'contact_id'})
    df_amo['crm'] = 'amo'

    df = pd.concat([df_hs, df_amo]).sort_values(by = 'primary_email', ascending= True)\
        .drop_duplicates(subset = ['contact_id', 'primary_email', 'crm'], keep = 'first')

    conn.close()
    return df


def fill_missing_values(arg):
    df = arg.fillna(
        {'amo_id': -1, 'responsible_user_id': -1, 'name': -1, 'phone': -1, 'primary_email': -1, 'timezone': -1,
         'region': -1,
         'city': -1, 'contact_source': -1, 'contact_medium': -1, 'contact_campaign': -1, 'contact_content': -1,
         'contact_term': -1})
    return df


def change_type(arg):
    df = arg.astype(
        {'contact_id': int, 'amo_id': int, 'responsible_user_id': int, 'name': str, 'phone': str, 'primary_email': str,
         'timezone': str,
         'region': str, 'city': str, 'contact_source': str, 'contact_medium': str, 'contact_campaign': str,
         'contact_content': str,
         'contact_term': str})
    return df


def transform(arg):
    df = arg.assign(phone=[*map(lambda x: re.sub('\D', '', x), arg.phone)])
    df = df.assign(region=df.region.str.replace('США', 'Unites States'))
    return df


def load(arg):
    client = bigquery.Client(project='fabled-sorter-289010')
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", )
    table = 'Renta_dataset.full_contacts_data'
    job = client.load_table_from_dataframe(arg, table, job_config=job_config)


if __name__ == "__main__":
    load(transform(change_type(fill_missing_values(query()))))
