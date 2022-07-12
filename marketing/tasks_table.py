import pymssql
import pandas as pd
from google.cloud import bigquery
import os
from google.oauth2 import service_account
import re

credentials = service_account.Credentials.from_service_account_file(
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json")
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"

Server = 'hscdbserver757.database.windows.net'
Database = 'Artem3010'
Username = 'Artem0899'
Password = 'mDyqLMj9&IhiAOU9'

conn = pymssql.connect(Server, Username, Password, Database)
cursor = conn.cursor(as_dict=True)


def query():
    cursor.execute('''
    WITH pipelines AS
      (SELECT pipes.PipelineId AS pipeline_id,
              pipes.PipelineLabel AS pipeline_name,
              stages.DealStage AS stage_id,
              stages.StageLabel AS stage_name
       FROM DealPipeline pipes
       LEFT JOIN DealPipelineStage stages ON pipes.PipelineId = stages.PipelineId),
         leads AS
      (SELECT d.DealId AS element_id,
              dealname AS name,
              o.hubspot_owner_id AS responsible_user_id,
              pipelines.pipeline_name AS pipeline,
              course,
              pipelines.stage_name AS stage
       FROM Deal d
       LEFT JOIN pipelines ON d.pipeline = pipelines.pipeline_id
       AND d.dealstage = pipelines.stage_id
       LEFT JOIN OWNER o ON o.hubspot_owner_id = d.hubspot_owner_id
       LEFT JOIN DealContactAssociations c ON d.DealId = c.DealId),
         deals_tasks AS
      (SELECT a.DealId,
              a.EngagementId
       FROM
         (SELECT eng.*,
                 row_number() OVER (PARTITION BY EngagementId
                                    ORDER BY DealId) AS rn
          FROM EngagementDealAssociations eng) a
       WHERE a.rn = 1 )
    SELECT DISTINCT cast(eng.DealId AS float) AS element_id,
                    leads.pipeline,
                    course,
                    leads.name,
                    --eng.EngagementId,
                    cast(e.CreatedAt  AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time'  as datetime) AS created_at,
                    cast([TimeStamp]  AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time'  as datetime) AS complete_till_at,
                    cast(e.LastUpdated  AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time'  as datetime)  AS updated_at,
                    e.[Type] AS task_type,
                    BODY AS [text],
                            e.hubspot_owner_id AS responsible_user_id,
                            CASE
                                WHEN lower(Status) in ('completed',
                                                       'sent') THEN 1
                                ELSE 0
                            END AS is_completed,
                            CASE
                                WHEN datepart(MINUTE, [TimeStamp]) >= datepart(MINUTE, e.LastUpdated) THEN 'active'
                                WHEN datepart(MINUTE, e.LastUpdated) > datepart(MINUTE, [TimeStamp]) THEN 'expired'
                            END AS task_status
    FROM Engagement e
    LEFT JOIN deals_tasks eng ON e.EngagementId = eng.EngagementId
    LEFT JOIN leads ON eng.DealId = leads.element_id
     ''')

    data = cursor.fetchall()
    df = pd.DataFrame(data)

    conn.close()
    return df


def transform(arg):
    df = arg.query('created_at >= "2022-07-06 13:00:00"')
    df = df.assign(crm='hs')
    df = df.astype({'element_id': int}, errors='ignore')
    df['is_completed'] = df.is_completed.apply(lambda x: True if x == 1 else False)
    return df


def load(arg):
    client = bigquery.Client(project='fabled-sorter-289010')
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", )
    table = 'Renta_dataset.full_tasks_data'
    job = client.load_table_from_dataframe(arg, table, job_config=job_config)


if __name__ == '__main__':
    load(transform(query()))
