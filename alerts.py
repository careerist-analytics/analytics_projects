import smtplib
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys

credentials = service_account.Credentials.from_service_account_file(
    "/fabled-sorter-289010-9aef64e16ce3.json")
project_id = 'fabled-sorter-289010'
password = 'fuKgad-hocnub-hifza1'
email = 'leonid.parubets@careerist.com'

today = pd.to_datetime(date.today(), format="%Y-%m-%d")

tables = {'amoCRM_Leads_https_jobeasy_amocrm_ru_292285003': 'created_at',
          'fb_ads_Max_Gusakov_act_419399878644385_2066201980': 'date',
          'g_ads': 'segments_date',
          'biz_callrecord': 'createdAt',
          'biz_interactionevent': 'createdAt',
          'biz_dealinvoice': 'createdAt',
          'biz_interaction': 'createdAt',
          'amoCRM_note': 'PARSE_DATETIME("%Y-%m-%d %H:%M:%S",  date_create)',
          'amoCRM_contacts': 'TIMESTAMP_SECONDS(date_create)',
          'amoCRM_Tasks_jobeasy_327585854': 'created_at'
          }


def checklist(tables_dict):
    dates_list = {}
    for i, k in tables_dict.items():
        if i != 'amoCRM_note':
            query = ('''
                    select cast(max({}) as date) as max_date from `Renta_dataset.{}`
                     ''').format(k, i)
            df = pd.read_gbq(query, project_id=project_id, credentials=credentials)
            dates_list[i] = df.max_date.iloc[0]
        else:
            query = ('''
                    select cast(max({}) as date) as max_date from `Renta_dataset.{}`
                    where PARSE_DATETIME("%Y-%m-%d %H:%M:%S",  date_create) <= current_date()
                     ''').format(k, i, today)
            df = pd.read_gbq(query, project_id=project_id, credentials=credentials)
            dates_list[i] = df.max_date.iloc[0]
    return pd.DataFrame(dates_list.items(), columns=['table_name', 'mx_date'])


def table_creation():
    df = checklist(tables)
    df = df.assign(td=pd.to_datetime(date.today(), format="%Y-%m-%d"))
    df = df.assign(mx_date=pd.to_datetime(df.mx_date, format="%Y-%m-%d"))
    df['diff'] = (df.td - df.mx_date).dt.days
    df = df.assign(checkType=[*map(lambda x: 'successful' if x <= 1 else 'ALERT', df['diff'])])
    df = df.sort_values(by='diff', ascending=False)
    df = df.iloc[:, [0, 1, 2, 4]]
    return df


def send_email():
    recipients = ['leonid.parubets@careerist.com']
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = "Integrations {}".format(str(today).split(' ')[0])
    msg['From'] = 'leonid.parubets@careerist.com'

    html = """\
            <html>
              <head></head>
              <body>
                {0}
                <p><br></p>
                <p>If you see status = &apos;ALERT&apos; please visit&nbsp;
                <a href="https://app.renta.im/integrations/list">https://app.renta.im/integrations/list</a></p>
                <p><br></p>
    <p>Best regards,</p>
    <p>Analytics Careerist team</p>
              </body>
            </html>
    """.format(df.to_html(index=False))

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    try:
        """Checking for connection errors"""

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()  # NOT NECESSARY
        server.starttls()
        server.ehlo()  # NOT NECESSARY
        server.login(email, password)
        server.sendmail(msg['From'], emaillist, msg.as_string())
        server.close()

    except Exception as e:
        print("Error for connection: {}".format(e))


df = table_creation()
send_email()
###