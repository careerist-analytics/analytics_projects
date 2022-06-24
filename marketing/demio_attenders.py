import requests
import time
import os
import sys

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as BraveService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

import pandas as pd
from datetime import datetime, timedelta

from google.oauth2 import service_account
from google.cloud import bigquery

CREDENTIALS = service_account.Credentials.from_service_account_file(
    'fabled-sorter-289010-9aef64e16ce3.json')

PROJECT_ID = 'fabled-sorter-289010'

HEADERS = {
  'Api-Key': 'omRofLaicCGhMs93337TaMd1lZgukrPY',
  'Api-Secret': 'vjn0sul05V5q'
}

PREFS = {
    'download.default_directory': "/Users/kazionovm/Documents/export",
    'download.prompt_for_download': False,
    'download.directory_upgrade': True,
    'safebrowsing.enabled': False,
    'safebrowsing.disable_download_protection': True
}

BINARY_PATH = '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
options = webdriver.ChromeOptions()
options.binary_location = BINARY_PATH
options.headless = True

options.add_experimental_option('prefs', PREFS)

RENAME_COLS = {
    "Name": "name", "Email": "email",
    "Registration Date & Time": "registration_date",
    "Session Date & Time": "session_date",
    "Join Timestamp": "session_join_at",
    "Exit Timestamp": "session_exit_at",
    "Attendance (Minutes)": "attendance_minutes",
    "Attendance (%)": "attendance_ratio",
    "Focus (%)": "focus_ratio"
}

COLS_TO_CHOOSE = [
 'event_id', 'event_name', 'date_id',
 'name', 'email', 'registration_date', 'session_date',
 'session_join_at', 'session_exit_at',
 'attendance_minutes', 'attendance_ratio', 'focus_ratio'
]

def construct_event_url(event_id, date_id):
    return f"https://my.demio.com/manage/event/{event_id}?activity&people&people_type=attended&filter=session&session={date_id}"


def get_events(max_date, headers):
    try:
        resp = requests.get('https://my.demio.com/api/v1/events?type=past', headers=headers, timeout=5)
        events = pd.DataFrame(resp.json())[['id', 'name', 'date_id', 'timestamp', 'zone']]
        events.loc[:, 'timestamp'] = events.loc[:, 'timestamp'].apply(
            lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        events = events[(events['timestamp'] >= max_date) & ~(events['name'].str.contains("FL"))].reset_index(drop=True)

        return events
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
        return None
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        return None
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
        return None
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)


def download_events_info(events, options):
    driver = webdriver.Chrome(
        service=BraveService(ChromeDriverManager(chrome_type=ChromeType.BRAVE).install()), options=options
    )

    try:
        for i, event in enumerate(events.to_dict('records')):
            driver.get(construct_event_url(event['id'], event['date_id']))

            try:
                if i == 0:
                    driver.find_element_by_link_text('Log in to your account').click()
                    wait = WebDriverWait(driver, 10)

                    driver.implicitly_wait(5)

                    el = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@type='email']")))
                    el.send_keys("recruiting@careerist.com")
                    el.send_keys(Keys.TAB)

                    time.sleep(2.5)

                    el = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@type='password']")))
                    el.send_keys("Emergencyh0st")
                    el.send_keys(Keys.ENTER)

                if i % 10 == 0:
                    time.sleep(15)

                driver.implicitly_wait(10)
                x = driver.find_element_by_class_name("people-export")

                x.click()
                print(f"✅ downloaded: {event['name']} | {event['id']}")

                time.sleep(5)

            except Exception as e:
                print(e)
                continue

        return 200

    except Exception as e:
        print(e)
        return None

    finally:
        driver.quit()


def join_events_info(events, folder_path, rename_cols, cols_to_choose):
    events_dfs = []

    for event in events.to_dict('records'):
        try:
            files = list(filter(lambda x: str(event['id']) in x, os.listdir(folder_path)))

            if files:
                x = pd.read_csv(f"{folder_path}/{files[0]}").rename(columns=rename_cols)

                x['event_id'] = event['id']
                x['event_name'] = event['name']
                x['date_id'] = event['date_id']

                x['registration_date'] = pd.to_datetime(x['registration_date'])
                x['session_date'] = pd.to_datetime(x['session_date'])
                x['session_join_at'] = x['session_date'] + pd.to_timedelta(x['session_join_at'])
                x['session_exit_at'] = x['session_date'] + pd.to_timedelta(x['session_exit_at'], errors="coerce")

                events_dfs.append(x[cols_to_choose])

        except Exception as e:
            print(e)
            continue

    return events_dfs


def upload_table_bigquery(df, table_location):
    try:
        credentials = service_account.Credentials.from_service_account_file('fabled-sorter-289010-9aef64e16ce3.json')
        client = bigquery.Client(project='fabled-sorter-289010', credentials=credentials)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        job = client.load_table_from_dataframe(df, table_location, job_config=job_config)

        print("✅ Successfuly uploaded")
    except Exception as error:
        print('⛔️ Error: ' + repr(error))
        sys.exit()

if __name__ == "__main__":
    events = get_events("2022-06-22", HEADERS)

    if isinstance(events, pd.DataFrame):
        download_response = download_events_info(events, options)

        events_dfs = join_events_info(events, "export", RENAME_COLS, COLS_TO_CHOOSE)

        if events_dfs:
            events_info = pd.concat(events_dfs).sort_values(by='session_date', ascending=False).reset_index(drop=True)

            upload_table_bigquery(events_info, "Renta_dataset.demio_attenders")
        else:
            print("Events dfs are empty")
            sys.exit()


    else:
        sys.exit()
