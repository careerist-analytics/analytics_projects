import pandas as pd
import os
from sqlalchemy import create_engine
from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_file\
    ("/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"

engine = create_engine('mysql+pymysql://readonly:readonly@167.172.135.185:3306/careerist_careerist')
my_data = pd.read_sql('''
WITH students_jas AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT ba.jasAccountId) AS students_jas
   FROM biz_JasUser bju
   LEFT JOIN lms_UserCourse luc ON bju.id = luc.userId
   LEFT JOIN lms_Course lc ON luc.courseId = lc.id
   LEFT JOIN biz_CourseLabel bcl ON lc.learningArea = bcl.id
   LEFT JOIN biz_JasAccount bja ON bja.jasUserId = bju.id
   LEFT JOIN biz_Apply ba ON ba.jasAccountId = bja.id
   WHERE bcl.labelName != 'JAS'
   GROUP BY 1) ,
     jas_operators AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT ba.operatorId) AS jas_operators ,
          count(*) AS total_applie
   FROM careerist_careerist.biz_BizUser bbu
   LEFT JOIN careerist_careerist.biz_Apply ba ON ba.operatorId = bbu.id
   WHERE bbu.name NOT IN ('Igor' ,
                          'test' ,
                          'operator')
   GROUP BY 1) ,
     duplicate_applies AS
  (SELECT dt ,
          count(*) AS duplicate_applies
   FROM
     (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
             jobName ,
             applyCompany ,
             count(*) AS da
      FROM careerist_careerist.biz_Apply ba
      WHERE jobName IS NOT NULL
      GROUP BY 1 ,
               2 ,
               3) t
   WHERE da > 1
   GROUP BY 1) ,
     unique_company AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT applyCompany) AS unique_company
   FROM careerist_careerist.biz_Apply ba
   GROUP BY 1) ,
     all_students_total AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS all_students_total
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_CALL'
   GROUP BY 1) ,
     sms_incoming AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS sms_incoming
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_SMS'
   GROUP BY 1) ,
     sms_outgoing AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS sms_outgoing
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'OUTGOING_SMS'
   GROUP BY 1) ,
     all_students_missed AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS all_students_missed
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_CALL'
     AND STATUS IN ('busy' ,
                    'no-answer' ,
                    'failed')
   GROUP BY 1) ,
     all_st_duration_over_80 AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS all_st_duration_over_80
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_CALL'
     AND duration > 80
   GROUP BY 1) ,
     all_st_duration_below_80 AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS all_st_duration_below_80
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_CALL'
     AND STATUS = 'completed'
     AND duration < 80
   GROUP BY 1) ,
     all_st_duration_over_300 AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) AS all_st_duration_over_300
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   WHERE `type` = 'INCOMING_CALL'
     AND STATUS = 'completed'
     AND duration > 300
   GROUP BY 1) ,
     offers AS
  (SELECT DATE_FORMAT(DATE, '%%Y-%%m-%%01') dt ,
          count(*) AS offers
   FROM careerist_careerist.biz_JobOffer bjo
   GROUP BY 1) ,
     jas_offers_1 AS
  (SELECT DATE_FORMAT(DATE, '%%Y-%%m-%%01') dt ,
          count(*) AS jas_offers_1
   FROM careerist_careerist.biz_JobOffer bjo
   WHERE isByJas = 1
   GROUP BY 1) ,
     jas_offers_0 AS
  (SELECT DATE_FORMAT(DATE, '%%Y-%%m-%%01') dt ,
          count(*) AS jas_offers_0
   FROM careerist_careerist.biz_JobOffer bjo
   WHERE isByJas != 1
   GROUP BY 1) ,
     running_time AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          count(*) / 60 AS running_time
   FROM careerist_careerist.biz_JasOperatorWorkMinute bjowm
   GROUP BY 1) ,
     working_time AS
  (SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt ,
          sum(isActive) / 60 AS working_time
   FROM careerist_careerist.biz_JasOperatorWorkMinute bjowm
   GROUP BY 1) ,
     avg_duration AS
  (SELECT DATE_FORMAT(ppne.createdAt, '%%Y-%%m-%%01') dt ,
          avg(duration) AS avg_duration
   FROM careerist_careerist.phonerouter_PhoneNumberEvent ppne
   GROUP BY 1) ,
     grand_total AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS grand_total
   FROM careerist_careerist.biz_Apply ba
   GROUP BY 1) ,
     easy AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS easy
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
   GROUP BY 1) ,
     company AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS company
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
   GROUP BY 1) ,
     glassdoor_1 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS glassdoor_1
   FROM careerist_careerist.biz_Apply ba
   WHERE lower(jobboard) = 'glassdoor'
   GROUP BY 1) ,
     glassdoor_2 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS glassdoor_2
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
     AND lower(jobboard) = 'glassdoor'
   GROUP BY 1) ,
     glassdoor_3 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS glassdoor_3
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
     AND lower(jobboard) = 'glassdoor'
   GROUP BY 1) ,
     indeed_1 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS indeed_1
   FROM careerist_careerist.biz_Apply ba
   WHERE lower(jobboard) = 'indeed'
   GROUP BY 1) ,
     indeed_2 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS indeed_2
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
     AND lower(jobboard) = 'indeed'
   GROUP BY 1) ,
     indeed_3 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS indeed_3
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
     AND lower(jobboard) = 'indeed'
   GROUP BY 1) ,
     monster_1 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS monster_1
   FROM careerist_careerist.biz_Apply ba
   WHERE lower(jobboard) = 'monster'
   GROUP BY 1) ,
     monster_2 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS monster_2
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
     AND lower(jobboard) = 'monster'
   GROUP BY 1) ,
     monster_3 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS monster_3
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
     AND lower(jobboard) = 'monster'
   GROUP BY 1) ,
     zip_1 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS zip_1
   FROM careerist_careerist.biz_Apply ba
   WHERE lower(jobboard) = 'Ziprecruiter'
   GROUP BY 1) ,
     zip_2 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS zip_2
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
     AND lower(jobboard) = 'Ziprecruiter'
   GROUP BY 1) ,
     zip_3 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS zip_3
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
     AND lower(jobboard) = 'Ziprecruiter'
   GROUP BY 1) ,
     dice_1 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS dice_1
   FROM careerist_careerist.biz_Apply ba
   WHERE lower(jobboard) = 'dice'
   GROUP BY 1) ,
     dice_2 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS dice_2
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 1
     AND lower(jobboard) = 'dice'
   GROUP BY 1) ,
     dice_3 AS
  (SELECT DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt ,
          count(DISTINCT id) AS dice_3
   FROM careerist_careerist.biz_Apply ba
   WHERE isEasy = 0
     AND lower(jobboard) = 'dice'
   GROUP BY 1)
SELECT * ,
       0.3 AS price_easy ,
       0.47 AS price_company
FROM students_jas s
    LEFT JOIN jas_operators USING (dt)
    LEFT JOIN duplicate_applies USING (dt)
    LEFT JOIN unique_company USING (dt)
    LEFT JOIN all_students_total USING (dt)
    LEFT JOIN sms_incoming USING (dt)
    LEFT JOIN sms_outgoing USING (dt)
    LEFT JOIN all_students_missed USING (dt)
    LEFT JOIN all_st_duration_over_80 USING (dt)
    LEFT JOIN all_st_duration_below_80 USING (dt)
    LEFT JOIN all_st_duration_over_300 USING (dt)
    LEFT JOIN offers USING (dt)
    LEFT JOIN jas_offers_1 USING (dt)
    LEFT JOIN jas_offers_0 USING (dt)
    LEFT JOIN running_time USING (dt)
    LEFT JOIN working_time USING (dt)
    LEFT JOIN avg_duration USING (dt)
    LEFT JOIN grand_total USING (dt)
    LEFT JOIN easy USING (dt)
    LEFT JOIN company USING (dt)
    LEFT JOIN glassdoor_1 USING (dt)
    LEFT JOIN glassdoor_2 USING (dt)
    LEFT JOIN glassdoor_3 USING (dt)
    LEFT JOIN indeed_1 USING (dt)
    LEFT JOIN indeed_2 USING (dt)
    LEFT JOIN indeed_3 USING (dt)
    LEFT JOIN monster_1 USING (dt)
    LEFT JOIN monster_2 USING (dt)
    LEFT JOIN monster_3 USING (dt)
    LEFT JOIN zip_1 USING (dt)
    LEFT JOIN zip_2 USING (dt)
    LEFT JOIN zip_3 USING (dt)
    LEFT JOIN dice_1 USING (dt)
    LEFT JOIN dice_2 USING (dt)
    LEFT JOIN dice_3 USING (dt)
WHERE s.dt IS NOT NULL''', engine)

my_data = my_data.fillna(0)

client = bigquery.Client(project='fabled-sorter-289010')

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.JAS'
# job_config.write_disposition = 'WRITE_APPEND'

# Load data to BQ
job = client.load_table_from_dataframe(my_data, table, job_config=job_config)

