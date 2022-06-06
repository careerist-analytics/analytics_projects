import pandas as pd
import os
from sqlalchemy import create_engine
from google.oauth2 import service_account
from google.cloud import bigquery
credentials = service_account.Credentials.from_service_account_file("/fabled-sorter-289010-9aef64e16ce3.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/leonidparubets/PycharmProjects/Careerist/fabled-sorter-289010-9aef64e16ce3.json"


engine = create_engine('mysql+pymysql://readonly:readonly@167.172.135.185:3306/careerist_careerist')
my_data = pd.read_sql(''' with students_jas as (select  DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt, count(DISTINCT ba.jasAccountId) as students_jas  from biz_JasUser bju
						left join lms_UserCourse luc on bju.id = luc.userId
						left join lms_Course lc on luc.courseId = lc.id
						left join biz_CourseLabel bcl on lc.learningArea = bcl.id
						left join biz_JasAccount bja on bja.jasUserId  = bju.id
						left join biz_Apply ba on ba.jasAccountId = bja.id
							where  bcl.labelName != 'JAS'
						group by 1),
	jas_operators as (select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt, count(distinct ba.operatorId) as jas_operators, count(*) as total_applie from careerist_careerist.biz_BizUser bbu
						left join careerist_careerist.biz_Apply ba on ba.operatorId = bbu.id
							where bbu.name not in ('Igor', 'test', 'operator')
						group by 1),
duplicate_applies as (select dt, count(*) as duplicate_applies from
							(select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt, jobName, applyCompany, count(*) as da from careerist_careerist.biz_Apply ba
							WHERE jobName is not null
							group by 1,2,3) t
						where da > 1
						group by 1),
	unique_company as (select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt, count(distinct applyCompany) as unique_company from careerist_careerist.biz_Apply ba
						group by 1),
all_students_total as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as all_students_total from careerist_careerist.phonerouter_PhoneNumberEvent ppne
					   	where `type` = 'INCOMING_CALL'
					   group by 1),
	  sms_incoming as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as sms_incoming from careerist_careerist.phonerouter_PhoneNumberEvent ppne
					   	where `type` = 'INCOMING_SMS'
				  	   group by 1),
 	  sms_outgoing as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as sms_outgoing from careerist_careerist.phonerouter_PhoneNumberEvent ppne
					   	where `type` = 'OUTGOING_SMS'
				  	   group by 1),
all_students_missed as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as all_students_missed from careerist_careerist.phonerouter_PhoneNumberEvent ppne
							where `type` = 'INCOMING_CALL'
							and status in ('busy', 'no-answer', 'failed' )
						group by 1),
all_st_duration_over_80 as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as all_st_duration_over_80 from careerist_careerist.phonerouter_PhoneNumberEvent ppne
							where `type` = 'INCOMING_CALL'
							and duration > 80
						group by 1),
all_st_duration_below_80 as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as all_st_duration_below_80 from careerist_careerist.phonerouter_PhoneNumberEvent ppne
								where `type` = 'INCOMING_CALL'
								and status = 'completed'
								and duration < 80
							group by 1),
all_st_duration_over_300 as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*) as all_st_duration_over_300 from careerist_careerist.phonerouter_PhoneNumberEvent ppne
								where `type` = 'INCOMING_CALL'
								and status = 'completed'
								and duration > 300
							group by 1
							),
			offers as (select DATE_FORMAT(date, '%%Y-%%m-%%01') dt, count(*) as offers from careerist_careerist.biz_JobOffer bjo
					   group by 1),
		jas_offers_1 as (select DATE_FORMAT(date, '%%Y-%%m-%%01') dt, count(*) as jas_offers_1 from careerist_careerist.biz_JobOffer bjo
							where isByJas = 1
					   group by 1),
		jas_offers_0 as (select DATE_FORMAT(date, '%%Y-%%m-%%01') dt, count(*) as jas_offers_0 from careerist_careerist.biz_JobOffer bjo
							where isByJas != 1
						group by 1),
		running_time as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, count(*)/60 as running_time from careerist_careerist.biz_JasOperatorWorkMinute bjowm
						 group by 1),
		working_time as (select DATE_FORMAT(createdAt, '%%Y-%%m-%%01') dt, sum(isActive)/60 as working_time from careerist_careerist.biz_JasOperatorWorkMinute bjowm
						 group by 1),
        avg_duration as (select  DATE_FORMAT(ppne.createdAt, '%%Y-%%m-%%01') dt, avg(duration) as avg_duration  from careerist_careerist.phonerouter_PhoneNumberEvent ppne
						 group by 1),
		grand_total as (select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as grand_total  from careerist_careerist.biz_Apply ba
						group by 1),
			   easy as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as easy from careerist_careerist.biz_Apply ba
						where isEasy = 1
						group by 1),
			company as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as company from careerist_careerist.biz_Apply ba
						where isEasy = 0
						group by 1),
			glassdoor_1 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as glassdoor_1 from careerist_careerist.biz_Apply ba
						where lower(jobboard) = 'glassdoor'
						group by 1),
			glassdoor_2 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as glassdoor_2 from careerist_careerist.biz_Apply ba
						where isEasy = 1
						and lower(jobboard) = 'glassdoor'
						group by 1),
			glassdoor_3 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as glassdoor_3 from careerist_careerist.biz_Apply ba
						where isEasy = 0
						and lower(jobboard) = 'glassdoor'
						group by 1),
			indeed_1 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as indeed_1 from careerist_careerist.biz_Apply ba
						where lower(jobboard) = 'indeed'
						group by 1),
			indeed_2 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as indeed_2 from careerist_careerist.biz_Apply ba
						where isEasy = 1
						and lower(jobboard) = 'indeed'
						group by 1),
			indeed_3 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as indeed_3 from careerist_careerist.biz_Apply ba
						where isEasy = 0
						and lower(jobboard) = 'indeed'
						group by 1),
			monster_1 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as monster_1 from careerist_careerist.biz_Apply ba
						where lower(jobboard) = 'monster'
						group by 1),
			monster_2 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as monster_2 from careerist_careerist.biz_Apply ba
						where isEasy = 1
						and lower(jobboard) = 'monster'
						group by 1),
			monster_3 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as monster_3 from careerist_careerist.biz_Apply ba
						where isEasy = 0
						and lower(jobboard) = 'monster'
						group by 1),
			zip_1 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as zip_1 from careerist_careerist.biz_Apply ba
						where lower(jobboard) = 'Ziprecruiter'
						group by 1),
			zip_2 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as zip_2 from careerist_careerist.biz_Apply ba
						where isEasy = 1
						and lower(jobboard) = 'Ziprecruiter'
						group by 1),
			zip_3 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as zip_3 from careerist_careerist.biz_Apply ba
						where isEasy = 0
						and lower(jobboard) = 'Ziprecruiter'
						group by 1),
			dice_1 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as dice_1 from careerist_careerist.biz_Apply ba
						where lower(jobboard) = 'dice'
						group by 1),
			dice_2 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as dice_2 from careerist_careerist.biz_Apply ba
						where isEasy = 1
						and lower(jobboard) = 'dice'
						group by 1),
			dice_3 as (
						select DATE_FORMAT(ba.createdAt, '%%Y-%%m-%%01') dt,  count(distinct id) as dice_3 from careerist_careerist.biz_Apply ba
						where isEasy = 0
						and lower(jobboard) = 'dice'
						group by 1)
select *, 0.3 as price_easy, 0.47 as price_company from students_jas s
left join jas_operators using(dt)
left join duplicate_applies using(dt)
left join unique_company using(dt)
left join all_students_total using(dt)
left join sms_incoming using(dt)
left join sms_outgoing using(dt)
left join all_students_missed using(dt)
left join all_st_duration_over_80 using(dt)
left join all_st_duration_below_80 using(dt)
left join all_st_duration_over_300 using(dt)
left join offers using(dt)
left join jas_offers_1 using(dt)
left join jas_offers_0 using(dt)
left join running_time using(dt)
left join working_time using(dt)
left join avg_duration using(dt)
left join grand_total using(dt)
left join easy using(dt)
left join company using(dt)
left join glassdoor_1 using(dt)
left join glassdoor_2 using(dt)
left join glassdoor_3 using(dt)
left join indeed_1 using(dt)
left join indeed_2 using(dt)
left join indeed_3 using(dt)
left join monster_1 using(dt)
left join monster_2 using(dt)
left join monster_3 using(dt)
left join zip_1 using(dt)
left join zip_2 using(dt)
left join zip_3 using(dt)
left join dice_1 using(dt)
left join dice_2 using(dt)
left join dice_3 using(dt)
where s.dt is not null ''', engine)

my_data = my_data.fillna(0)

client = bigquery.Client(project='fabled-sorter-289010')


job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Define table name, in format dataset.table_name
table = 'Renta_dataset.JAS'
#job_config.write_disposition = 'WRITE_APPEND'

# Load data to BQ
job = client.load_table_from_dataframe(my_data, table, job_config = job_config)

