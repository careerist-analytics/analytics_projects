with t1 as --- new learning area daily/weekly/monthly
(
  SELECT 
    DAY AS dt, 
    courseId, 
    courseName, 
    loggedIn 
  FROM 
    Analytics.student_signups 
  WHERE 
    courseName = 'Sales Training Introduction Course'
), 
t as (
  select 
    * 
  from 
    UNNEST(
      GENERATE_DATE_ARRAY(
        '2022-06-01', 
        DATE_ADD(
          CURRENT_DATE(), 
          INTERVAL -1 DAY
        ), 
        INTERVAL 1 Day
      )
    ) AS dt
), 
t2 as (
  --- button click | CTA (new area learning users)
  SELECT 
    cast(recordedAt AS date) dt, 
    courseId, 
    count(*) AS click_button 
  FROM 
    Analytics.student_requests 
  WHERE 
    buttonText = 'Get a callback' 
  GROUP BY 
    1, 
    2
), 
t3_1 as --- total leads quantity 
(
  SELECT 
    *, 
    ROW_NUMBER() over(
      partition by l.contact_id, 
      l.course 
      order by 
        l.date_created desc
    ) rn, 
    ROW_NUMBER() over(
      partition by l.contact_id, 
      l.pipeline 
      order by 
        l.date_created desc
    ) rn_p 
  FROM 
    Renta_dataset.full_leads_data l 
  WHERE 
    source_key = 'formSourceSalesNew2' 
    AND lower(name) not like '%test%'
), 
t3 as (
  select 
    cast(date_created as date) as dt, 
    count(*) as leads 
  from 
    t3_1 
  where 
    (
      (
        rn = 1 
        and rn_p >= 1
      ) -- один контакт на разных курсах
      or (
        rn > 1 
        and pipeline != 'Qualification pipeline'
      )
    ) 
  group by 
    1
), 
t4 as (
  --- total leads qualified 
  SELECT 
    cast(date_created AS date) AS dt, 
    COUNT(DISTINCT element_id) AS leads_q 
  FROM 
    Renta_dataset.full_leads_data 
  WHERE 
    source_key = 'formSourceSalesNew2' 
    AND pipeline = 'Qualification pipeline' 
    AND status_id = 142 
    AND lower(name) not like '%test%' 
  GROUP BY 
    1
), 
t5 as (
  --- total leads enrolled 
  SELECT 
    cast(date_created AS date) AS dt, 
    COUNT(DISTINCT element_id) AS leads_e 
  FROM 
    Renta_dataset.full_leads_data 
  WHERE 
    source_key = 'formSourceSalesNew2' 
    AND pipeline = 'Tech Sales (SC)' 
    AND status_id = 142 
    AND lower(name) not like '%test%' 
  GROUP BY 
    1
), 
t6 as (
  --- how many users watched video 
  SELECT 
    DAY AS dt, 
    courseId, 
    courseName, 
    userViews 
  FROM 
    Analytics.student_video_views 
  WHERE 
    courseName = 'Sales Training Introduction Course'
), 
t7 as (
  select 
    * 
  from 
    (
      select 
        *, 
        ROW_NUMBER() over(
          order by 
            recordedAt desc
        ) as rn 
      from 
        Analytics.student_material_clicks 
      where 
        courseId = 457
    ) t 
  where 
    rn = 1
) 
select 
  t.dt, 
  t1.courseId, 
  t1.courseName, 
  COALESCE(loggedIn, 0) as loggedIn, 
  COALESCE(click_button, 0) as click_button, 
  COALESCE(leads, 0) as leads, 
  COALESCE(leads_q, 0) as leads_q, 
  COALESCE(leads_e, 0) as leads_e, 
  COALESCE(userViews, 0) as userViews, 
  COALESCE(clickedStudentsCount, 0) as clicks_students, 
  COALESCE(clicksCount, 0) as clicks_count, 
  ROW_NUMBER() over() as rn 
from 
  t 
  left join t1 using(dt) 
  left join t2 using(dt, courseId) 
  left join t3 using(dt) 
  left join t4 using(dt) 
  left join t5 using(dt) 
  left join t6 using(dt, courseId) 
  left join t7 using(courseId)