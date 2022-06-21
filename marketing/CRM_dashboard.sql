WITH leads AS
  (SELECT element_id AS id,
          cast(date_created AS date) AS dt,
          cast(date_closed AS date) AS dt_closed,
          name,
          contact_id,
          lead_source AS SOURCE,
          lead_medium AS medium,
          CASE
              WHEN lower(lead_content) like '%digest%' THEN 'Дайджест (контентные)' -- если содержит

              WHEN lower(lead_content) like 'sale%' THEN 'Sale (промо со скидками)' -- если содержит

              WHEN lower(lead_content) like '%first_lesson%' THEN 'Первый урок' --есть

              WHEN lower(lead_content) = '%oto_3_id1%' THEN 'Вебинары' --есть

              WHEN lower(lead_content) like '%anons_course%' THEN 'Анонс курса' --есть

              WHEN lower(lead_content) like '%reactivation%' THEN 'Реактиация'
              WHEN lower(lead_content) like '%welcome%' THEN 'Welcome' -- если содержит

              WHEN lower(lead_content) like '%refferals%' THEN 'Рефералка'
              ELSE 'Другое'
          END AS content_,
          CASE
              WHEN lower(lead_campaign) like '%manual%' THEN 'Manual QA'
              WHEN lower(lead_campaign) like '%qaa%' THEN 'Automation QA'
              WHEN lower(lead_campaign) like '%syseng%' THEN 'Systems Engineer'
              WHEN lower(lead_campaign) like '%techsales%' THEN 'Tech Sales'
              WHEN lower(lead_campaign) like '%salestraining%' THEN 'Sales Training'
              WHEN lower(lead_campaign) like '%salesengineer%' THEN 'Sales Engineer'
              WHEN lower(lead_campaign) like '%careerup%' THEN 'Career UP'
              WHEN lower(lead_campaign) like '%jas%' THEN 'JAS'
              WHEN lower(lead_campaign) like '%python%' THEN 'Python'
              WHEN lower(lead_campaign)like '%usjob%' THEN 'US Job'
              ELSE 'ALL'
          END AS campaign,
          replace(lead_term, '#applyForm', '') AS term,
          pipeline,
          CASE
              WHEN status_id = 142
                   AND sale != 0 THEN sale
              ELSE 0
          END AS revenue,
          CASE
              WHEN status_id = 142
                   AND sale != 0 THEN 1
              ELSE 0
          END AS sales,
          ROW_NUMBER() over(PARTITION BY l.contact_id, l.course
                            ORDER BY l.date_created DESC) rn,
                       ROW_NUMBER() over(PARTITION BY l.contact_id, l.pipeline
                                         ORDER BY l.date_created DESC) rn_p
   FROM Renta_dataset.full_leads_data l)
SELECT *,
       CASE
           WHEN SOURCE = 'webinar' THEN 'Вебинары'
           ELSE content_
       END AS content
FROM leads
WHERE ((rn = 1
        AND rn_p >= 1)-- один контакт на разных курсах

       OR (rn > 1
           AND pipeline!= 'Qualification pipeline'))-- контакт на разных пайплайнах, но не на квале

  AND lower(SOURCE) in ('email',
                        'webinar')
  AND lower(name) not like '%test%'