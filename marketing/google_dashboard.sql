WITH leads AS
  (SELECT l.date_created,
          l.name,
          l.contact_id,
          l.course,
          c.contact_source,
          l.pipeline,
          CASE
              WHEN status_id = 142
                   AND sale > 0 THEN sale
              ELSE 0
          END AS payment_all,
          CASE
              WHEN status_id = 142
                   AND sale != 0 THEN 1
              ELSE 0
          END AS enrolled,
          CASE
              WHEN status_id = 143
                   AND sale = 0 THEN 1
              ELSE 0
          END AS lost,
          ROW_NUMBER() over(PARTITION BY l.contact_id, l.course
                            ORDER BY l.date_created DESC) rn,
                       ROW_NUMBER() over(PARTITION BY l.contact_id, l.pipeline
                                         ORDER BY l.date_created DESC) rn_p
   FROM Renta_dataset.full_leads_data l
   LEFT JOIN Renta_dataset.full_contacts_data c ON l.contact_id = c.contact_id
   WHERE c.contact_source = 'google'
     AND cast(l.date_created AS date) >= '2021-12-01'
     AND l.course in ('Tech Sales',
                      'Sales Engineer',
                      'Sales Training',
                      'Automation QA',
                      'Systems Engineer',
                      'Manual QA',
                      'CareerUp QA',
                      'JAS')
     AND lower(l.name) not like '%reactivation%'
     AND lower(l.name) not like '%test%'
     AND lower(l.name) not like '%автосделка%'
     AND l.pipeline != 'Reactivation' ),
     leads_total AS
  (SELECT cast(date_created AS date) AS dt,
          course AS label,
          contact_source AS SOURCE,
          count(*) AS leads_amo,
          sum(payment_all) AS payment_all,
          sum(enrolled) AS enrolled,
          sum(lost) AS lost
   FROM leads
   WHERE ((rn = 1
           AND rn_p >= 1)-- один контакт на разных курсах
          OR (rn > 1
              AND pipeline!= 'Qualification pipeline'))-- контакт на разных пайплайнах, но не на квале
   GROUP BY 1,
            2,
            3
   ORDER BY 1 DESC),
     g_adw AS
  (SELECT segments_date AS dt,
          ga.campaign_name,
          g.course AS label,
          sum(metrics_impressions) AS impressions,
          sum(metrics_clicks) AS clicks,
          sum(metrics_cost_micros)/1000000 AS amount_spent
   FROM Renta_dataset.g_ads ga
   LEFT JOIN Renta_dataset.ggl_campaigns g ON g.campaign_name = ga.campaign_name
   WHERE segments_date >= '2021-12-01'
   GROUP BY 1,
            2,
            3)
SELECT *,
       row_number() over(PARTITION BY dt, label) AS rn
FROM g_adw
LEFT JOIN leads_total USING(dt,
                            label)

------------------- new_table 

WITH leads AS
  (SELECT l.date_created,
          l.name,
          l.contact_id,
          l.course,
          c.contact_source,
          l.pipeline,
          CASE
              WHEN status_id = 142
                   AND sale > 0 THEN sale
              ELSE 0
          END AS payment_all,
          CASE
              WHEN status_id = 142
                   AND sale != 0 THEN 1
              ELSE 0
          END AS enrolled,
          CASE
              WHEN status_id = 143
                   AND sale = 0 THEN 1
              ELSE 0
          END AS lost,
          ROW_NUMBER() over(PARTITION BY l.contact_id, l.course
                            ORDER BY l.date_created DESC) rn,
                       ROW_NUMBER() over(PARTITION BY l.contact_id, l.pipeline
                                         ORDER BY l.date_created DESC) rn_p
   FROM Renta_dataset.full_leads_data l
   LEFT JOIN Renta_dataset.full_contacts_data c ON l.contact_id = c.contact_id
   WHERE c.contact_source = 'google'
     AND cast(l.date_created AS date) >= '2021-12-01'
     AND l.course in ('Tech Sales',
                      'Sales Engineer',
                      'Sales Training',
                      'Automation QA',
                      'Systems Engineer',
                      'Manual QA',
                      'CareerUp QA',
                      'JAS')
     AND lower(l.name) not like '%reactivation%'
     AND lower(l.name) not like '%test%'
     AND lower(l.name) not like '%автосделка%'
     AND l.pipeline != 'Reactivation' ),
     leads_total AS
  (SELECT cast(date_created AS date) AS dt,
          course AS label,
          contact_source AS SOURCE,
          count(*) AS leads_amo,
          sum(payment_all) AS payment_all,
          sum(enrolled) AS enrolled,
          sum(lost) AS lost
   FROM leads
   WHERE ((rn = 1
           AND rn_p >= 1)-- один контакт на разных курсах

          OR (rn > 1
              AND pipeline!= 'Qualification pipeline'))-- контакт на разных пайплайнах, но не на квале

   GROUP BY 1,
            2,
            3
   ORDER BY 1 DESC),
     g_adw AS
  (SELECT segments_date AS dt,
          ga.campaign_name,
          g.course AS label,
          sum(metrics_impressions) AS impressions,
          sum(metrics_clicks) AS clicks,
          sum(metrics_cost_micros)/1000000 AS amount_spent
   FROM Renta_dataset.g_ads ga
   LEFT JOIN Renta_dataset.ggl_campaigns g ON g.campaign_name = ga.campaign_name
   WHERE segments_date >= '2021-12-01'
   GROUP BY 1,
            2,
            3)
SELECT *,
       row_number() over(PARTITION BY dt, label) AS rn
FROM g_adw
LEFT JOIN leads_total USING(dt,
                            label)
