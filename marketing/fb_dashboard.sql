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
   WHERE c.contact_source = 'facebook'
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
     fb AS
  (SELECT `date` AS dt,
          fb.campaign_name,
          f.course AS label,
          sum(spend) AS amount_spent,
          sum(impressions) AS impressions,
          sum(inline_link_clicks) AS clicks,
          sum(lead) AS leads
   FROM Renta_dataset.fb_ads_Max_Gusakov_act_419399878644385_2066201980 fb
   LEFT JOIN Renta_dataset.fb_campaigns f ON f.campaign_name = fb.campaign_name
   WHERE date >= '2021-12-01'
     AND fb.campaign_name not like '%webinar%'
     AND fb.campaign_name not like '%Insta%'
     AND fb.campaign_name not like '%ig_olesia%'
   GROUP BY 1,
            2,
            3)
SELECT *,
       row_number() over(PARTITION BY dt, label) AS rn
FROM leads_total
LEFT JOIN fb USING(dt,
                   label)