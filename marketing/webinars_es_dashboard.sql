WITH fb AS
  (SELECT string(parse_date("%d%m%y", substr(replace(replace(ad_name, '_', ''), '.', ''), -6, 6))) AS webinar_date,
          substr(replace(replace(ad_name, '_', ''), '.', ''), -6, 6) AS web_date,
          f.course,
          f.course_label AS label,
          t.campaign_name,
          ad_name,
          sum(spend) AS amount_spent,
          sum(impressions) AS impressions,
          sum(inline_link_clicks) AS clicks
   FROM Renta_dataset.fb_ads_Max_Gusakov_act_419399878644385_2066201980 t
   LEFT JOIN Renta_dataset.fb_campaigns f ON t.campaign_name = f.campaign_name
   WHERE t.campaign_name LIKE '%webinar%'
     AND `date` >= '2021-11-10'
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6
   HAVING webinar_date > '2021-11-23'),
     tilda AS
  (SELECT utm_term,
          count(*) AS tilda_count
   FROM Renta_dataset.tilda
   GROUP BY 1),
     demio AS
  (SELECT name,
          CASE
              WHEN lower(name) LIKE '%training%' THEN 'WebST'
              WHEN lower(name) LIKE '%manual%' THEN 'WebQA'
              WHEN lower(name) LIKE '%open%' THEN 'WebOpen'
              WHEN lower(name) LIKE '%sales engineer%' THEN 'WebTS'
              WHEN lower(name) LIKE '%tech%' THEN 'WebTS'
              WHEN lower(name) LIKE '%career%' THEN 'WebCU'
              WHEN lower(name) LIKE '%system%' THEN 'WebSE'
              WHEN lower(name) LIKE '%automation%' THEN 'WebQAA'
          END AS label,
          concat(substr(replace(replace(name, '_', ''), '.', ''), -5, 5), substr(cast(timestamp(datetime(timestamp(`timestamp`), 'US/Pacific')) AS string), 3, 2)) AS web_date,
          participants,
          total
   FROM Renta_dataset.demio
   WHERE TIMESTAMP >= '2021-11-10 00:00:00'),
     t AS
  (SELECT main_contact_id AS id,
          CASE
              WHEN status_id = '142'
                   AND sale > 0 THEN sale
              ELSE 0
          END AS payment_all,
          CASE
              WHEN status_id = '142'
                   AND sale > 0 THEN 1
              ELSE 0
          END AS contracts_all
   FROM Renta_dataset.amoCRM_Leads_https_jobeasy_amocrm_ru_292285003
   WHERE created_at >= '2021-11-01 00:00:00'
     AND (custom_fields_name = 'utm_campaign_lead'
          OR custom_fields_name = 'Course label')
   GROUP BY 1,
            2,
            3),
     t1 AS
  (SELECT cast(id AS string) AS id,
          substr(replace(replace(json_extract_scalar(t, '$.values[0].value'), '_', ''), '.', ''), -6, 6) AS web_date,
          json_extract_scalar(t, '$.name') AS custom_fields,
          json_extract_scalar(t, '$.values[0].value') AS custom_value
   FROM Renta_dataset.amoCRM_contacts,
        unnest(json_extract_array(custom_fields, '$')) t
   GROUP BY 1,
            2,
            3,
            4 --HAVING custom_fields = 'utm_campaign'),
 ),
     leads_fb AS
  (SELECT CASE
              WHEN custom_value LIKE '%manual%' THEN 'WebQA'
              WHEN custom_value LIKE '%open%' THEN 'WebOpen'
              WHEN custom_value LIKE '%syseng%' THEN 'WebSE'
              WHEN custom_value LIKE '%tech%' THEN 'WebTS'
              WHEN custom_value LIKE '%salestraining%' THEN 'WebST'
              WHEN custom_value LIKE '%salesengineer%' THEN 'WebST'
              WHEN custom_value LIKE '%career%' THEN 'WebCU'
              WHEN custom_value LIKE '%automation%' THEN 'WebQAA'
          END AS lead_label,
          web_date,
          count(*) AS leads_amo,
          sum(contracts_all) AS contracts_all,
          sum(payment_all) AS payment_all
   FROM t
   LEFT JOIN t1 USING (id)
   GROUP BY 1,
            2),
     es AS
  (SELECT SIZE,
          substr(web_date, -6, 6) AS web_date,
          CASE
              WHEN name like '%mqa%' THEN 'WebQA'
              WHEN name like '%htjIT%' THEN 'WebOpen'
              WHEN name like '%career%' THEN 'WebCU'
              WHEN name like '%_sa%' THEN 'WebSE'
              WHEN name like '%_se%' THEN 'WebTS' --ts
              WHEN name like '%_st%' THEN 'WebST' -- st
              WHEN name like '%_ts%' THEN 'WebTS' -- ts
              WHEN name like '%_qaa%' THEN 'WebQAA'
              ELSE 'unknown'
          END AS label
   FROM Renta_dataset.expertsender)
SELECT fb.*,
       tilda.tilda_count,
       demio.participants,
       demio.total,
       leads_fb.leads_amo,
       leads_fb.contracts_all,
       leads_fb.payment_all,
       es.size,
       bo.bo_revenue,
       bo.bo_sales,
       bo.pp_sales,
       bo.pp_revenue,
       row_number() over(PARTITION BY fb.webinar_date, fb.label) AS rn
FROM fb
LEFT JOIN tilda ON tilda.utm_term = fb.ad_name
LEFT JOIN demio ON fb.label = demio.label
AND cast(fb.web_date AS int) = cast(demio.web_date AS int)
LEFT JOIN leads_fb ON leads_fb.lead_label = fb.label
AND leads_fb.web_date = fb.web_date
LEFT JOIN es ON fb.web_date = es.web_date
AND es.label = fb.label
LEFT JOIN Renta_dataset.bo_webinars_revenue bo ON fb.web_date = bo.web_date
AND fb.label = bo.lead_label
ORDER BY 1 DESC