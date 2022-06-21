WITH es AS
  (SELECT cast(date AS date) AS dt,
          cast(sent AS int) AS sent,
          cast(bounced AS int) AS bounced,
          cast(delivered AS int) AS delivered,
          cast(opens AS int) AS opens,
          cast(uniqueopens AS int) AS uniqueopens,
          cast(clicks AS int) AS clicks,
          cast(uniqueclicks AS int) AS uniqueclicks,
          cast(clickers AS int) AS clickers,
          cast(complaints AS int) AS complaints,
          cast(unsubscribes AS int) AS unsubscribes,
          cast(goals AS int) AS goals,
          cast(goalsvalue AS int) AS goalsvalue
   FROM Renta_dataset.expertsender_stats),
     leads AS
  (SELECT cast(date_created AS date) AS dt,
          cast(date_closed AS date) AS dt_closed,
          element_id,
          name,
          contact_id,
          pipeline,
          lead_source,
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
          ROW_NUMBER() over(PARTITION BY contact_id, course
                            ORDER BY date_created DESC) rn,
                       ROW_NUMBER() over(PARTITION BY contact_id, pipeline
                                         ORDER BY date_created DESC) rn_p
   FROM Renta_dataset.full_leads_data),
     SOURCE AS
  (SELECT dt,
          dt_closed,
          element_id AS id,
          name,
          revenue,
          sales,
          lead_source AS SOURCE
   FROM leads
   WHERE ((rn = 1
           AND rn_p >= 1)-- один контакт на разных курсах

          OR (rn > 1
              AND pipeline!= 'Qualification pipeline'))-- контакт на разных пайплайнах, но не на квале )

     AND lead_source = 'email'
     AND lower(name) not like '%test%' )
SELECT es.*,
       sum(revenue) AS revenue,
       sum(sales) AS sales,
       count(*) AS leads
FROM es
LEFT JOIN SOURCE s ON s.dt = es.dt
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13
ORDER BY 1 DESC



/*

WITH leads AS
  (SELECT cast(date_created AS date) AS dt,
          cast(date_closed AS date) AS dt_closed,
          element_id,
          name,
          contact_id,
          pipeline,
          lead_source,
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
          ROW_NUMBER() over(PARTITION BY contact_id, course
                            ORDER BY date_created DESC) rn,
                       ROW_NUMBER() over(PARTITION BY contact_id, pipeline
                                         ORDER BY date_created DESC) rn_p
   FROM Renta_dataset.full_leads_data
   WHERE lower(name) not like '%test%'
     AND lead_source = 'email')
SELECT dt,
       dt_closed,
       element_id AS id,
       name,
       revenue,
       sales,
       lead_source AS SOURCE
FROM leads
WHERE ((rn = 1
        AND rn_p >= 1)-- один контакт на разных курсах

       OR (rn > 1
           AND pipeline!= 'Qualification pipeline')) -- контакт на разных пайплайнах, но не на квале */
