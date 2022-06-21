WITH demio AS
  (SELECT name,
          participants,
          non_participating,
          CASE
              WHEN lower(name) LIKE '%training%' THEN 'Sales Training'
              WHEN lower(name) LIKE '%manual%' THEN 'Manual QA'
              WHEN lower(name) LIKE '%sales engineer%' THEN 'Sales Engineer'
              WHEN lower(name) LIKE '%tech%' THEN 'Tech Sales'
              WHEN lower(name) LIKE '%career%' THEN 'CareerUp QA'
              WHEN lower(name) LIKE '%system%' THEN 'Systems Engineer'
              WHEN lower(name) LIKE '%automation%' THEN 'Automation QA'
          END AS label,
          datetime(timestamp(datetime(timestamp(`timestamp`), 'US/Pacific')), 'Europe/Moscow') AS dt,
          cast(timestamp(datetime(timestamp(TIMESTAMP), 'US/Pacific')) AS date) AS course_month
   FROM Renta_dataset.demio
   WHERE event_type = 'lesson'),
     es AS
  (SELECT *,
          CASE
              WHEN course like '%mqa%' THEN 'Manual QA'
              WHEN course like '%career%' THEN 'CareerUp QA'
              WHEN (course = 'sa'
                    OR course = '%syseng%') THEN 'Systems Engineer'
              WHEN course = 'se' THEN 'Sales Engineer' --ts
              WHEN course = 'st' THEN 'Sales Training' -- st
              WHEN course = 'ts' THEN 'Tech Sales' -- ts
              WHEN course = 'qaa' THEN 'Automation QA'
              ELSE 'unknown'
          END AS label
   FROM Renta_dataset.expertsender_firstlesson
   WHERE course != 'first'),
     leads AS
  (SELECT l.date_closed,
          l.name,
          c.primary_email AS email,
          l.course,
          l.course_month,
          l.sale
   FROM Renta_dataset.full_leads_data l
   LEFT JOIN Renta_dataset.full_contacts_data c ON l.contact_id = c.contact_id
   WHERE lower(l.name) not like '%reactivation%'
     AND lower(l.name) not like '%test%'
     AND lower(l.name) not like '%автосделка%'
     AND l.pipeline not in ('Reactivation',
                            'Qualification pipeline')
     AND l.course_month IS NOT NULL
     AND status_id = 142
     AND sale > 0),
     revenue AS
  (SELECT d.paymentType,
          email,
          CASE
              WHEN i.isCreatedInPublic IS NULL THEN 0
              ELSE i.isCreatedInPublic
          END AS PUBLIC,
          sum(i.amount) AS bo_amount,
   FROM Renta_dataset.biz_interaction i
   LEFT JOIN Renta_dataset.biz_dealinvoice d ON i.id = d.interactionId
   WHERE isPaid = 1 --and email = 'konanroland@gmail.com'
   GROUP BY 1,
            2,
            3)
SELECT d.name,
       d.label,
       d.course_month,
       d.dt AS lesson_dt,
       CASE
           WHEN r.paymentType IS NULL THEN 'unknown'
           ELSE r.paymentType
       END AS payment_type,
       l.date_closed,
       l.name AS student_name,
       l.email,
       coalesce(l.sale, 0) AS sale,
       r.bo_amount,
       COALESCE(r.public, 0) AS PUBLIC,
       e.size,
       d.participants,
       d.non_participating,
       CASE
           WHEN date_diff(date_closed, dt, MINUTE) > 0 THEN 'after'
           ELSE 'before'
       END AS stage,
       ROW_NUMBER() over(PARTITION BY d.label, d.course_month) AS rn
FROM demio d
LEFT JOIN leads l ON d.label = l.course
AND d.course_month = l.course_month
LEFT JOIN es e ON e.label = d.label
AND cast(e.web_date AS date) = d.course_month
LEFT JOIN revenue r ON r.email = l.email