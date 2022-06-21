-- #1
SELECT studentId,
       courseId,
       name,
       c.contractType,
       split(p.courseName, ',')[safe_ordinal(1)] AS Course,
       c.courseName,
       c.courseStartedAt,
       CASE
           WHEN l1a IS TRUE THEN 1
           ELSE 0
       END AS lesson_1,
       CASE
           WHEN l2a IS TRUE THEN 1
           ELSE 0
       END AS lesson_2,
       CASE
           WHEN l3a IS TRUE THEN 1
           ELSE 0
       END AS lesson_3,
       CASE
           WHEN l4a IS TRUE THEN 1
           ELSE 0
       END AS lesson_4,
       CASE
           WHEN l5a IS TRUE THEN 1
           ELSE 0
       END AS lesson_5,
       CASE
           WHEN l6a IS TRUE THEN 1
           ELSE 0
       END AS lesson_6,
       CASE
           WHEN l7a IS TRUE THEN 1
           ELSE 0
       END AS lesson_7,
       CASE
           WHEN l8a IS TRUE THEN 1
           ELSE 0
       END AS lesson_8,
       CASE
           WHEN l9a IS TRUE THEN 1
           ELSE 0
       END AS lesson_9,
       CASE
           WHEN l10a IS TRUE THEN 1
           ELSE 0
       END AS lesson_10,
       CASE
           WHEN l11a IS TRUE THEN 1
           ELSE 0
       END AS lesson_11,
       CASE
           WHEN l12a IS TRUE THEN 1
           ELSE 0
       END AS lesson_12,
       CASE
           WHEN l13a IS TRUE THEN 1
           ELSE 0
       END AS lesson_13,
       CASE
           WHEN l14a IS TRUE THEN 1
           ELSE 0
       END AS lesson_14,
       CASE
           WHEN l15a IS TRUE THEN 1
           ELSE 0
       END AS lesson_15,
       CASE
           WHEN l16a IS TRUE THEN 1
           ELSE 0
       END AS lesson_16,
       CASE
           WHEN l17a IS TRUE THEN 1
           ELSE 0
       END AS lesson_17,
       CASE
           WHEN l18a IS TRUE THEN 1
           ELSE 0
       END AS lesson_18,
       CASE
           WHEN l19a IS TRUE THEN 1
           ELSE 0
       END AS lesson_19,
       l1v AS video_1,
       l2v AS video_2,
       l3v AS video_3,
       l4v AS video_4,
       l5v AS video_5,
       l6v AS video_6,
       l7v AS video_7,
       l8v AS video_8,
       l9v AS video_9,
       l10v AS video_10,
       l11v AS video_11,
       l12v AS video_12,
       l13v AS video_13,
       l14v AS video_14,
       l15v AS video_15,
       l16v AS video_16,
       l17v AS video_17,
       l18v AS video_18,
       l19v AS video_19,
       l1h AS homework_1,
       l2h AS homework_2,
       l3h AS homework_3,
       l4h AS homework_4,
       l5h AS homework_5,
       l6h AS homework_6,
       l7h AS homework_7,
       l8h AS homework_8,
       l9h AS homework_9,
       l10h AS homework_10,
       l11h AS homework_11,
       l12h AS homework_12,
       l13h AS homework_13,
       l14h AS homework_14,
       l15h AS homework_15,
       l16h AS homework_16,
       l17h AS homework_17,
       l18h AS homework_18,
       l19h AS homework_19,
       l1qc AS quiz_correct_1,
       l2qc AS quiz_correct_2,
       l3qc AS quiz_correct_3,
       l4qc AS quiz_correct_4,
       l5qc AS quiz_correct_5,
       l6qc AS quiz_correct_6,
       l7qc AS quiz_correct_7,
       l8qc AS quiz_correct_8,
       l9qc AS quiz_correct_9,
       l10qc AS quiz_correct_10,
       l11qc AS quiz_correct_11,
       l12qc AS quiz_correct_12,
       l13qc AS quiz_correct_13,
       l14qc AS quiz_correct_14,
       l15qc AS quiz_correct_15,
       l16qc AS quiz_correct_16,
       l17qc AS quiz_correct_17,
       l18qc AS quiz_correct_18,
       l1ql AS quiz_length_1,
       l2ql AS quiz_length_2,
       l3ql AS quiz_length_3,
       l4ql AS quiz_length_4,
       l5ql AS quiz_length_5,
       l6ql AS quiz_length_6,
       l7ql AS quiz_length_7,
       l8ql AS quiz_length_8,
       l9ql AS quiz_length_9,
       l10ql AS quiz_length_10,
       l11ql AS quiz_length_11,
       l12ql AS quiz_length_12,
       l13ql AS quiz_length_13,
       l14ql AS quiz_length_14,
       l15ql AS quiz_length_15,
       l16ql AS quiz_length_16,
       l17ql AS quiz_length_17,
       l18ql AS quiz_length_18,
       l1qc/l1ql AS _q1,
       l2qc/l2ql AS _q2,
       l3qc/l3ql AS _q3,
       l4qc/l4ql AS _q4,
       l5qc/l5ql AS _q5,
       l6qc/l6ql AS _q6,
       l7qc/l7ql AS _q7,
       l8qc/l8ql AS _q8,
       l9qc/l9ql AS _q9,
       l10qc/l10ql AS _q10,
       l11qc/l11ql AS _q11,
       l12qc/l12ql AS _q12,
       l13qc/l13ql AS _q13,
       l14qc/l14ql AS _q14,
       l15qc/l15ql AS _q15,
       l16qc/l16ql AS _q16,
       l17qc/l17ql AS _q17,
       l18qc/l18ql AS _q18,
       l19qc/l19ql AS _q19
FROM Analytics.student_progress p
INNER JOIN Analytics.student_courses c USING(studentId,
                                             courseId)
WHERE p.courseName = 'Manual QA, March 3, 2022' ---name = 'Veena Mistry'



--#2
(with t as (select * from Analytics.student_progress unpivot include nulls (example for stage in (
			l1v, l1qc, l1ql, l1h, l2v, l2qc, l2ql, l2h, l3v, l3qc, l3ql, l3h, l4v, l4qc, l4ql, l4h, l5v, l5qc, l5ql, l5h, l6v,
			l6qc, l6ql, l6h, l7v, l7qc, l7ql, l7h, l8v, l8qc, l8ql, l8h, l9v, l9qc, l9ql, l9h, l10v, l10qc, l10ql,
			l10h, l11v, l11qc, l11ql, l11h, l12v, l12qc, l12ql, l12h, l13v, l13qc, l13ql, l13h,l14v, l14qc, l14ql, l14h, 
			l15v, l15qc, l15ql, l15h, l16v, l16qc, l16ql, l16h, l17v, l17qc, l17ql, l17h, l18v, l18qc, l18ql, l18h, l19v, 
			l19qc, l19ql,l19h  ) )
)
select name, stage, example, c.contractType, split(t.courseName,',')[safe_ordinal(1)] as Course, c.courseStartedAt from t
	inner join Analytics.student_courses c using(studentId, courseId)
)
union all
(with t as (select * from Analytics.student_progress unpivot include nulls (example for stage in (
 			l1a ,l2a, l3a, l4a, l5a, l6a, l7a,l8a, l9a, l10a, l11a, l12a,  l13a, l14a, l15a, l16a, l17a,  l18a, l19a  ) )
)
select name, stage, case when example is true then 1 when example is null then 0  else 0 end as example, c.contractType, split(t.courseName,',')[safe_ordinal(1)] as Course, c.courseStartedAt from t
	inner join Analytics.student_courses c using(studentId, courseId)
)
union all (select null as name, null as stage ,null as example, null as contractType, null as Course, null as courseStartedAt)
