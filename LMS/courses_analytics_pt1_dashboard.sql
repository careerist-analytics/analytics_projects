SELECT DISTINCT (split(courseName, ','))[safe_ordinal(1)] AS Course,
                courseName,
                s.courseStartedAt ,
                studentName,
                email,
                CASE
                    WHEN contractType IS NULL THEN 'unknown'
                    WHEN length(contractType) = 0 THEN 'unknown'
                    ELSE contractType
                END AS contractType,
                COALESCE(total, 0) AS score, --round(rand()*100,0) as score,
 CASE
     WHEN s.step = 0 THEN 0
     ELSE 1
 END AS course_completed, --step,
 CASE
     WHEN internshipProject1 IS NOT NULL
          AND internshipGrade1 > 1 THEN 1
     ELSE 0
 END AS internship_1,
 CASE
     WHEN internshipProject2 IS NOT NULL
          AND internshipGrade2 > 1 THEN 1
     ELSE 0
 END AS internship_2,
 CASE
     WHEN mentorAssignedAt IS NOT NULL THEN 1
     ELSE 0
 END AS mentorship,
 CASE
     WHEN c.checklistType = 'finalInterview'
          AND c.isCompleted IS TRUE THEN 1
     ELSE 0
 END AS interview_passed,
 cast(c.timestamp AS date) AS interview_dt,
 CASE
     WHEN jasUserId IS NOT NULL THEN 1
     ELSE 0
 END AS JAS,
 j.userId,
 cast(cast(j.date AS TIMESTAMP) AS date) AS offer_date,
 CASE
     WHEN j.userId IS NOT NULL THEN 1
     ELSE 0
 END AS Job_Offer,
 CASE
     WHEN j.isBgCheckPassed IS NULL THEN 0
     ELSE 1
 END AS BG_check,
 CASE
     WHEN j.isViaClientService IS NULL THEN 0
     ELSE 1
 END AS CS,
 CASE
     WHEN o.total_offers IS NULL THEN 0
     ELSE o.total_offers
 END AS total_offers,
 internshipProject1,
 internshipProject2,
 internshipGrade1,
 internshipGrade2,
 mentor,
 mentorAssignedAt,
 jasAccountCreatedAt
FROM Analytics.student_courses s
LEFT JOIN
  (SELECT *
   FROM Mentorship_dataset_PROD.Checklist_Submission_PROD
   WHERE checklistType = 'finalInterview'
     AND isCompleted IS TRUE
     AND courseId IS NOT NULL) c ON s.studentId = c.userId
AND c.courseId = s.courseId
LEFT JOIN
  (SELECT *
   FROM
     (SELECT userId,
             isBgCheckPassed,
             isViaClientService,
             courseId, date, ROW_NUMBER () over(PARTITION BY userId, courseId
                                                ORDER BY date DESC) AS rn
      FROM Mentorship_dataset_PROD.Job_Offer_PROD
      WHERE status = 'accepted') a
   WHERE a.rn = 1) j ON j.userId = s.studentId
AND j.courseId = s.courseId
LEFT JOIN
  (SELECT userId,
          courseId,
          count(*) AS total_offers
   FROM
     (SELECT DISTINCT userId,
                      courseId,
                      companyName AS total_offers
      FROM Mentorship_dataset_PROD.Job_Offer_PROD) offers
   GROUP BY 1,
            2) o ON o.userId = s.studentId
AND o.courseId = s.courseId
LEFT JOIN
  (SELECT *
   FROM
     (SELECT courseId,
             studentId,
             scoring,
             updatedAt,
             row_number() over(PARTITION BY courseId, studentId
                               ORDER BY updatedAt DESC) AS rn
      FROM Analytics.student_scoring)
   WHERE rn = 1) sc ON sc.courseId = s.courseId
AND sc.studentId = s.studentId
WHERE lower(studentName) not like '%test%'
  AND lower(s.courseName) not  like '%copy%' --and s.studentId = 139017
--and lower(s.courseName) not  like '%test%'
--and s.studentName =  'David Onguko'
--in ('Mason Kitchens', 'Haridurga Guduru', 'Daniel Durojaiye')
--and s.courseName = 'Manual QA, October 17th, 2021'
--and s.studentName = 'Bryan Brooks'