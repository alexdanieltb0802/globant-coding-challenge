-- number of employees hired per quarter in a given job

USE globant;

select department,
       job,
       count(if(Q_2021=1,1,null)) as Q1,
       count(if(Q_2021=2,1,null)) as Q2,
       count(if(Q_2021=3,1,null)) as Q3,
       count(if(Q_2021=4,1,null)) as Q4
FROM
(SELECT hired_employees_backup.*, departments_backup.department, jobs_backup.job, quarter(datetime) Q_2021
FROM  hired_employees_backup
INNER JOIN departments_backup ON  hired_employees_backup.department_id = departments_backup.id
INNER JOIN jobs_backup ON  hired_employees_backup.job_id = jobs_backup.id
WHERE year(datetime) = "2021") as base
GROUP BY jobs_backup.job
ORDER BY departments_backup.department