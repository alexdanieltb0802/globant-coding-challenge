-- initial version to group hired employees by quarter months

USE globant;

SELECT departments_backup.department, jobs_backup.job, COUNT(hired_employees_backup.id) AS Q1
FROM  hired_employees_backup
INNER JOIN departments_backup ON  hired_employees_backup.department_id = departments_backup.id
INNER JOIN jobs_backup ON  hired_employees_backup.job_id = jobs_backup.id
WHERE year(datetime) = '2021' AND month(datetime) between '1' AND '4' 
GROUP BY departments_backup.department, jobs_backup.job