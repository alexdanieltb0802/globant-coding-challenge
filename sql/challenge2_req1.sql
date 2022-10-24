-- number of employees hired per quarter in a given job

USE globant;

select department,
       job,
       count(if(Q_2021=1,1,null)) as Q1,
       count(if(Q_2021=2,1,null)) as Q2,
       count(if(Q_2021=3,1,null)) as Q3,
       count(if(Q_2021=4,1,null)) as Q4
FROM
(SELECT hired_employees.*, departments.department, jobs.job, quarter(datetime) Q_2021
FROM  hired_employees
INNER JOIN departments ON  hired_employees.department_id = departments.id
INNER JOIN jobs ON  hired_employees.job_id = jobs.id
WHERE year(datetime) = "2021") as base
GROUP BY jobs.job
ORDER BY departments.department
