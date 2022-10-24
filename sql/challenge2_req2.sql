-- count of the number of employees hired in 2021 higher than the average ordered descending

SELECT
  id,
  department,
  hired
FROM
  (SELECT b_h.*, 139 as mean
  FROM
    (SELECT d.id, d.department, count(e.id) hired
    FROM  hired_employees e
    INNER JOIN departments d ON  e.department_id = d.id
    WHERE year(datetime) = "2021"
    GROUP BY department) b_h
  ) b_m
WHERE hired > mean