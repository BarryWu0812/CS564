SELECT departments.department_name, AVG(jobs.max_salary) AS avg_max
FROM employees
JOIN jobs ON employees.job_id = jobs.job_id
JOIN departments ON employees.department_id = departments.department_id
GROUP BY departments.department_name
HAVING avg_max > 8000;
