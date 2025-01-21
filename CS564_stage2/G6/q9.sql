SELECT departments.department_name
FROM employees
JOIN departments on employees.department_id = departments.department_id
WHERE employees.salary = (SELECT MAX(salary) FROM employees);
