SELECT departments.department_name, COUNT(employees.employee_id) AS employee_count
FROM employees
JOIN departments ON employees.department_id = departments.department_id
GROUP BY departments.department_name
ORDER BY employee_count DESC;
