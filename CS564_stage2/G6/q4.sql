SELECT
	COUNT(e.employee_id) AS num_of_employees
FROM 
	departments d
JOIN 

	employees e ON d.department_id == e.department_id
WHERE
	d.department_name = 'Shipping';
