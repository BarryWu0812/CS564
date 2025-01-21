SELECT
	COUNT(e.employee_id) AS num_of_employee
FROM
	employees e
JOIN
	departments d ON e.department_id = d.department_id
JOIN 
	locations l on d.location_id = l.location_id
JOIN 
	countries c on l.country_id = c.country_id
JOIN
	regions r on c.region_id = r.region_id
WHERE
	r.region_name = 'Europe';
