SELECT 
	c.country_name 
	
FROM
	countries c
JOIN
	regions r ON r.region_id = c.region_id
WHERE
	r.region_name = 'Europe';

	
	
