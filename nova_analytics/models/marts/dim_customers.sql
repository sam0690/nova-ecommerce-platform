SELECT
	id as customer_id,
	name,
	email,
	country,
	signup_date
FROM {{source ('shop','customers') }}
