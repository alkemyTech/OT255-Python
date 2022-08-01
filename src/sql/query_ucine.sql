--WITH localidad_t AS (
--	SELECT l2.localidad, array_agg(l2.codigo_postal) as codigo_postal
--	FROM localidad l2
--	GROUP BY l2.localidad
--	)
SELECT
	replace(lsc1.universities, '-', ' ') as university,
	replace(lsc1.careers, '-', ' ') as career,
	to_date(lsc1.inscription_dates, 'DD-MM-YYYY') as inscription_date,
	CASE 
		WHEN split_part(lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(lsc1.names, '-', 2)
		ELSE split_part(lsc1.names, '-', 1)
	END as first_name, -- select for first name: second name if preffix / first name if no preffix
	CASE 
		WHEN split_part(lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(lsc1.names, '-', 3)
		ELSE split_part(lsc1.names, '-', 2)
	END as last_name, -- select for last name: third name if preffix / second name if no preffix
	lsc1.sexo as gender,
	extract('year' from age(to_date(lsc1.birth_dates, 'DD-MM-YYYY'))) as age, -- calculate age from birth date
	replace(lsc1.locations, '-', ' ') as location,
--	l1.codigo_postal as postal_code, -- select possible postal codes inside an array to avoid duplicated results
--	l1.localidad as location,
	lsc1.emails as email
FROM lat_sociales_cine lsc1 -- LEFT JOIN localidad_t l1 on replace(lsc1.locations, '-', ' ') = l1.localidad -- join localidad to get postal codes
WHERE lsc1.universities = 'UNIVERSIDAD-DEL-CINE' -- filter by university
AND to_date(lsc1.inscription_dates, 'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01' -- filer by incription date
ORDER BY lsc1.id -- order by original id to compare original table and query result
;