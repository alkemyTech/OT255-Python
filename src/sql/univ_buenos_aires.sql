SELECT
	uk.universidades as university,
	uk.carreras as career,
	to_date(uk.fechas_de_inscripcion, 'YY-MON-DD') as inscription_date,
	CASE 
		WHEN split_part(uk.nombres, '-', 1) in ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(uk.nombres, '-', 2)
		ELSE split_part(uk.nombres, '-', 1)
	END as first_name, -- select for first name: second name if preffix / first name if no preffix
	CASE 
		WHEN split_part(uk.nombres, '-', 1) in ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(uk.nombres, '-', 3)
		ELSE split_part(uk.nombres, '-', 2)
	END as last_name, -- select for last name: third name if preffix / second name if no preffix
	uk.sexo as gender,
	extract('year' from age(to_date(uk.fechas_nacimiento, 'YY-MON-DD'))) as age, -- calculate age from birth date
	l1.codigo_postal as postal_code,
	l1.localidad as location,
	uk.emails as email
FROM uba_kenedy uk LEFT JOIN localidad as l1 on CAST(uk.codigos_postales as int) = l1.codigo_postal -- join localidad to get locations
WHERE uk.universidades = 'universidad-de-buenos-aires' -- filter by university
	AND to_date(uk.fechas_de_inscripcion, 'YY-MON-DD') BETWEEN '2020-09-01' AND '2021-02-01' -- filer by incription date
ORDER BY uk.id -- order by original id to compare original table and query result
;