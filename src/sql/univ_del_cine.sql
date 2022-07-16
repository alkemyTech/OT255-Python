SELECT
	lsc1.universities as university,
	lsc1.careers as career,
	to_date(lsc1.inscription_dates, 'DD-MM-YYYY') as inscription_date,
	CASE 
		WHEN split_part(lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(lsc1.names, '-', 2)
		ELSE split_part(lsc1.names, '-', 1)
	END as first_name,
	CASE 
		WHEN split_part(lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(lsc1.names, '-', 3)
		ELSE split_part(lsc1.names, '-', 2)
	END as last_name,
	lsc1.sexo as gender,
	extract('year' from age(to_date(lsc1.birth_dates, 'DD-MM-YYYY'))) as age,
	l1.codigo_postal as postal_code,
	l1.localidad as location,
	lsc1.emails as email
FROM lat_sociales_cine lsc1 LEFT JOIN (
	SELECT l1_temp1.localidad, l1_temp1.codigo_postal
	FROM (
		SELECT DISTINCT ON(localidad) localidad, codigo_postal
		FROM localidad l1_temp2
		ORDER BY localidad, codigo_postal
		) as l1_temp1
	) as l1 on REPLACE(lsc1.locations, '-', ' ') = l1.localidad
WHERE lsc1.universities = 'UNIVERSIDAD-DEL-CINE'
AND to_date(lsc1.inscription_dates, 'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
ORDER BY lsc1.id;