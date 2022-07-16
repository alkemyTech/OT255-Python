SELECT lsc0.id,
	lsc0.university,
	lsc0.career,
	lsc0.inscription_date,
	lsc1.first_name,
	lsc1.last_name,
	lsc0.gender,
	lsc2.age,
	lsc3.postal_code,
	lsc3.location,
	lsc0.email
FROM (
	SELECT id,
		universities as university,
		careers as career,
		to_date(inscription_dates, 'DD-MM-YYYY') as inscription_date,
		sexo as gender,
		emails as email
	FROM lat_sociales_cine temp0_lsc1
	) as lsc0 -- get existing columns with desired names
JOIN (
	SELECT temp1_lsc1.id,
		CASE 
			WHEN split_part(temp1_lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(temp1_lsc1.names, '-', 2)
			ELSE split_part(temp1_lsc1.names, '-', 1)
		END as first_name,
		CASE 
			WHEN split_part(temp1_lsc1.names, '-', 1) IN ('DR.', 'DR.', 'MRS.', 'MS.') THEN split_part(temp1_lsc1.names, '-', 3)
			ELSE split_part(temp1_lsc1.names, '-', 2)
		END as last_name
	FROM lat_sociales_cine temp1_lsc1
	) as lsc1 ON lsc0.id = lsc1.id -- get name splitted into first_name and last_name
JOIN (
	SELECT temp2_lsc1.id, temp2_lsc1.birth_date, extract('year' from age(temp2_lsc1.birth_date)) as age
	FROM(
		SELECT temp2_lsc2.id, to_date(temp2_lsc2.birth_dates, 'DD-MM-YYYY') as birth_date
		FROM lat_sociales_cine temp2_lsc2
		) as temp2_lsc1
	) as lsc2 ON lsc0.id = lsc2.id -- get age from existing birth date
JOIN (
	SELECT temp3_lsc1.id, temp3_l1.codigo_postal as postal_code, temp3_l1.localidad as location
	FROM lat_sociales_cine temp3_lsc1 LEFT JOIN (
		SELECT localidad, codigo_postal
		FROM (
			SELECT DISTINCT ON(localidad) localidad, codigo_postal
			FROM localidad temp3_l3
			ORDER BY localidad, codigo_postal
			) as temp3_l2
		) as temp3_l1 on REPLACE(temp3_lsc1.locations, '-', ' ') = temp3_l1.localidad
	) as lsc3 ON lsc0.id = lsc3.id -- get postal code from 'localidad' table (selecting the lowest CP for locations with same name)
WHERE university = 'UNIVERSIDAD-DEL-CINE' AND lsc0.inscription_date BETWEEN '2020-09-01' AND '2021-02-01'
ORDER BY lsc0.id;