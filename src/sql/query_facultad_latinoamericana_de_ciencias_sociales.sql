SELECT universities AS university,
careers AS career,
to_date(inscription_dates, 'DD-MM-YYYY') as inscription_date,
CASE
		WHEN SPLIT_PART(names, '-', 1) IN ('MRS.', 'MISS', 'MR.', 'DR.') THEN SPLIT_PART(names, '-', 2)
ELSE SPLIT_PART(names, '-', 1)
END AS first_name,
CASE
		WHEN SPLIT_PART(names, '-', 1) IN ('MRS.', 'MISS', 'MR.', 'DR.') THEN SPLIT_PART(names, '-', 3)
ELSE SPLIT_PART(names, '-', 2)
END AS last_name,
sexo AS gender,
DATE_PART('year', now()::date) - DATE_PART('year', to_date(birth_dates, 'DD-MM-YYYY')::date) AS age,
localidad2.codigo_postal AS postal_code,
locations AS location,
emails AS email
FROM lat_sociales_cine 
LEFT JOIN (SELECT localidad, 
		   ARRAY_AGG(codigo_postal ORDER BY codigo_postal) codigo_postal 
		   FROM localidad 
		   GROUP BY localidad
		   ) 
		   AS localidad2
ON lat_sociales_cine.locations = replace(localidad2.localidad, ' ', '-')
WHERE universities='-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND to_date(inscription_dates, 'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'