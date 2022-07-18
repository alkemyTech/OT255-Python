
SELECT universidades AS university,
carreras AS career,
to_date(fechas_de_inscripcion, 'DD-Mon-YY') AS inscription_date,
SPLIT_PART(nombres, '-', 1) AS first_name,
SPLIT_PART(nombres, '-', 2) AS last_name,
sexo AS gender,
DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(fechas_nacimiento, 'YY-Mon-DD')::date) AS age,
codigos_postales AS postal_code,
localidad.localidad AS location,
emails AS email
FROM uba_kenedy 
LEFT JOIN localidad
ON cast(uba_kenedy.codigos_postales as integer) = localidad.codigo_postal
WHERE universidades='universidad-j.-f.-kennedy'
AND to_date(fechas_de_inscripcion, 'YY-Mon-DD') BETWEEN '2020-09-01' AND '2021-02-01'