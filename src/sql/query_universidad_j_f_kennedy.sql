/* Para el calculo de edad con fechas incorrecas como 71/Feb/2000
opté por tomar la ultima fecha del mes, la cual seria en el caso anterior
29/feb/2000 y asi poder hacer el calculo.
Dentro de los condicionales, está tambien el caso de años bisiestos
desde 1920 hasta 2024

No se modificó la tabla, solo fueron unos condicionales dentro de la
consulta
*/

SELECT universidades AS university,
carreras AS career,
to_date(fechas_de_inscripcion, 'DD-Mon-YY') AS inscription_date,
SPLIT_PART(nombres, '-', 1) AS first_name,
SPLIT_PART(nombres, '-', 2) AS last_name,
sexo AS gender,
CASE
	WHEN SPLIT_PART(fechas_nacimiento, '-', 2) IN ('Jan', 'Mar', 'May', 'Jul', 'Aug', 'Oct', 'Dec')
		AND SPLIT_PART(fechas_nacimiento, '-', 1) NOT BETWEEN '1' AND '31'
			THEN DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(concat('31', SPLIT_PART(fechas_nacimiento, '-', 2), SPLIT_PART(fechas_nacimiento, '-', 3)), 'DD-Mon-YY')::date)
	WHEN SPLIT_PART(fechas_nacimiento, '-', 2) IN ('Apr', 'Jun', 'Sep', 'Nov')
		AND SPLIT_PART(fechas_nacimiento, '-', 1) NOT BETWEEN '1' AND '30'
			THEN DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(concat('30', SPLIT_PART(fechas_nacimiento, '-', 2), SPLIT_PART(fechas_nacimiento, '-', 3)), 'DD-Mon-YY')::date)
	WHEN SPLIT_PART(fechas_nacimiento, '-', 2) = 'Feb'
		AND SPLIT_PART(fechas_nacimiento, '-', 3) IN ('2024', '2020', '2016', '2012', '2008', '2004', '2000', '1996', '1992', '1988', '1984', '1980', '1976', '1972', '1968', '1964', '1960', '1956', '1952', '1948', '1944', '1940', '1936','1932', '1928', '1924', '1920')
			AND SPLIT_PART(fechas_nacimiento, '-', 1) NOT BETWEEN '1' AND '29'
				THEN DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(concat('29', SPLIT_PART(fechas_nacimiento, '-', 2), SPLIT_PART(fechas_nacimiento, '-', 3)), 'DD-Mon-YY')::date)
	WHEN SPLIT_PART(fechas_nacimiento, '-', 2) = 'Feb'
		AND SPLIT_PART(fechas_nacimiento, '-', 3) NOT IN ('2024', '2020', '2016', '2012', '2008', '2004', '2000', '1996', '1992', '1988', '1984', '1980', '1976', '1972', '1968', '1964', '1960', '1956', '1952', '1948', '1944', '1940', '1936','1932', '1928', '1924', '1920')
			AND SPLIT_PART(fechas_nacimiento, '-', 1) NOT BETWEEN '1' AND '28'
				THEN DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(concat('28', SPLIT_PART(fechas_nacimiento, '-', 2), SPLIT_PART(fechas_nacimiento, '-', 3)), 'DD-Mon-YY')::date)
	
	ELSE DATE_PART('year', NOW()::date) - DATE_PART('year', to_date(concat(SPLIT_PART(fechas_nacimiento, '-', 1), SPLIT_PART(fechas_nacimiento, '-', 2), SPLIT_PART(fechas_nacimiento, '-', 3)), 'DD-Mon-YY')::date)
END AS age,
codigos_postales AS postal_code,
localidad.localidad AS location,
emails AS email
FROM uba_kenedy 
JOIN localidad
ON cast(uba_kenedy.codigos_postales as integer) = localidad.codigo_postal
WHERE universidades='universidad-j.-f.-kennedy'
AND to_date(fechas_de_inscripcion, 'DD-Mon-YY') BETWEEN '01-09-2020' AND '01-02-2021'