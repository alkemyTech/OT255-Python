
SELECT universidad AS university,
careers AS carrer,
to_date(fecha_de_inscripcion, 'YY-MON-DD') AS inscription_date,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',2)
ELSE split_part(names::text,'-',1) end AS first_name,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',3)
ELSE split_part(names::text,'-',2) end AS last_name,
sexo AS gender,
extract('year' from age(to_date(birth_dates, 'YY-MON-DD'))) AS age,
array_agg(l.codigo_postal ) AS postal_code,
CAST(UPPER(REPLACE (l.localidad,'-',' ')) AS character varying) AS location,
correos_electronicos AS email

FROM  public.palermo_tres_de_febrero ui
LEFT JOIN localidad l ON  CAST (ui.codigo_postal AS int)= l.codigo_postal

WHERE universidad = '_universidad_de_palermo'
and to_date(fecha_de_inscripcion, 'DD-MON-YYYY') BETWEEN ' 01-09-2020' AND '01/02/2021'
group by
university,
carrer,
inscription_date,
first_name,
last_name,
gender,
age,
location,
email


