SELECT univiersities AS university,
carrera AS carrer,
to_date(inscription_dates, 'YY-MON-DD') AS inscription_date,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',2)
ELSE split_part(names::text,'-',1) end AS first_name,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',3)
ELSE split_part(names::text,'-',2) end AS last_name,	
sexo AS gender,
extract('year' from age(to_date(fechas_nacimiento, 'YY-MON-DD'))) AS age,

CAST(UPPER(REPLACE (ui.localidad,'-',' ')) AS character varying) AS location,
email AS email

FROM  public.rio_cuarto_interamericana ui

WHERE univiersities = '-universidad-abierta-interamericana' 
and to_date(inscription_dates, 'YY-MON-DD') BETWEEN '2020-09-01' AND '2021-02-01'