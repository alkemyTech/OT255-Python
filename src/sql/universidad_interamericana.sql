SET datestyle = ymd;
SELECT univiersities AS university,
carrera AS carrer,
inscription_dates AS inscription_date,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',2)
ELSE split_part(names::text,'-',1) end AS first_name,
CASE
	WHEN split_part(names::text,'-',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(names::text,'-',3)
ELSE split_part(names::text,'-',2) end AS last_name,	
sexo AS gender,
AGE (CAST (fechas_nacimiento AS DATE)) AS age,
codigo_postal  AS postal_code,
CAST(UPPER(REPLACE (ui.localidad,'-',' ')) AS character varying) AS location,
email AS email

FROM  public.rio_cuarto_interamericana ui
LEFT JOIN localidad l ON  CAST (UPPER(REPLACE (ui.localidad,'-',' ')) AS character varying)= l.localidad 

WHERE univiersities = '-universidad-abierta-interamericana' 
and inscription_dates between '20-Sep-01' and '21-Feb-01';