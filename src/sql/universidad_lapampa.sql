SET datestyle = dmy;
SELECT universidad AS university,
carrerra AS carrer,
fechaiscripccion AS inscription_date,
CASE
	WHEN split_part(nombrre::text,' ',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(nombrre::text,' ',2)
ELSE split_part(nombrre::text,' ',1) end AS first_name,
CASE
	WHEN split_part(nombrre::text,' ',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(nombrre::text,' ',3)
ELSE split_part(nombrre::text,' ',2) end AS last_name,
sexo AS gender,
AGE (CAST (nacimiento AS DATE)) AS age,
CAST (lp.codgoposstal AS int) AS postal_code,
localidad AS location,
eemail AS email

FROM   public.moron_nacional_pampa lp
LEFT JOIN localidad l ON CAST (lp.codgoposstal AS int) = l.codigo_postal
WHERE universidad = 'Universidad nacional de la pampa' 
and fechaiscripccion between '20-Sep-01' and '21-Feb-01';