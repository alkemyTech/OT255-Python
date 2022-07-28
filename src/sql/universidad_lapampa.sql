SELECT universidad AS university,
carrerra AS carrer,
to_date(fechaiscripccion,'DD-MM-YYYY') AS inscription_date,
CASE
	WHEN split_part(nombrre::text,' ',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(nombrre::text,' ',2)
ELSE split_part(nombrre::text,' ',1) end AS first_name,
CASE
	WHEN split_part(nombrre::text,' ',1) in ('Mr.','Dr.','Ms.','Mrs.') THEN split_part(nombrre::text,' ',3)
ELSE split_part(nombrre::text,' ',2) end AS last_name,
sexo AS gender,
extract('year' from age(to_date(nacimiento, 'DD-MM-YYYY'))) AS age,
CAST (lp.codgoposstal AS int) AS postal_code,
eemail AS email
FROM   public.moron_nacional_pampa lp
WHERE universidad = 'Universidad nacional de la pampa' 
AND to_date(fechaiscripccion, 'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01';