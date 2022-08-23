SELECT
  universidad as university,
  carrerra as career,
  fechaiscripccion as inscription_date,
  case
  	when split_part(nombrre::text, ' ',1) in ('Mr.','Dr.','Ms.', 'Mrs.') then split_part(nombrre::text, ' ',2)
  	else split_part(nombrre::text, ' ',1)
  end as first_name,
  case
  	when split_part(nombrre::text, ' ',1) in ('Mr.','Dr.','Ms.', 'Mrs.') then split_part(nombrre::text, ' ',3)
  	else split_part(nombrre::text, ' ',2)
  end as last_name,
  sexo as gender,
  AGE(TO_DATE(nacimiento,'DD-MM-YYYY')) as age,
  codgoposstal as postal_code,
  l.localidad  as location, 
  eemail as email
FROM
    public.moron_nacional_pampa mnp 
left join localidad l on
	cast(codgoposstal as int)  = l.codigo_postal 
WHERE 
    universidad='Universidad de morón'
AND TO_DATE(fechaiscripccion,'DD-MM-YYYY') BETWEEN '2020/09/01' AND '2021/02/01'