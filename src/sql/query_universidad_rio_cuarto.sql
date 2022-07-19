SELECT
  univiersities as university,
  carrera as career,
  TO_DATE(inscription_dates ,'YY-Mon-DD')  as inscription_date,
  case
  	when split_part(names::text, '-',1) in ('Mr.','Dr.','Ms.', 'Mrs.') then split_part(names::text, '_',2)
  	else split_part(names::text, '-',1)
  end as first_name,
  case
  	when split_part(names::text, '-',1) in ('Mr.','Dr.','Ms.', 'Mrs.') then split_part(names::text, '_',3)
  	else split_part(names::text, '-',2)
  end as last_name,
  sexo as gender,
  AGE(TO_DATE(fechas_nacimiento,'YY-Mon-DD')) as age,
  reverse(split_part(reverse(direcciones::text), '-', 1))  as postal_code,
  direcciones as location, 
  email as email
FROM
    public.rio_cuarto_interamericana rci  
WHERE 
    univiersities ='Universidad-nacional-de-río-cuarto'
AND TO_DATE(inscription_dates ,'YY-Mon-DD') BETWEEN '2020-09-01' AND '2021-02-01'