select
universidad as university,
carrera as career,
fecha_de_inscripcion::date as inscription_date,
CASE
      WHEN split_part(nombre::text, '_',1) in ('MR.','DR.', 'MS.','MRS.')  THEN split_part(nombre::text, '_',2)
ELSE split_part(nombre::text, '_',1)
end as first_name,
CASE
      WHEN split_part(nombre::text, '_',1) in ('MR.','DR.', 'MS.', 'MRS.')  THEN split_part(nombre::text, '_',3)
ELSE split_part(nombre::text, '_',2)
end as last_name,
sexo as gender,
extract (year from age (now()::date,fecha_nacimiento::date))   as age,
array_agg (loc.codigo_postal) as postal_code,
replace (svm.localidad,'_',' ') as location,
email
from salvador_villa_maria svm
join localidad loc
on loc.localidad = replace (svm.localidad,'_',' ')
where svm.universidad = 'UNIVERSIDAD_DEL_SALVADOR'
and fecha_de_inscripcion::date between '2020-09-01' and '2021-02-01'
group by
university,
career,
inscription_date,
first_name,
last_name,
gender,
age,
location,
email