select
universidad as university,
carrera as career,
fecha_de_inscripcion as inscription_date,
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
disloc.codigo_postal as postal_code,
replace (svm.localidad,'_',' ') as location,
email
from salvador_villa_maria svm
join (
select distinct on (loc.localidad) loc.localidad, loc.codigo_postal from localidad loc
order by loc.localidad) disloc
on disloc.localidad = replace (svm.localidad,'_',' ')
where svm.universidad = 'UNIVERSIDAD_DEL_SALVADOR'