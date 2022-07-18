select
universidad as university,
carrera as career,
fecha_de_inscripcion as inscription_date,
CASE
      WHEN split_part(name::text, ' ',1) in ('MR.','DR.', 'MS.','MRS.')  THEN split_part(name::text, ' ',2)
ELSE split_part(name::text, ' ',1)
end as first_name,
CASE
      WHEN split_part(name::text, ' ',1) in ('MR.','DR.', 'MS.', 'MRS.')  THEN split_part(name::text, ' ',3)
ELSE split_part(name::text, ' ',2)
end as last_name,
sexo as gender,
extract (year from age (now()::date,fecha_nacimiento::date))   as age,
fc.codigo_postal as postal_code,
disloc.localidad as location,
correo_electronico as email
from flores_comahue fc
join (
select * from localidad loc
) disloc
on disloc.codigo_postal = cast(fc.codigo_postal as integer)
where fc.universidad = 'UNIV. NACIONAL DEL COMAHUE'
