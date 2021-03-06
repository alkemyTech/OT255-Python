-- Consulta 2: Universidad de Villa Maria
select
	universidad as university,
	carrera as career,
	fecha_de_inscripcion as inscription_date,
/* Split de 'name' en 'first_name' y 'second_name' */
	case	
	when split_part(nombre,'_', 1) in ('MR.','MRS.','DR.','MS.') then split_part(nombre,'_',2)
else split_part(nombre,'_', 1)
end as first_name,
case	
	when split_part(nombre,'_', 1) in ('MR.','MRS.','DR.','MS.') then split_part(nombre,'_',3)
else split_part(nombre,'_', 2)
end as last_name,
	sexo as gender,
	(current_date - to_date(to_char(fecha_nacimiento :: DATE, 'yyyy-mm-dd'),'yyyy-mm-dd'))/365 as age, /* Convierto fecha_nacimiento -> a string con formato 'yyyy-mm-dd'-> a date con formato 'yyyy-mm-dd' */
/* codigo postal y localidad */
/* A cada persona le corresponde un unico codigo postal, si este es unico, evito duplicados*/
	array_agg(localidad.codigo_postal) as postal_code,
	replace(salvador_villa_maria.localidad, '_', ' ') as location,	
	email,
	id
from
	salvador_villa_maria
/* Contiene todo lo de la tabla Izquierda (universidades) y se le agregan los matchs con la tabla de la derecha (localidad) */ 
left join localidad
/* Intercambio los caracteres '_', ' ' para que las salvador_villa_maria.localidad se pueda comparar con localidad.codigo_postal */
on replace(salvador_villa_maria.localidad, '_', ' ') = localidad.localidad
where 
	universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
and 
	to_date(to_char(fecha_de_inscripcion  :: DATE, 'yyyy-mm-dd'),'yyyy-mm-dd') between '2020/09/01' and '2021/02/01'
group by id, salvador_villa_maria.localidad


	