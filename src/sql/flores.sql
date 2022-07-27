-- Consulta 1 - Universidad de Flores
select 	
	universidad as university, 
	carrera as career, 
	fecha_de_inscripcion as inscription_date,
/* Split de 'name' en 'first_name' y 'second_name' */
	case	
		when split_part(name,' ', 1) in ('MR.','MRS.','DR.','MS.') then split_part(name,' ',2)
	else split_part(name,' ', 1)
	end as first_name,
	case	
		when split_part(name,' ', 1) in ('MR.','MRS.','DR.','MS.') then split_part(name,' ',3)
	else split_part(name,' ', 2)
	end as second_name,
	sexo as gender,
	--(current_date - to_date(fecha_nacimiento, 'YYYY-MM-DD'))/365 as age, /* Otra forma de consultar la edad, verificar */
	date_part('year', current_date) - date_part('year', to_date(fecha_nacimiento,'yy')) as age,
	/* codigo postal y localidad */
	flores_comahue.codigo_postal as postal_code,
	localidad.localidad as location,
	correo_electronico as email
from 
	flores_comahue 
/* Contiene todo lo de la tabla Izquierda (universidades) y se le agregan los matchs con la tabla de la derecha (localidad) */
left join localidad
on cast(flores_comahue.codigo_postal as int) = localidad.codigo_postal
where 
	universidad = 'UNIVERSIDAD DE FLORES'
and 
	to_date(fecha_de_inscripcion,'yyy-mm-dd') between '2020/09/01' and '2021/02/01'