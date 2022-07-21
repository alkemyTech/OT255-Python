
select university, 
       career, 
       fecha_de_inscripcion, 
       obtener_nombre(nombres) as first_name,
       obtener_apellido(nombres) as last_name,
       sexo as genero,
       obtener_codigo_postal(localidad) as codigo_postal,
       localidad, 
       calcular_edad(fecha_de_nacimiento) as edad, 
       email
from ( select *, 
              arreglar_fecha(inscription_date) as fecha_de_inscripcion,
              arreglar_fecha(birth_date) as fecha_de_nacimiento,
              string_to_array(nombre, ' ') as nombres,
              upper(location) as localidad 
       from jujuy_utn 
       where university = 'universidad nacional de jujuy') 
       as univ_nacional_de_jujuy
where fecha_de_inscripcion between '2020-09-01' and '2021-02-01';


create function arreglar_fecha(fecha varchar)
returns date
language plpgsql
as
$$
declare nueva_fecha date;
begin
   select to_date(fecha,'YYYY/MM/DD') into nueva_fecha;
   return nueva_fecha;
end;
$$;

create function obtener_codigo_postal(localidad_requerida varchar)
returns int
language plpgsql
as
$$
declare codigo_postal_buscado int;
begin
   select codigo_postal into codigo_postal_buscado from localidad 
   where localidad = localidad_requerida;

   return codigo_postal_buscado;
end;
$$;

create function obtener_nombre(nombres varchar[])
returns varchar
language plpgsql
as
$$
declare nombre_buscado varchar;
begin
   IF array_length(nombres,1) = 3 THEN
      nombre_buscado := nombres[2];
   else
      nombre_buscado := nombres[1];
   END IF;
  
   return nombre_buscado;
end;
$$;

create function obtener_apellido(nombres varchar[])
returns varchar
language plpgsql
as
$$
declare apellido varchar;
begin
   IF array_length(nombres,1) = 3 THEN
      apellido := nombres[3];
   else
      apellido := nombres[2];
   END IF;
  
   return apellido;
end;
$$;

create function calcular_edad(fecha_de_nacimiento date)
returns int
language plpgsql
as
$$
declare edad integer;
begin
   select DATE_PART('year',age(now(),fecha_de_nacimiento)) 
   into edad;
   return edad;
end;
$$;



