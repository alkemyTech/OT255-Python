
select university, 
       career, 
       arreglar_fecha(inscription_date) as inscription_date, 
       obtener_nombre(nombre) as first_name,
       obtener_apellido(nombre) as last_name,
       sexo as gender,
       calcular_edad(birth_date) as age,
       obtener_codigo_postal(location) as postal_code,
       upper(location) as location,  
       email
from  jujuy_utn 
where university = 'universidad nacional de jujuy' and
      inscription_date between '2020-09-01' and '2021-02-01';

 
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
   localidad_requerida := upper(localidad_requerida);
   select codigo_postal into codigo_postal_buscado from localidad 
   where localidad = localidad_requerida;

   return codigo_postal_buscado;
end;
$$;

create function obtener_nombre(nombres varchar)
returns varchar
language plpgsql
as
$$
declare nombre_buscado varchar;
declare lista_de_nombres varchar[];
begin
   lista_de_nombres := string_to_array(nombres, ' ');
   IF array_length(lista_de_nombres,1) = 3 THEN
      nombre_buscado := lista_de_nombres[2];
   else
      nombre_buscado := lista_de_nombres[1];
   END IF;
  
   return nombre_buscado;
end;
$$;

create function obtener_apellido(nombres varchar)
returns varchar
language plpgsql
as
$$
declare apellido varchar;
declare lista_de_nombres varchar[];
begin
   lista_de_nombres := string_to_array(nombres, ' ');
   IF array_length(lista_de_nombres,1) = 3 THEN
      apellido := lista_de_nombres[3];
   else
      apellido := lista_de_nombres[2];
   END IF;
  
   return apellido;
end;
$$;

create function calcular_edad(birth_date varchar)
returns int
language plpgsql
as
$$
declare edad integer;
begin
   select DATE_PART('year',age(now(),arreglar_fecha(birth_date))) 
   into edad;
   return edad;
end;
$$;



