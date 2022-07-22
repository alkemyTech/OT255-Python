
select university, 
       career, 
       to_date(inscription_date,'YYYY/MM/DD') as inscription_date,
       case
       when array_length(string_to_array(nombre, ' '),1) = 3 
          then split_part(nombre,' ', 2)
          else split_part(nombre,' ', 1)
       end as first_name, 
       case 
       when array_length(string_to_array(nombre, ' '),1) = 3 
          then split_part(nombre,' ', 3)
          else split_part(nombre,' ', 2)
       end as last_name, 
       sexo as gender,
       date_part('year',age(now(),to_date(birth_date,'YYYY/MM/DD'))) as age,
       array_agg(codigo_postal) as postal_code,
       upper(location) as location,
       email
from 
      jujuy_utn left join localidad 
      on upper(location) = localidad
where university = 'universidad nacional de jujuy' 
      and to_date(inscription_date,'YYYY/MM/DD') 
      between '2020-09-01' and '2021-02-01'
group by id;