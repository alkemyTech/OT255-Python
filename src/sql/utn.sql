SELECT
  ju.university,
  ju.career,
  ju.inscription_date,
  SPLIT_PART(ju.nombre, ' ', 1) AS first_name, SPLIT_PART(ju.nombre, ' ', 2) AS last_name,
  ju.sexo AS gender,
  EXTRACT(year from AGE(CURRENT_DATE, TO_DATE (ju.birth_date,'YYYY-MM-DD'))) AS age,
  array_agg(l.codigo_postal) as postal_code,
  location,
  ju.email
FROM 
  public.jujuy_utn ju
LEFT JOIN public.localidad l
  ON ju.location = LOWER(l.localidad)
  WHERE university='universidad tecnológica nacional'
  AND TO_DATE(inscription_date ,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01'
GROUP BY 
  id