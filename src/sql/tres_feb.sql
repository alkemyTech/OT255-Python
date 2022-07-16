SELECT
  ptdf.universidad AS university,
  ptdf.careers AS career,
  ptdf.fecha_de_inscripcion AS inscription_date,
    CASE
    WHEN SPLIT_PART(ptdf.names::TEXT, '_', 1) IN ('MR.', 'DR.', 'MS.', 'MRS.') THEN SPLIT_PART(names::TEXT, '_', 2)
ELSE SPLIT_PART(ptdf.names::TEXT, '_', 1)
end as first_name,
CASE 
    WHEN SPLIT_PART(ptdf.names::TEXT, '_', 1) IN ('MR.', 'DR.', 'MS.', 'MRS.') THEN SPLIT_PART(names::TEXT, '_', 3)
ELSE SPLIT_PART(ptdf.names::TEXT, '_', 2)
END AS last_name,
  ptdf.sexo AS gender,
  DATE_PART ('year', CURRENT_DATE) - DATE_PART('year', TO_DATE (ptdf.birth_dates,'YY')) AS age,
  ptdf.codigo_postal AS postal_code,
  l.localidad AS location,
  ptdf.correos_electronicos AS email
FROM public.palermo_tres_de_febrero ptdf LEFT JOIN public.localidad l
    ON CAST (ptdf.codigo_postal AS INTEGER) = l.codigo_postal
WHERE universidad='universidad_nacional_de_tres_de_febrero'
AND fecha_de_inscripcion BETWEEN '20/Sep/01' AND '21/Feb/01'