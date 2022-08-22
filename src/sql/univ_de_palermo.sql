SELECT
        universidad AS university,
        careers AS career,
        TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') AS inscripcion_date,
        CASE WHEN ARRAY_LENGTH(string_to_array(names, ' '),1) = 3
            THEN SPLIT_PART(names, '_', 2)
            ELSE SPLIT_PART(names, '_', 1)
        end as first_name,
        CASE WHEN ARRAY_LENGTH(string_to_array(names, ' '),1) = 3
            THEN SPLIT_PART(names, '_', 3)
            ELSE SPLIT_PART(names, '_', 2)
        END AS last_name,
        sexo AS gender,
        CASE WHEN CAST(SUBSTRING(birth_dates,8,2) AS INTEGER) >= 00 AND
                  CAST(SUBSTRING(birth_dates,8,2) AS INTEGER) <= 07
            THEN
                DATE_PART('year',AGE(
                    TO_DATE(CONCAT(
                        SUBSTRING(birth_dates,1,7),'20',SUBSTRING(birth_dates,8,2)),
                        'DD/Mon/YYYY')))
            ELSE
                DATE_PART('year',AGE(
                    TO_DATE(CONCAT(SUBSTRING(birth_dates,1,7),'19',SUBSTRING(birth_dates,8,2)),
                        'DD/Mon/YYYY')))
        END AS age,
        palermo_tres_de_febrero.codigo_postal AS postal_code,
        localidad AS location,
        correos_electronicos AS email
FROM
     palermo_tres_de_febrero LEFT JOIN localidad
     ON CAST(palermo_tres_de_febrero.codigo_postal AS INTEGER) = localidad.codigo_postal
WHERE universidad = 'universidad_nacional_de_tres_de_febrero'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') BETWEEN '01/09/20' AND '01/02/21';