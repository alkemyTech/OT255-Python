from transform_fac_latam import transform_fac_latam
from transform_univ_jfk import transform_univ_jfk


def transform_faclatam_ujfk():
    """Esta funcion ejecuta la funcion transform_fac_latam() y transform_univ_jfk
    para que el python callable solo ejecute esta funcion, y está ejecute las
    demas"""
    transform_fac_latam()
    transform_univ_jfk()
