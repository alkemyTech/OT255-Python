import pandas as pd
from pathlib import Path


def manip_pandas(university):
    # Lectura del CSV
    path_csv = (
        Path(__file__).parent.parent.parent / "files" / "raw" / (university + ".csv")
    )
    df = pd.read_csv(path_csv)

    return df
