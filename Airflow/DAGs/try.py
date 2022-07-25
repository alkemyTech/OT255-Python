import sys
from pathlib import Path

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

from src.py_functions import process_univ, query_univ

query_univ.main("uba")
process_univ.main("uba")
query_univ.main("ucine")
process_univ.main("ucine")