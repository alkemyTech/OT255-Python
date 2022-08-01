<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py
<<<<<<< HEAD
=======
>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py
def query_univ(univ):
    import datetime
    import os
    import shelve
    from pathlib import Path
<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py

    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    assert isinstance(univ, str), "University acronym must be string type."
    
    # -- INITIAL CONFIG --
=======
import datetime
import os
import shelve
import sys
from pathlib import Path
=======
>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py

    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    assert isinstance(univ, str), "University acronym must be string type."
    
    # -- INITIAL CONFIG --
<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py
    # set the repository main folder as cwd
    os.chdir(Path(sys.path[0]) / "..")

>>>>>>> ccb5a7c (UC-UBA combine tasks for each university in one script)
=======
>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py
    # set the name of the university as a variable to simplify code reuse.
    univ_name = univ
    # set the destination path as a variable to simplify code reuse.
    raw_path = Path.cwd() / "files/raw"

    # check if destination and temp folders exists. create them if not.
    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)
    if not os.path.isdir(raw_path / "../temp"):
        os.makedirs(raw_path / "../temp")

<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py
<<<<<<< HEAD
    print(Path.cwd())

=======
>>>>>>> ccb5a7c (UC-UBA combine tasks for each university in one script)
=======
    print(Path.cwd())

>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py
    # -- ENVIRONMENT VARIABLES --
    # load environment variables with python-dotenv module.
    load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    # -- SCRIPT --
    # read the sql script into a local variable.
<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py
<<<<<<< HEAD
    query = open(f"./include/sql/query_{univ_name}.sql", "r")
=======
    query = open(f"src/sql/query_{univ_name}.sql", "r")
>>>>>>> ccb5a7c (UC-UBA combine tasks for each university in one script)
=======
    query = open(f"./include/sql/query_{univ_name}.sql", "r")
>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py
    # create engine to make the connection with the database.
    engine = create_engine(
        f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    # read query result into a pandas dataframe.
    df_query = pd.read_sql_query(query.read(), engine)
    # incorporate date info to the file name.
    today = datetime.datetime.now()
    file_name = "_".join(
        [
            univ_name,
<<<<<<< HEAD
<<<<<<< HEAD
            "-".join(
                [str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]
            ),
=======
            "-".join([str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]),
>>>>>>> ccb5a7c (UC-UBA combine tasks for each university in one script)
=======
            "-".join(
                [str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]
            ),
>>>>>>> aeb2904 (UC-UBA load scripts as modules for DAG tasks)
        ]
    )
    # write query result into a csv file inside the destination folder.
    df_query.to_csv(raw_path / "".join([file_name, ".csv"]), index=False)
    # save file name of the last file with shelve module.
    shelf_file = shelve.open(f"{raw_path}/../temp/last_file")
    shelf_file[f"{univ_name}_filename"] = file_name
    shelf_file.close()

<<<<<<< HEAD:Airflow/src/py_functions/query_univ.py
<<<<<<< HEAD
<<<<<<< HEAD
    print("Query task finished")
=======
=======

>>>>>>> aeb2904 (UC-UBA load scripts as modules for DAG tasks)
if __name__ == "__main__":
    main()
>>>>>>> ccb5a7c (UC-UBA combine tasks for each university in one script)
=======
    print("Query task finished")
>>>>>>> 9a2c7c5 (UC-UBA fix dags and callables for both univerisities: working):src/py_functions/query_univ.py
