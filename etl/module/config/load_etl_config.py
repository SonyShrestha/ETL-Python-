import pandas as pd
from utilities.variables import Variables

VAR=Variables()

def get_config():
    """Returns ETL config dictionary"""
    try:
        sql="""select * from {}.{}""".format(VAR.score_db, VAR.etl_config)
        config=pd.read_sql(sql,con=VAR.mysql_engine)

        etl_config={
            "start_date":config["start_date"][0],
            "end_date":config["end_date"][0],
            "date_format":config["source_date_format"][0],
            "schedule_interval":config["schedule_interval"][0],
            "chunk_size":config["chunk_size"][0]
        }

        return etl_config

    except Exception as e:
        raise e
