import pandas as pd
from utilities.variables import Variables

VAR=Variables()

def get_config():
    try:
        sql="""select * from {}.{}""".format(VAR.score_db, VAR.tables_config)
        config=pd.read_sql(sql,con=VAR.mysql_engine,index_col="name")

        return config.to_dict(orient="index")
        
    except Exception as err:
        raise err