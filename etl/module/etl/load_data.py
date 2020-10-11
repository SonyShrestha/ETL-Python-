import pandas as pd
from datetime import datetime,timedelta
from utilities.logger import *
from utilities.variables import Variables
from module.config import load_etl_config
from module.config import load_tables_config


VAR=Variables()
tables_config=load_tables_config.get_config()

etl_config=load_etl_config.get_config()
etl_start_date=etl_config["start_date"]
etl_end_date=etl_config["end_date"]
etl_date_format=etl_config["date_format"]
etl_schedule_interval= etl_config["schedule_interval"]
etl_chunk_size= etl_config["chunk_size"]
etl_date_range= etl_start_date,etl_end_date
etl_date= [datetime(dt.year,dt.month,dt.day) for dt in etl_date_range]
date_range = [dt.strftime(etl_date_format) for dt in etl_date]


def load_master():
    logger.info("Loading account master data")
    try:
        if tables_config["account_master"]["source_schema"] is None:
            table_name=tables_config["account_master"]["source_table"]
        else:
            table_name=tables_config["account_master"]["source_schema"]+"."+tables_config["account_master"]["source_table"]

        if tables_config["account_master"]["incremental"]=="yes":
            sql_query="""select ACCOUNT_NUMBER,
            CUSTOMER_CODE,BRANCH,PRODUCT,PRODUCT_CATEGORY,ACC_OPEN_DATE,'SYSTEM' AS CREATED_BY,
            case when ACTIVE_FLAG='ACTIVE' THEN 1 ELSE 0 END AS ACTIVE_FLAG,ACC_CLOSED_DATE FROM {}
            where {} between to_date('{}','dd-mm-yyyy')
            and to_date('{}','dd-mm-yyyy')
            """.format(table_name,tables_config["account_master"]["date_col"],date_range[0],date_range[1])
        
        if tables_config["account_master"]["incremental"]=="no":
            truncate_table_query="""truncate table {}.{}""".format(VAR.rw_db,VAR.account_master_table)
            VAR.mysql_engine.execute(truncate_table_query)

            sql_query="""select ACCOUNT_NUMBER,
            CUSTOMER_CODE,
            BRANCH,PRODUCT,PRODUCT_CATEGORY,ACC_OPEN_DATE,'SYSTEM' AS CREATED_BY,
            case when ACTIVE_FLAG='ACTIVE' THEN 1 ELSE 0 END AS ACTIVE_FLAG,ACC_CLOSED_DATE FROM {})
            """.format(table_name)

        logger.info(sql_query)
        for account_master_df in pd.read_sql_query(sql=sql_query,con=VAR.oracle_engine, chunksize=etl_chunk_size):
                account_master_df.to_sql(VAR.account_master_table, schema=VAR.rw_db,
                                        con=VAR.mysql_engine, index=False,
                                        if_exists='append')
                
    except Exception as err:
        raise err


def load_transaction():
    try:
        if tables_config["account_master"]["source_schema"] is None:
            table_name=tables_config["transaction_base"]["source_table"]
        else:
            table_name=tables_config["transaction_base"]["source_schema"]+"."+tables_config["transaction_base"]["source_table"]

        if tables_config["transaction_base"]["incremental"]=="yes":
            sql_query="""select TRAN_DATE,ACCOUNT_NUMBER,
            BRANCH,PRODUCT,AMOUNT as LCY_AMOUNT,TRAN_CODE as transaction_code,DESCRIPTION AS description1,
            case when DC_INDICATOR='D' THEN 'withdraw' else 'deposit' end as dc_indicator
            FROM {}
            where {} between to_date('{}','dd-mm-yyyy')
            and to_date('{}','dd-mm-yyyy')
            """.format(table_name,tables_config["transaction_base"]["date_col"],date_range[0],date_range[1])
        
        if tables_config["transaction_base"]["incremental"]=="no":
            truncate_table_query="""truncate table {}.{}""".format(VAR.rw_db,VAR.transaction_table)
            VAR.mysql_engine.execute(truncate_table_query)

            sql_query="""select TRAN_DATE,ACCOUNT_NUMBER,
            BRANCH,PRODUCT,AMOUNT as LCY_AMOUNT,TRAN_CODE as transaction_code,DESCRIPTION AS description1,
            case when DC_INDICATOR='D' THEN 'withdraw' else 'deposit' end as dc_indicator
            FROM {})
            """.format(table_name)

        for account_master_df in pd.read_sql_query(sql=sql_query,con=VAR.oracle_engine, chunksize=etl_chunk_size):
                account_master_df.to_sql(VAR.transaction_table, schema=VAR.rw_db,
                                        con=VAR.mysql_engine, index=False,
                                        if_exists='append')
    except Exception as err:
        raise err


def load_summary():
    try:
        if tables_config["balance_summary"]["source_schema"] is None:
            table_name=tables_config["balance_summary"]["source_table"]
        else:
            table_name=tables_config["balance_summary"]["source_schema"]+"."+tables_config["balance_summary"]["source_table"]

        if tables_config["balance_summary"]["incremental"]=="yes":
            sql_query="""select TRAN_DATE,ACCOUNT_NUMBER,
            BRANCH,PRODUCT,LCY_AMOUNT
            FROM {}
            where {} between to_date('{}','dd-mm-yyyy')
            and to_date('{}','dd-mm-yyyy')
            """.format(table_name,tables_config["balance_summary"]["date_col"],date_range[0],date_range[1])
        
        if tables_config["balance_summary"]["incremental"]=="no":
            truncate_table_query="""truncate table {}.{}""".format(VAR.rw_db,VAR.daily_summary_table)
            VAR.mysql_engine.execute(truncate_table_query)

            sql_query="""select TRAN_DATE,ACCOUNT_NUMBER,
            BRANCH,PRODUCT,LCY_AMOUNT
            FROM {}
            """.format(table_name)

        for account_master_df in pd.read_sql_query(sql=sql_query,con=VAR.oracle_engine, chunksize=etl_chunk_size):
                account_master_df.to_sql(VAR.daily_summary_table, schema=VAR.rw_db,
                                        con=VAR.mysql_engine, index=False,
                                        if_exists='append')
    except Exception as err:
        raise err


def load_loan():
    try:
        if tables_config["loan"]["source_schema"] is None:
            table_name=tables_config["loan"]["source_table"]
        else:
            table_name=tables_config["loan"]["source_schema"]+"."+tables_config["loan"]["source_table"]

        
        truncate_table_query="""truncate table {}.{}""".format(VAR.rw_db,VAR.loan_master_table)
        VAR.mysql_engine.execute(truncate_table_query)

        sql_query="""SELECT customer_code,
        MAX(CASE WHEN is_active = 1 and provision_type = 0 THEN 3
        WHEN is_active = 1 and provision_type = 1 THEN 2.4
        WHEN is_active = 1 and provision_type = 2 THEN 1.8
        WHEN is_active = 1 and provision_type = 3 THEN 1.2
        WHEN is_active = 1 and provision_type = 4 THEN 0.6
        WHEN provision_type = 5 THEN 0
        ELSE 0 END) as has_loan, count(1) as loan_count,
        MAX(CASE WHEN provision_type = 5 THEN 1 ELSE 0 END) as is_bad,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as payment_completed_count,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_loan,
        SUM(penalty_count) as penalty_number
        FROM {} GROUP BY customer_code
        """.format(table_name)

        for account_master_df in pd.read_sql_query(sql=sql_query,con=VAR.oracle_engine, chunksize=etl_chunk_size):
                account_master_df.to_sql(VAR.loan_master_table, schema=VAR.rw_db,
                                        con=VAR.mysql_engine, index=False,
                                        if_exists='append')
    except Exception as err:
        raise err

def update_schedule():
    next_start_date=etl_date[0]+timedelta(1)
    next_end_date=next_start_date+timedelta(int(etl_schedule_interval)-1)
   
    prev_sql="""update {}.{} set prev_start_date='{}',prev_end_date='{}',start_date='{}',end_date='{}'
     where id=1""".format(VAR.rw_db,
    VAR.etl_config,etl_start_date,etl_end_date,next_start_date,next_end_date)
    
    VAR.mysql_engine.execute(prev_sql)



def main(action):
    try:
        if action=='etl':
            load_master()
            load_transaction()
            load_summary()
            load_loan()
            update_schedule()
        if action=='load_master':
            load_master()
        if action=='load_transaction':
            load_transaction()
        if action=='load_summary':
            load_summary()
        if action=='load_loan':
            load_loan()

    except Exception as err:
        raise err
    

if __name__ == "__main__":
    main(action)