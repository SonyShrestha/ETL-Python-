""" Author: Sony Shrestha
Description: Initialize variables from config file
Date: 10th October, 2020
"""

import json
import os
from sqlalchemy import create_engine
#from sqlalchemy.engine.url import URL

class Variables:
    def __init__(self):
        try:
            # Get current directory
            self.current_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

            # Add config/config.json
            config_dir_path=os.path.join(self.current_dir,'config')
            config_file_path=os.path.join(config_dir_path,'config.json')

            # Reading configuration
            with open(config_file_path, "r") as file:
                self.config = json.load(file)

            # mysql database Connection
            self.mysql_engine = self.get_mysql_connection()
            
            # oracle conection
            self.oracle_engine=self.get_oracle_connection()
            
            # logging 
            self.logging_level=self.config["LOGGING_LEVEL"]
            self.enable_screen_log=self.config["ENABLE_SCREEN_LOG"]

            # RW Tables
            self.rw_db = self.config["RW"]["db"]
            self.account_master_table = self.config["RW"]["account_master"]
            self.loan_master_table=self.config["RW"]["bank_loan"]
            self.transaction_table = self.config["RW"]["transaction"]
            self.daily_summary_table = self.config["RW"]["daily_summary"]

            # score table
            self.score_db=self.config["CREDIT_SCORE"]["db"]
            self.etl_config = self.config["CREDIT_SCORE"]["etl_config"]
            self.tables_config = self.config["CREDIT_SCORE"]["etl_table_config"]

        except Exception as err:
            raise Exception(err)


    def get_mysql_connection(self):
        """Creates mysql database connection."""
        mysql_con=self.config["DB_CONNECTION"]["MYSQL"]
        mysql_con_url=("{driver}://{username}:{password}@{hostname}")

        try:
            engine=create_engine(mysql_con_url.format(driver=mysql_con["drivername"],
            username=mysql_con["username"],
            password=mysql_con["password"],
            hostname=mysql_con["host"]))
            return engine

        except ConnectionError as err:
            raise err


    def get_oracle_connection(self):
        """Create oracle database connection."""
        oracle_con = self.config["DB_CONNECTION"]["ORACLE"]
        oracle_con_url = ("{driver}://{username}:{password}@(DESCRIPTION = "
                          "(LOAD_BALANCE=on) (FAILOVER=ON) "
                          "(ADDRESS = (PROTOCOL = TCP)(HOST = {hostname})(PORT = {port})) "
                          "(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = {SID})))")

        try:
            engine = create_engine(oracle_con_url.format(username=oracle_con['username'],
                                                         password=oracle_con['password'],
                                                         hostname=oracle_con['host'],
                                                         port=oracle_con['port'],
                                                         SID=oracle_con['sid_name'],
                                                         driver=oracle_con['drivername']))
            return engine

        except ConnectionError as err:
            raise err


    def __getitem__(self, key):
        """Get the value from key."""
        return self.config[key]



if __name__ == "__main__":
    VAR = Variables()
