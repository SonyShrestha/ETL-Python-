"""
Author: Sony Shrestha
Date: 1 September, 2020
Description: This module gnerates logging information
"""

import logging
import os
from utilities.variables import Variables
from datetime import datetime


VAR=Variables()

# Get root directory and append logs folder in it
log_dir = os.path.join(VAR.current_dir, "logs")


# Creete logs folder if does not exist
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)


# Create log file named today's_date.log
file_name = datetime.today().strftime("%Y-%m-%d.log")


# Get full path of log file
log_file_path = os.path.join(log_dir, file_name)


# Get logging level provided in config.json file
if VAR.logging_level == "debug":
    level = logging.DEBUG
elif VAR.logging_level == "info":
    level = logging.INFO
elif  VAR.logging_level == "error":
    level = logging.ERROR
else:
    level = logging.INFO

#Logging
formatter='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'

logging.basicConfig(filename=log_file_path,format=formatter,datefmt='%Y-%m-%d:%H:%M:%S',level=level,filemode='a')

if VAR.enable_screen_log.lower()=="yes":
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter(formatter)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)

else:
    logger=logging
    
#logger.debug("This is a debug log")
#logger.info("This is an info log")
#logger.critical("This is critical")
#logger.error("An error occurred")