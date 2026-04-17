import json
import logging
from datetime import date 

logger=logging.getLogger(__name__) # to create a logger object for the current module, which can be used to log messages with different severity levels (e.g., info, warning, error) throughout the code.

def load_data(): 
    """This function is responsible for loading the data from the specified file path and returning it as a Python object. It also includes error handling to manage potential issues with file access or JSON decoding."""
    file_path=f"./data/yt_data_{date.today()}.json"
    
    try:
        logger.info(f"Processing file: YT_data{date.today()}")# to log an informational message indicating that the file is being processed, which can be helpful for tracking the progress of the data loading process.
        
        with open(file_path,"r",encoding="utf-8") as raw_data:
            data=json.load(raw_data)
        return data
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")# to log an error message if the specified file is not found, which can help identify issues with file paths or missing data files.
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from file: {file_path}")
        raise e    