# file_checker.py

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

spark = SparkSession.builder.appName("CheckMissingFiles").getOrCreate()

def check_missing_files(base_path, start_date, end_date, file_format="parquet"):
    """
    Check for missing files in the specified path from start_date to end_date
    and restore files that do not exist in the path.

    Parameters:
        base_path (str): Path ของไฟล์ที่ต้องการตรวจสอบ
        start_date (str):  YYYY-MM-DD
        end_date (str):  YYYY-MM-DD
        file_format (str): (default = 'parquet')

    Returns:
        List[str]: List of files that do not yet exist in the path
    """
    missing_files = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        file_path = f"{base_path}/{date_str}.{file_format}"
        
        # Check if the file exists
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        
        # Add next day
        current_date += timedelta(days=1)
    
    return missing_files
