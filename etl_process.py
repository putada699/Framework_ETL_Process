# etl_process.py

from datetime import datetime
from file_checker import check_missing_files

def etl_process():
    base_path = "/path/to/your/files"
    start_date = "2024-10-01"
    end_date = datetime.now().strftime("%Y-%m-%d")  # cell current date

    # get missing date
    missing_files = check_missing_files(base_path, start_date, end_date)

    if missing_files:
        print("Missing files detected:", missing_files)

    else:
        print("All files are present. Proceeding with ETL.")

# get main funtion
if __name__ == "__main__":
    etl_process()
