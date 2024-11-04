# file_checker.py

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

# สร้าง SparkSession (สามารถเปลี่ยนเป็น SparkContext ถ้ามี Spark session อยู่แล้ว)
spark = SparkSession.builder.appName("CheckMissingFiles").getOrCreate()

def check_missing_files(base_path, start_date, end_date, file_format="csv"):
    """
    ตรวจสอบไฟล์ที่หายไปใน path ที่กำหนดตั้งแต่ start_date ถึง end_date
    และคืนค่าไฟล์ที่ยังไม่มีใน path

    Parameters:
        base_path (str): Path ของไฟล์ที่ต้องการตรวจสอบ
        start_date (str): วันที่เริ่มต้นในรูปแบบ YYYY-MM-DD
        end_date (str): วันที่สิ้นสุดในรูปแบบ YYYY-MM-DD
        file_format (str): ชนิดของไฟล์ (default = 'csv')

    Returns:
        List[str]: รายการไฟล์ที่ยังไม่มีใน path
    """
    missing_files = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        file_path = f"{base_path}/{date_str}.{file_format}"
        
        # ตรวจสอบว่าไฟล์มีอยู่หรือไม่
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        
        # เพิ่มวันถัดไป
        current_date += timedelta(days=1)
    
    return missing_files
