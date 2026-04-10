import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(override = True)

def build_spark(app_name: str = "CCF") -> SparkSession:
    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", python_path)
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", python_path)
    os.environ["SPARK_LOCAL_IP"] = os.environ.get("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )