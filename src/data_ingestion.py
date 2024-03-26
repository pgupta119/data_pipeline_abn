from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from logger import setup_logger  # Make sure to have this import statement if your logger setup is in a different file
from constants import LOG_FILE
# Set up the logger
logger = setup_logger(LOG_FILE)

def read_csv(spark: SparkSession, filepath: str) -> DataFrame:
    try:
        logger.info(f"Reading CSV file from path: {filepath}")
        df_csv = spark.read.csv(filepath, header=True, inferSchema=True)
        logger.info("CSV file read successfully")
        return df_csv
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise e

def read_json(spark: SparkSession, filepath: str) -> DataFrame:
    try:
        # Define the schema for the inner "stocks" array
        stocksSchema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        # Define the schema for the entire document
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("stocks", ArrayType(stocksSchema), True)
        ])

        logger.info(f"Reading JSON file from path: {filepath}")
        df_json = spark.read.schema(schema).json(filepath, multiLine=True)
        logger.info("JSON file read successfully")
        return df_json
    except Exception as e:
        logger.error(f"Error reading JSON file: {str(e)}")
        raise e
