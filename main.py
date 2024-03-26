from pyspark.sql import SparkSession
from src.data_ingestion import read_csv, read_json

from src.data_processing import calculate_collateral_value
from src.data_storage import save_as_parquet
from logger import setup_logger  # Import the setup_logger function
import warnings
from constants import LOG_FILE
# Ignore all warnings for better clarity
warnings.simplefilter('ignore')

def main():
    # Initialize logger

    # Set up the logger
    logger = setup_logger(LOG_FILE)

    # Start Spark session
    spark = SparkSession.builder.appName("DataPipeline").getOrCreate()
    logger.info("Spark session started")

    # Paths to data
    clients_path = "data/input_data/Clients.csv"
    collaterals_path = "data/input_data/Collaterals.csv"
    stocks_path = "data/input_data/Stocks.json"
    output_path = "data/output_data/collateral_status.parquet"

    # Ingest data
    logger.info("Ingesting data")
    clients_df = read_csv(spark, clients_path)
    collaterals_df = read_csv(spark, collaterals_path)
    stocks_df = read_json(spark, stocks_path)

    # Process data
    logger.info("Calculating collateral value")
    collateral_status_df = calculate_collateral_value(clients_df, collaterals_df, stocks_df)
    df = spark.createDataFrame(collateral_status_df)

    # Save data
    logger.info(f"Saving results to {output_path}")
    save_as_parquet(df, output_path)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
