from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import StringType, FloatType


class DataQualityEnhancer:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def trim_extra_spaces(self, columns):
        """Trim extra spaces from specified columns."""
        for column in columns:
            self.dataframe = self.dataframe.withColumn(column, trim(col(column)))
        return self

    def remove_duplicates(self, key_columns):
        """Remove duplicate rows based on key columns."""
        self.dataframe = self.dataframe.dropDuplicates(key_columns)
        return self

    def canonicalize_data(self, column, mapping_function):
        """Apply a canonicalization function to a column."""
        self.dataframe = self.dataframe.withColumn(column, mapping_function(col(column)))
        return self

    def run_quality_checks(self):
        """Placeholder for running predefined data quality checks."""
        # Implement specific data quality checks (nulls, data type validations, etc.)
        return self

    def get_dataframe(self):
        """Return the enhanced DataFrame."""
        return self.dataframe

