import pyspark
from pyspark.sql import functions as F


class BaseProcessor:
    def __init__(self, spark):
        self.spark = spark

    def process_data(self, data):
        return

    def clean_data(self, data):
        """Remove data with missing columns or null values
        for 'brand', 'item', 'size', 'condition' or 'cost'"""

        required_columns = ["item", "brand", "cost", "condition", "size"]
        data_cols = data.columns
        if not all(column in data_cols for column in required_columns):
            return None
        else:
            data = data.withColumn("condition", F.lower("condition"))
            return data.dropna(subset=required_columns)
