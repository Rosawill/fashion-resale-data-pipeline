import csv
import json


class BaseExtractor:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, file_path):
        if file_path.endswith(".csv"):
            return self.spark.read.csv(file_path, header=True, inferSchema=True)
        elif file_path.endswith(".json"):
            return self.spark.read.json(file_path)
        else:
            raise ValueError("Unsupported file format")
