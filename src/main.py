import os
import sys
from src.data.extract.base_extractor import BaseExtractor
from src.data.process.base_processor import BaseProcessor
from src.data.process.djerf_avenue_processor import DjerfAvenueProcessor
from src.data.process.oscar_de_la_renta_processor import OscarDeLaRentaProcessor
from src.reporting.create_report import Report
from src.reporting.pm_analyzer import ProfitMarginAnalyzer
from pyspark.sql import *


def main():

    spark = SparkSession.builder.appName("Resale Value Analyzer").getOrCreate()

    raw_data_dir = "../data/raw/"

    # initialize classes to extract + process data and calculated estimated profit margins
    base_extractor = BaseExtractor(spark)
    base_processor = BaseProcessor(spark)
    # with this example, we assume that each item costs $20 to process and
    #   our desired profit margin (total profit from resale item) would be $20 minimum
    pm_analyzer = ProfitMarginAnalyzer(spark, desired_pm=20, avg_processing_cost=20)
    reporter = Report(spark)

    for file_name in os.listdir(raw_data_dir):
        # get file path, brand name based on file name,
        #   and load initial data
        file_path = os.path.join(raw_data_dir, file_name)
        brand_name, _ = os.path.splitext(file_name)
        data = base_extractor.load_data(file_path)

        # remove data with missing values and normalize into
        #   common, predetermined schema
        if brand_name == "djerf_avenue":
            processor = DjerfAvenueProcessor()
        elif brand_name == "oscar_de_la_renta":
            processor = OscarDeLaRentaProcessor()
        processed_data = processor.process_data(data)

        # remove data with missing values for required columns
        #   once columns have been normalized across brands
        cleaned_data = base_processor.clean_data(processed_data)

        # predict profit margin based on quality of item
        #   and set required profit margin for resale eligibility
        pm_data = pm_analyzer.calculate_pm(cleaned_data)
        min_pm = 20

        # print results for each brand:
        #   number of items received from brand
        #   number of items with valid data
        #   number of items with invalid data
        #   number of items with desired predicted profit margin
        reporter.create_report(brand_name, processed_data, pm_data, min_pm)

    spark.stop()


if __name__ == "__main__":
    main()
