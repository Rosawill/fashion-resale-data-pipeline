from pyspark.sql import functions as F
from src.utils.pm_helper import udf_factor_processing_costs


class ProfitMarginAnalyzer:
    """
    Based on item 'condition' and 'cost' and desired profit_margin,
            determine resale eligibility
    """

    def __init__(self, spark, desired_pm, avg_processing_cost):
        self.spark = spark
        self.conditions_allowed = ["poor", "fair", "good", "new", "like new"]
        self.desired_pm = desired_pm
        self.avg_processing_cost = avg_processing_cost

    def calculate_pm(self, data):
        data = data.withColumn(
            "pm_decrease_percentage",
            F.when(F.col("condition") == "poor", F.lit(0.4))
            .when(F.col("condition") == "fair", F.lit(0.3))
            .when(F.col("condition") == "good", F.lit(0.2))
            .when(F.col("condition") == "new", F.lit(0))
            .when(F.col("condition") == "like new", F.lit(0.0))
            .otherwise(F.lit(None)),
        )

        data_with_pm = data.withColumn(
            "profit_margin",
            udf_factor_processing_costs(
                "pm_decrease_percentage",
                F.lit(self.avg_processing_cost),
                "cost",
            ),
        )
        return data_with_pm
