from pyspark.sql import types as T
from pyspark.sql.functions import udf


def factor_processing_costs(pm_decrease_percent, avg_processing_cost, cost):
    item_value = (1 - pm_decrease_percent) * cost
    return item_value - avg_processing_cost


udf_factor_processing_costs = udf(factor_processing_costs, T.FloatType())
