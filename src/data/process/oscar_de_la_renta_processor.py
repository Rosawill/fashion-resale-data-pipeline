from src.data.process.base_processor import BaseProcessor


class OscarDeLaRentaProcessor(BaseProcessor):
    def __init__(self):
        return

    def process_data(self, data):
        processed_df = data.withColumnRenamed("full_price", "cost")
        return processed_df
