from src.data.process.base_processor import BaseProcessor


class DjerfAvenueProcessor(BaseProcessor):
    def __init__(self):
        return

    def process_data(self, data):
        processed_df = data.withColumnRenamed("title", "item").withColumnRenamed(
            "quality", "condition"
        )
        return processed_df
