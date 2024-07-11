import pyspark


class Report:
    def __init__(self, spark):
        self.spark = spark

    def update_brand_name(self, brand):
        """Turn brand name from 'this_format' to 'This Format'."""
        brand = brand.replace("_", " ").title()
        return brand

    def get_percentage(self, numerator, denominator):
        return round((numerator / denominator) * 100, 2)

    def create_report(self, brand, processed_data, pm_data, desired_pm):
        """
        print results for each brand:
                1. number of items received from brand
                2. number of items with valid data
                3. number of items with inavlid data, data completeness rate
                4. number of items with desired predicted profit margin, resale eligibility rate
        """
        brand_name = self.update_brand_name(brand)
        total_items = processed_data.count()
        valid_items = pm_data.count()
        resale_eligible_items = pm_data.filter(
            pm_data.profit_margin > desired_pm
        ).count()

        print("\n")
        print(f"{brand_name} sent us {total_items} items")
        print(f"{valid_items} items had valid data")
        print(
            f"{total_items - valid_items} items had invalid data, "
            + f"with a {self.get_percentage(valid_items, total_items)}% rate of data completeness"
        )
        print(
            f"{resale_eligible_items} items had the desired predicted profit margin of ${desired_pm}, "
            + f"with a {self.get_percentage(resale_eligible_items, valid_items)}% rate of resale eligibility among valid items"
        )

        return
