import allure

from tests.fabric.test_csv_driven_bronze_to_silver_validation import (
    TestCSVDrivenETLValidation as _BaseCSVDrivenETLValidation,
)


@allure.epic("ETL Testing Framework")
@allure.feature("CSV-Driven Silver To Gold Validation")
class TestCSVDrivenSilverToGoldValidation(_BaseCSVDrivenETLValidation):
    """CSV-driven ETL validation for Silver to Gold."""

    CSV_FILE = "data/etl_validation_silver_to_gold_tests.csv"
    SOURCE_LAYER = "SILVER"
    TARGET_LAYER = "GOLD"
