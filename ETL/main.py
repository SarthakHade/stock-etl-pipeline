import logging
from extract import extract_data
from transform import transform_data
from load import load_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("ETL pipeline started")

    df = extract_data()
    logger.info(f"Extracted {len(df)} records")

    clean_df = transform_data(df)
    logger.info(f"Transformed data, rows: {len(clean_df)}")

    load_data(clean_df)
    logger.info("Data loaded successfully")

    logger.info("ETL pipeline finished")

if __name__ == "__main__":
    run_pipeline()
