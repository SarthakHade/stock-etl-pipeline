from extract import extract_data
from transform import transform_data

def run_pipeline():
    raw_df = extract_data()
    clean_df = transform_data(raw_df)
    print(">>> Skipping MySQL load in Docker <<<")

if __name__ == "__main__":
    run_pipeline()
