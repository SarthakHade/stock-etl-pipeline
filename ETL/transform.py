import pandas as pd

def transform_data(df):
    print(">>> Transforming data <<<")

    # Add stock identifier
    df['Symbol'] = 'AAPL'

    # Ensure datetime type
    df['Date'] = pd.to_datetime(df['Date'])

    # Data quality
    df = df.dropna()
    df = df.drop_duplicates()

    print(">>> Transformation complete <<<")
    return df
