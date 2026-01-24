import time
time.sleep(15)

import mysql.connector

def load_data(df):
    print(">>> RUNNING DIMENSIONAL + INCREMENTAL LOAD INTO stock_dw <<<")

    conn = mysql.connector.connect(
        host="mysql",
        user="nobita",
        password="1a2s@3d4f",
        database="stock_dw"
    )

    cursor = conn.cursor()

    # --------------------------------------------------
    # STEP 1: GET WATERMARK (LAST LOADED DATE)
    # --------------------------------------------------
    cursor.execute("""
        SELECT MAX(d.full_date)
        FROM fact_stock_prices f
        JOIN dim_date d ON f.date_id = d.date_id
    """)
    result = cursor.fetchone()
    last_loaded_date = result[0]
    import pandas as pd
    last_loaded_date = pd.to_datetime(last_loaded_date)


    if last_loaded_date:
        print("Last loaded date:", last_loaded_date)
        # Incremental filter
        df = df[df['Date'] > last_loaded_date]
    else:
        print("No previous data found. Running full load.")

    # --------------------------------------------------
    # STEP 2: HANDLE NO-NEW-DATA CASE
    # --------------------------------------------------
    if df.empty:
        print("No new data to load.")
        cursor.close()
        conn.close()
        return

    # --------------------------------------------------
    # STEP 3: DIMENSIONAL + FACT LOADING
    # --------------------------------------------------
    fact_rows = []

    for _, row in df.iterrows():

    # -------- DATE DIMENSION --------
        cursor.execute(
        "SELECT date_id FROM dim_date WHERE full_date = %s",
        (row['Date'],)
    )
    result = cursor.fetchone()

    if result is None:
        cursor.execute(
            """
            INSERT INTO dim_date
            (full_date, day, month, year, quarter, weekday)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                row['Date'],
                row['Date'].day,
                row['Date'].month,
                row['Date'].year,
                (row['Date'].month - 1) // 3 + 1,
                row['Date'].strftime('%A')
            )
        )
        date_id = cursor.lastrowid
    else:
        date_id = result[0]

    # -------- STOCK DIMENSION --------
        cursor.execute(
        "SELECT stock_id FROM dim_stock WHERE stock_symbol = %s",
        (row['Symbol'],)
    )
    result = cursor.fetchone()

    if result is None:
        cursor.execute(
            "INSERT INTO dim_stock (stock_symbol) VALUES (%s)",
            (row['Symbol'],)
        )
        stock_id = cursor.lastrowid
    else:
        stock_id = result[0]

    # -------- COLLECT FACT ROW --------
        fact_rows.append((
        date_id,
        stock_id,
        row['Open'],
        row['Close'],
        row['High'],
        row['Low'],
        row['Volume']
    ))

# -------- BATCH UPSERT INTO FACT TABLE --------
        cursor.executemany(
    """
    INSERT INTO fact_stock_prices
    (date_id, stock_id, open_price, close_price, high_price, low_price, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        open_price = VALUES(open_price),
        close_price = VALUES(close_price),
        high_price = VALUES(high_price),
        low_price = VALUES(low_price),
        volume = VALUES(volume)
    """,
        fact_rows
)



    conn.commit()
    cursor.close()
    conn.close()

    print("Incremental load completed successfully.")
