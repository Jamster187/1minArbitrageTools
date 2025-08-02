import pandas as pd
from sqlalchemy import create_engine, text

# User input section — fill these in
db_uri = "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance"
table_name = 'binance_sui_usdc:usdc_1m'  # Include colon if needed
target_timestamp = 1750359900000         # Timestamp in milliseconds
search_window_ms = 0                 # Look ± this many milliseconds (default: 1 minute)

# Create engine and connect
engine = create_engine(db_uri)

def query_nearest_record(table, target_ts, window):
    quoted_table = f'"{table}"'  # Ensure colon table names work

    query = f"""
    SELECT 
        *, 
        to_timestamp("timestamp" / 1000.0) AS human_time,
        ABS("timestamp" - :target_ts) AS time_diff
    FROM {quoted_table}
    WHERE "timestamp" BETWEEN :start_ts AND :end_ts
    ORDER BY time_diff
    LIMIT 5;
    """

    with engine.connect() as conn:
        result = pd.read_sql_query(
            text(query), 
            conn,
            params={
                "target_ts": target_ts,
                "start_ts": target_ts - window,
                "end_ts": target_ts + window
            }
        )
    
    if result.empty:
        print(f"No records found ±{window // 1000} seconds from timestamp {target_ts}")
    else:
        print(f"Top {len(result)} rows closest to {target_ts} (±{window // 1000}s):\n")
        print(result)

def find_candle(uri, table, dt, window_sec=60):
    engine = create_engine(uri)
    ts_ms = int(pd.to_datetime(dt).value // 10**6)
    with engine.connect() as conn:
        rows = conn.execute(text(f'''
        SELECT "timestamp", to_timestamp("timestamp"/1000) AS ts,
               low, high, open, close, volume,
               ABS("timestamp" - :ts) AS diff_ms
        FROM "{table}"
        WHERE "timestamp" BETWEEN :ts - :win AND :ts + :win
        ORDER BY diff_ms LIMIT 5
        '''), {"ts": ts_ms, "win": window_sec * 1000}).fetchall()
    for r in rows:
        print(r)

# Run the query
query_nearest_record(table_name, target_timestamp, search_window_ms)

# Usage
find_candle(db_uri, table_name, "2025-06-13 00:02:00", window_sec=600)
