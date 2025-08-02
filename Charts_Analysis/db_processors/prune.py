import psycopg2
from psycopg2 import sql

# Database connection settings
DB_CONFIG = {
    "dbname": "Testing_Data_Collection_Kucoin",  # Change this to your database name
    "user": "postgres",  # Change this to your username
    "password": "",  # Change this to your password
    "host": "localhost",  # Change this to your database host
    "port": "5432",  # Change this if using a non-default port
}

# Define the cutoff timestamp in Unix Epoch milliseconds (October 8, 2024)
CUTOFF_TIMESTAMP_MS = 1753016430000
CUTOFF_TIMESTAMP = CUTOFF_TIMESTAMP_MS / 1000  # Convert to seconds for TIMESTAMP type

def delete_old_data():
    """Deletes rows from all tables where timestamp is older than the cutoff."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Identify tables with BIGINT (Epoch timestamp) or TIMESTAMP column
        cursor.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema') -- Exclude system tables
            AND ((data_type = 'bigint' AND column_name ILIKE '%timestamp%')
                 OR (data_type IN ('timestamp without time zone', 'timestamp with time zone')));
        """)

        tables = cursor.fetchall()

        if not tables:
            print("No tables with timestamp columns found.")
            return

        for schema, table, column, data_type in tables:
            table_full_name = f"{schema}.{table}"
            print(f"Processing table: {table_full_name}, Column: {column}, Type: {data_type}")

            if data_type == "bigint":
                cutoff_value = CUTOFF_TIMESTAMP_MS  # Epoch in milliseconds
                delete_query = sql.SQL("DELETE FROM {}.{} WHERE {} < {} RETURNING *").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(column),
                    sql.Literal(cutoff_value)
                )
            else:
                cutoff_value = f"TO_TIMESTAMP({CUTOFF_TIMESTAMP})"  # Convert to TIMESTAMP
                delete_query = sql.SQL("DELETE FROM {}.{} WHERE {} < {}::TIMESTAMP RETURNING *").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(column),
                    sql.Literal(cutoff_value)
                )

            # Execute deletion in batches to avoid performance issues
            while True:
                cursor.execute(delete_query)
                deleted_rows = cursor.rowcount
                conn.commit()

                if deleted_rows == 0:
                    break  # Stop if no more rows match the condition

                print(f"Deleted {deleted_rows} rows from {table_full_name}")

        print("Old data deletion completed.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    delete_old_data()
