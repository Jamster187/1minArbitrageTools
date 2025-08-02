import psycopg2

# Define your connection settings for the default database (e.g., 'postgres')
conn_info = {
    "dbname": "postgres",  # Default database like 'postgres'
    "user": "postgres",  # Your PostgreSQL username
    "password": "!!!",  # Your PostgreSQL password
    "host": "localhost",  # Change if needed
    "port": "5432"  # Change if needed
}

# Function to count rows in a table
def count_rows_in_table(conn, table_name):
    try:
        cur = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name};"
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        return result[0]
    except Exception as e:
        print(f"Error counting rows in table {table_name}: {e}")
        return 0

# Function to count rows in all tables in a database
def count_rows_in_database(database_name):
    try:
        conn_info["dbname"] = database_name
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()

        # Get all tables in the 'public' schema
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cur.fetchall()

        total_rows = 0
        for table in tables:
            table_name = table[0]
            print(f"Counting rows in table {table_name} of database {database_name}")
            total_rows += count_rows_in_table(conn, table_name)

        cur.close()
        conn.close()
        return total_rows

    except Exception as e:
        print(f"Error connecting to {database_name}: {e}")
        return 0

# Main logic to count rows across all databases
def count_all_rows():
    try:
        # Connect to the default 'postgres' database
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()

        # Get list of all databases (excluding template databases)
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cur.fetchall()

        total_rows = 0
        for db in databases:
            db_name = db[0]
            print(f"Counting rows in database: {db_name}")
            total_rows += count_rows_in_database(db_name)

        cur.close()
        conn.close()

        print(f"Total number of rows across all databases: {total_rows}")

    except Exception as e:
        print(f"Error: {e}")

# Run the script
count_all_rows()
