import psycopg2

# Define your connection settings for the default database (like 'postgres')
conn_info = {
    "dbname": "postgres",  # Default database like 'postgres'
    "user": "postgres",  # Your PostgreSQL username
    "password": "!!!",  # Your PostgreSQL password
    "host": "localhost",  # Change if needed
    "port": "5432"  # Change if needed
}

# Function to count tables in a database
def count_tables_in_database(database_name):
    try:
        conn_info["dbname"] = database_name
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0]
    except Exception as e:
        print(f"Error connecting to {database_name}: {e}")
        return 0

# Main logic to count tables across all databases
def count_all_tables():
    try:
        # Connect to the default 'postgres' database
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()
        
        # Get list of all databases (excluding template databases)
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cur.fetchall()
        
        total_tables = 0
        for db in databases:
            db_name = db[0]
            print(f"Counting tables in database: {db_name}")
            total_tables += count_tables_in_database(db_name)
            print(f"Market has {count_tables_in_database(db_name)} markets")
        
        cur.close()
        conn.close()
        
        print(f"Total number of tables across all databases: {total_tables}")
    
    except Exception as e:
        print(f"Error: {e}")

# Run the script
count_all_tables()
