import psycopg2
from psycopg2 import sql
import os

# Database connection parameters
db_params = {
    'dbname': 'postgres',  # Default database, can be any valid database name
    'user': 'postgres',
    'password': '!!!',
    'host': 'localhost',
    'port': '5432'
}

def get_database_sizes():
    try:
        # Connect to PostgreSQL
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                # Execute query to get database sizes
                cursor.execute("""
                    SELECT 
                        datname AS database_name,
                        pg_database_size(datname) AS size_bytes
                    FROM 
                        pg_database
                    ORDER BY 
                        pg_database_size(datname) DESC;
                """)
                
                # Fetch and display results
                databases = cursor.fetchall()
                total_size = 0
                for db_name, size_bytes in databases:
                    # Convert size to MB for easier reading
                    size_mb = size_bytes / (1024 * 1024)
                    print(f"Database: {db_name}, Size: {size_mb:.2f} MB")
                    total_size += size_bytes

                # Calculate and display total size in MB
                total_size_mb = total_size / (1024 * 1024)
                print(f"\nTotal Size of All Databases: {total_size_mb:.2f} MB")

    except Exception as e:
        print(f"Error fetching database sizes: {e}")

# Run the function
if __name__ == "__main__":
    get_database_sizes()