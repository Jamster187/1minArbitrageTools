#!/usr/bin/env python3

import psycopg2

def main():
    # 1. Set up your connection parameters
    dbname = "Testing_Data_Collection_Bitget"
    user = "postgres"
    password = ''
    host = "localhost"
    port = 5432

    # 2. Connect to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    
    try:
        cur = conn.cursor()
        
        # 3. Query to get only table_name (without schema)
        sql = """
            SELECT
                format('DELETE FROM %I WHERE "timestamp" < 1762340547000;', table_name) AS delete_sql
            FROM information_schema.columns
            WHERE column_name = 'timestamp'
              -- If you only want from schema "public", add:
              -- AND table_schema = 'public'
            ORDER BY table_schema, table_name;
        """
        
        cur.execute(sql)
        rows = cur.fetchall()
        
        # 4. Print the generated DELETE statements (without the schema part)
        for row in rows:
            print(row[0])
        
        cur.close()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
