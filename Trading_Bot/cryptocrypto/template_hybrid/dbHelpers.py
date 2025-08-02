from config import *
import psycopg2

conn = psycopg2.connect(host=KRAKEN_DB_HOST, dbname=KRAKEN_DB_NAME, user=KRAKEN_DB_USER, password=KRAKEN_DB_PASSWORD)
cur = conn.cursor()

def sqlSelect(string):
    cur.execute(string)
    targString = cur.fetchone()
    conn.commit()
    return targString

def sqlCommit(string):
    cur.execute(string)
    conn.commit()

def sqlSelect(string):
    cur.execute(string)
    targString = cur.fetchone()
    conn.commit()
    return targString

def sqlMultiSelect(string):
    cur.execute(string)
    targString = cur.fetchall()
    conn.commit()
    return targString