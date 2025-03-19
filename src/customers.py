# ELT method for customers

import src.common as common
#from src.common import read_file, caricamento_barra
import psycopg
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

# 3 metodi ETL per CUSTOMERS

def extract():
    print("Questo è il metodo EXTRACT dei clienti")
    df = common.read_file()
    return df

def transform(df):
    print("Questo è il metodo TRANSFORM dei clienti")
    df = common.drop_duplicates(df)
    df = common.check_null(df, ["customer_id"])
    df = common.format_string(df, ["region", "city"])
    df = common.format_cap(df)
    return df

#common.save_processed(df)

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo LOAD dei clienti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
            CREATE TABLE customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR,
            last_updated TIMESTAMP
            );
            """

            try:
                cur.execute(sql) # Inserimento report nel database
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                scelta = input("Do you want to delete table? Y/N ").strip().upper()
                if scelta == "Y":
                    sqldelete = """DROP TABLE customers"""
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella Customers!")
                    cur.execute(sql)

            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap, last_updated)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (pk_customer) DO UPDATE SET 
            (region, city, cap, last_updated) = (EXCLUDED.region, EXCLUDED.city, EXCLUDED.cap, EXCLUDED.last_updated)
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()

# integrazione città e regione
def complete_city_region():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
            SELECT * FROM customers 
            WHERE city = 'NaN' or region = 'NaN'
            """

            sql = f"""
                UPDATE customers AS c1 
                SET region = c2.region,
                last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                FROM customers AS c2
                WHERE c1.cap = c2.cap
                AND c1.cap <> 'NaN'
                AND c2.cap <> 'NaN'
                AND c1.region = 'NaN'
                AND c2.region <> 'NaN'
                RETURNING *
                ; """

            cur.execute(sql)

            print("Record con region aggiornata\n")

            for record in cur:
                print(record)

            sql = f"""
            UPDATE customers AS c1 
            SET city = c2.city,
            last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
            FROM customers AS c2
            WHERE c1.cap = c2.cap
            AND c1.cap <> 'NaN'
            AND c2.cap <> 'NaN'
            AND c1.city = 'NaN'
            AND c2.city <> 'NaN'
            RETURNING *
            ; """

            cur.execute(sql)
            print("Record con city aggiornata\n")
            for record in cur:
                print(record)

def main():
    print("Questo è il metodo MAIN dei clienti")
    df = extract()
    df = transform(df)
    print("Dati trasformati")
    print(df, end="\n\n")  # visualizza una riga vuota
    load(df)

# voglio usare questo file come fosse un modulo:
# i metodi definii sopra andranno importati per poter essere utilizzati


if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()


