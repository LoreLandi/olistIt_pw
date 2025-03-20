# ETL methods for ORDERS_PRODUCTS

import pandas as pd
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

def extract():
    print("Questo è il metodo EXTRACT degli ordini")
    df = common.read_file()
    return df

def transform(df):
    print("Questo è il metodo TRANSFORM degli ordini")
    df = common.drop_duplicates(df)
    df = common.check_null(df, ["order_id"])
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
                CREATE TABLE orders_products (
                pk_order_product VARCHAR,
                fk_order VARCHAR,
                fk_product VARCHAR,
                fk_seller VARCHAR,
                price FLOAT,
                freight  FLOAT,
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
                    sqldelete = """DROP TABLE orders_products"""
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella Orders-Products!")
                    cur.execute(sql)

            sql = """
            INSERT INTO orders_products
            (pk_order_product, fk_order, fk_product, fk_seller, price, freight, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pk_order_product) DO UPDATE SET 
            (fk_order, fk_product, fk_seller, price, freight, last_updated) = (EXCLUDED.fk_order, EXCLUDED.fk_product, EXCLUDED.fk_seller, EXCLUDED.price, EXCLUDED.freight, EXCLUDED.last_updated);
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()

def main():
    print("Questo è il metodo MAIN degli ordini")
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()


#CHAT GPT SUGGERISCE QUESTA ROBA!....HELP!!!

#ALTER TABLE orders_products
# ADD CONSTRAINT pk_unique UNIQUE(pk_order_product);