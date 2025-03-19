# ELT method for products

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
    print("Questo è il metodo EXTRACT dei prodotti")
    df = common.read_file()
    return df

def transform(df):
    print("Questo è il metodo TRANSFORM dei prodotti")
    df = common.drop_duplicates(df)
    df = common.check_null(df, ["product_id"])
    df = common.format_string(df, ["category"])
    # TODO collegare le nuove Macro-Categorie alla tabella products!
    return df

def load(df):
    print("Questo è il metodo LOAD dei prodotti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE products (
            pk_products_id VARCHAR PRIMARY KEY,
            fk_category VARCHAR,
            product_name INTEGER,
            product_description_lenght INTEGER,
            product_photos_qty INTEGER,
            last_updated TIMESTAMP
            );
            """

            try:
                cur.execute(sql) # Inserimento report nel database
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                scelta = input("Vuoi cancellare la tabella? (si/no) ").strip().upper()
                if scelta == "SI":
                    sqldelete = """DROP TABLE products"""
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella Products!")
                    cur.execute(sql)

            sql = """
                        INSERT INTO products
                        (pk_product_id, fk_category, product_name_lenght, product_description_lenght, product_photos_qty, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (pk_product_id) DO UPDATE SET 
                        (fk_category, product_name_lenght, product_description_lenght, product_photos_qty, last_updated) = (EXCLUDED.fk_category, EXCLUDED.product_name_lenght, EXCLUDED.product_description_lenght, EXCLUDED product_photos_qty, EXCLUDED.last_updated);
                        """

            common.caricamento_barra(df, cur, sql)
            conn.commit()


def main():
    print("Questo è il metodo MAIN dei prodotti")
    df = extract()
    df = transform(df)
    print("Dati trasformati")
    print(df, end="\n\n")  # visualizza una riga vuota
    load(df)

# Per usare questo file come fosse un modulo
# I metodi definiti sopra vanno importati per poter essere utilizzati

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()