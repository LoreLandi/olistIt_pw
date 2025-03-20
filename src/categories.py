# ETL methods for categories

import src.common as common
import datetime
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import psycopg
import os
from src.common import drop_duplicates

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    df= common.read_file()
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """   
            CREATE TABLE categories (
            pk_category SERIAL PRIMARY KEY,
            category_name VARCHAR UNIQUE,
            last_updated TIMESTAMP
            );
            """
            try:
               cur.execute(sql)
            # Inserimento report nel database
            except psycopg.errors.DuplicateTable as ex:
                print(ex)
                conn.commit()
                delete=input("Do you want to delete table? Y/N ").upper().strip()
                if delete =="Y":
                    sql_delete=""" DROP TABLE categories;"""
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Recreating categories table")
                    cur.execute(sql)
            sql = """
            INSERT INTO categories (category_name, last_updated) 
            VALUES (%s, %s) 
            ON CONFLICT (category_name) DO UPDATE 
            SET last_updated = EXCLUDED.last_updated;"""

            sql = """
            INSER INTO categories (category_name, last_updated) 
            VALUES 
            """

            common.caricamento_barra(df,cur,sql)
            conn.commit()

def t_macro_category(df,column):
    df["category_name"] = None
    df["category_name"] = np.where(df[f"{column}"] == "health_beauty", "beauty",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "computers_accessories", "computer",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "auto", "cars", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "bed_bath_table") |
                                   (df[f"{column}"] == "housewares") |
                                   (df[f"{column}"] == "fixed_telephony") |
                                   (df[f"{column}"] == "home_confort") |
                                   (df[f"{column}"] == "home_comfort_2") |
                                   (df[f"{column}"] == "la_cuisine"), " kitchenware",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "furniture_decor") |
                                   (df[f"{column}"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df[f"{column}"] == "furniture_mattress_and_upholstery") |
                                   (df[f"{column}"] == "furniture_living_room") |
                                   (df[f"{column}"] == "furniture_bedroom"), "furniture",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "sports_leisure") |
                                   (df[f"{column}"] == "fashion_sport"), "sport",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "perfumery", "perfumery",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "telephony", "smartphone",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "watches_gifts") |
                                   (df[f"{column}"] == "fashion_bags_accessories") |
                                   (df[f"{column}"] == "fashion_shoes") |
                                   (df[f"{column}"] == "luggage_accessories"), "accessories",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "food_drink") |
                                   (df[f"{column}"] == "food") |
                                   (df[f"{column}"] == "drinks"), "food", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "baby") |
                                   (df[f"{column}"] == "diapers_and_hygiene"), "baby",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "stationery", "stationery",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "tablets_printing_image") |
                                   (df[f"{column}"] == "office_furniture"), "office",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "toys", "toys", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "garden_tools") |
                                   (df[f"{column}"] == "costruction_tools_garden") |
                                   (df[f"{column}"] == "construction_tools_construction") |
                                   (df[f"{column}"] == "costruction_tools_tools") |
                                   (df[f"{column}"] == "home_construction") |
                                   (df[f"{column}"] == "construction_tools_lights") |
                                   (df[f"{column}"] == "construction_tools_safety") |
                                   (df[f"{column}"] == "flowers") |
                                   (df[f"{column}"] == "security_and_services") |
                                   (df[f"{column}"] == "signaling_and_security"),
                                   "construction & garden", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "small_appliances") |
                                   (df[f"{column}"] == "small_appliances_home_oven_and_coffee"),
                                   "small appliance ", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "fashion_male_clothing") |
                                   (df[f"{column}"] == "fashion_underwear_beach") |
                                   (df[f"{column}"] == "fashio_female_clothing") |
                                   (df[f"{column}"] == "fashion_childrens_clothes"),
                                   "clothing", df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "consoles_games", "video games",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "audio", "audio", df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "cool_stuff", "gift idea",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "air_conditioning") |
                                   (df[f"{column}"] == "home_appliances") |
                                   (df[f"{column}"] == "home_appliances_2"),
                                   "large appliance", df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "pet_shop", "pets", df["category_name"])
    df["category_name"] = np.where(
        df[f"{column}"] == "market_place", "second hand", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "electronics") |
                                   (df[f"{column}"] == "art") |
                                   (df[f"{column}"] == "arts_and_craftmanship"), "bricolage",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "party_supplies") |
                                   (df[f"{column}"] == "christmas_supplies"), "seasonal",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "agro_industry_and_commerce") |
                                   (df[f"{column}"] == "industry_commerce_and_business"),
                                   "commerce", df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "books_imported") |
                                   (df[f"{column}"] == "books_technical") |
                                   (df[f"{column}"] == "books_general_interest"), "books",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "musical_instruments") |
                                   (df[f"{column}"] == "music") |
                                   (df[f"{column}"] == "cds_dvds_musicals"), "music",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "computers"), "ITech",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "dvds_blu_ray"), "dvd & blu-ray",
                                   df["category_name"])
    df["category_name"] = np.where((df[f"{column}"] == "cine_photo"), "photo & video",
                                   df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "[Null]"), "others",
                                   df["category_name"])

    #nuova_categoria = {"product_category_name_english": "other", "product_category_name_italian": "altro",
                       #"category_name": "other"}

    #if not df["product_category_name_english"].isin(["other"]).any():
        #df = pd.concat([df, pd.DataFrame([nuova_categoria])], ignore_index=True)

    return df

def load_categories(df):
    categories_list = df["category_name"].unique()
    categories_list = pd.DataFrame(categories_list)
    print(categories_list)
    common.drop_duplicates(df["category_name"])
    load(categories_list)

def main():
    df = extract()
    df = t_macro_category(df,"product_category_name_english")
    load_categories(df)




if __name__ == "__main__":
    main()
