# ELT method for categories

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
    df = common.read_file()
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """   
            CREATE TABLE  categories (
            pk_category SERIAL PRIMARY KEY,
            category_name VARCHAR,
            last_updated TIMESTAMP
            );
            """
            try:
                cur.execute(sql)
            # Inserimento report nel database
            except psycopg.errors.DuplicateTable as ex:
                print(f"ERROR: {ex}")
                conn.commit()
                delete = input("Do you want to delete table? Y/N ").upper().strip()
                if delete == "Y":
                    sql_delete = """ 
                    DROP TABLE categories;
                    """
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Recreating customer table")
                    cur.execute(sql)
            sql = """
            INSERT INTO categories (category_name, last_updated) 
            VALUES (%s, %s);
            """

            """
            ON CONFLICT (pk_category) DO UPDATE SET
            (category_name,last_updated) = (EXCLUDED.name_category,EXCLUDED.last_updated);
            """

            print(f"Loading... \n{str(len(df))} row to insert.")
            tmax = 50
            if len(df) / 2 < 50:
                tmax = len(df)
            print("┌" + "─" * tmax + "┐")
            print("│", end="")
            perc_int = 2
            for index, row in df.iterrows():
                perc = float("%.2f" % ((index + 1) / len(df) * 100))
                if perc >= perc_int:
                    print("\r│" + "█" * (perc_int // 2) + str(int(perc)) + "%", end="")
                    # print(perc,end="")
                    perc_int += 2
                cur.execute(sql, (row[0], row["last_updated"]))
            print("\r│" + "█" * tmax + "│ 100% Completed!")
            print("└" + "─" * tmax + "┘")
            conn.commit()


def macro_category(df):
    df["category_name"] = None
    df["category_name"] = np.where(df["product_category_name_english"] == "health_beauty", "beauty",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "computers_accessories", "computer",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "auto", "cars", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "bed_bath_table") |
                                   (df["product_category_name_english"] == "housewares") |
                                   (df["product_category_name_english"] == "fixed_telephony") |
                                   (df["product_category_name_english"] == "home_confort") |
                                   (df["product_category_name_english"] == "home_comfort_2") |
                                   (df["product_category_name_english"] == "la_cuisine"), " kitchenware",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "furniture_decor") |
                                   (df["product_category_name_english"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df["product_category_name_english"] == "furniture_mattress_and_upholstery") |
                                   (df["product_category_name_english"] == "furniture_living_room") |
                                   (df["product_category_name_english"] == "furniture_bedroom"), "furniture",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "sports_leisure") |
                                   (df["product_category_name_english"] == "fashion_sport"), "sport",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "perfumery", "perfumery",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "telephony", "smartphone",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "watches_gifts") |
                                   (df["product_category_name_english"] == "fashion_bags_accessories") |
                                   (df["product_category_name_english"] == "fashion_shoes") |
                                   (df["product_category_name_english"] == "luggage_accessories"), "accessories",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "food_drink") |
                                   (df["product_category_name_english"] == "food") |
                                   (df["product_category_name_english"] == "drinks"), "food", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "baby") |
                                   (df["product_category_name_english"] == "diapers_and_hygiene"), "baby",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "stationery", "stationery",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "tablets_printing_image") |
                                   (df["product_category_name_english"] == "office_furniture"), "office",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "toys", "toys", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "garden_tools") |
                                   (df["product_category_name_english"] == "costruction_tools_garden") |
                                   (df["product_category_name_english"] == "construction_tools_construction") |
                                   (df["product_category_name_english"] == "costruction_tools_tools") |
                                   (df["product_category_name_english"] == "home_construction") |
                                   (df["product_category_name_english"] == "construction_tools_lights") |
                                   (df["product_category_name_english"] == "construction_tools_safety") |
                                   (df["product_category_name_english"] == "flowers") |
                                   (df["product_category_name_english"] == "security_and_services") |
                                   (df["product_category_name_english"] == "signaling_and_security"),
                                   "construction & garden", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "small_appliances") |
                                   (df["product_category_name_english"] == "small_appliances_home_oven_and_coffee"),
                                   "small appliance ", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "fashion_male_clothing") |
                                   (df["product_category_name_english"] == "fashion_underwear_beach") |
                                   (df["product_category_name_english"] == "fashio_female_clothing") |
                                   (df["product_category_name_english"] == "fashion_childrens_clothes"),
                                   "clothing", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "consoles_games", "video games",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "audio", "audio", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "cool_stuff", "gift idea",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "air_conditioning") |
                                   (df["product_category_name_english"] == "home_appliances") |
                                   (df["product_category_name_english"] == "home_appliances_2"),
                                   "large appliance", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "pet_shop", "pets", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "market_place", "second hand", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "electronics") |
                                   (df["product_category_name_english"] == "art") |
                                   (df["product_category_name_english"] == "arts_and_craftmanship"), "bricolage",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "party_supplies") |
                                   (df["product_category_name_english"] == "christmas_supplies"), "seasonal",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "agro_industry_and_commerce") |
                                   (df["product_category_name_english"] == "industry_commerce_and_business"),
                                   "commerce", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "books_imported") |
                                   (df["product_category_name_english"] == "books_technical") |
                                   (df["product_category_name_english"] == "books_general_interest"), "books",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "musical_instruments") |
                                   (df["product_category_name_english"] == "music") |
                                   (df["product_category_name_english"] == "cds_dvds_musicals"), "music",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "computers"), "ITech",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "dvds_blu_ray"), "dvd & blu-ray",
                                   df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "cine_photo"), "photo & video",
                                   df["category_name"])

    categories_list = df["category_name"].unique()
    categories_list = pd.DataFrame(categories_list)
    common.drop_duplicates(df["category_name"])
    print(categories_list)
    load(categories_list)
    return df


def main():
    df = extract()
    macro_category(df)


if __name__ == "__main__":
    main()
