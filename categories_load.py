from dotenv import load_dotenv
import pandas as pd
import numpy as np
import psycopg
import os


pd.set_option("display.max.rows", None)
pd.set_option("display.max.columns", None)
pd.set_option("display.width", None)

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

df = pd.read_csv(r"../data/raw/olistIT_categories.csv")


def load_categories(df):
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:

            sql = """
            CREATE TABLE IF NOT EXISTS categories(
            pk_id_categoria SERIAL PRIMARY KEY,
            categoria VARCHAR
            );
            """

            cur.execute(sql)

            sql = "INSERT INTO categories (categoria) VALUES (%s);"

            print(f"Caricamento in corso... {str(len(df))} righe da inserire.")

            perc_int = 0
            for index, row in df.iterrows():
                perc = float("%.2f" % ((index + 1) / len(df) * 100))
                if perc >= perc_int:
                    print(f"{round(perc)}% Completato")
                    perc_int += 5
                cur.execute(sql, row.to_list())
            conn.commit()


if __name__ == "__main__":
    df["category_name"] = None
    df["category_name"] = np.where(df["product_category_name_english"] == "health_beauty", "beauty", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "computers_accessories", "informatica", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "auto", "automobili", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "bed_bath_table") |
                                   (df["product_category_name_english"] == "housewares") |
                                   (df["product_category_name_english"] == "fixed_telephony") |
                                   (df["product_category_name_english"] == "home_confort") |
                                   (df["product_category_name_english"] == "home_comfort_2") |
                                   (df["product_category_name_english"] == "la_cuisine"), "casalinghi", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "furniture_decor") |
                                   (df["product_category_name_english"] ==  "kitchen_dining_laundry_garden_furniture") |
                                   (df["product_category_name_english"] ==  "furniture_mattress_and_upholstery") |
                                   (df["product_category_name_english"] ==  "furniture_living_room") |
                                   (df["product_category_name_english"] == "furniture_bedroom")
                                   , "arredamento", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "sports_leisure") |
                                   (df["product_category_name_english"] == "fashion_sport"), "sport", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "perfumery", "profumeria", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "telephony", "smartphone", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "watches_gifts") |
                                   (df["product_category_name_english"] == "fashion_bags_accessories") |
                                   (df["product_category_name_english"] == "fashion_shoes") |
                                   (df["product_category_name_english"] == "luggage_accessories"), "accessori", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "food_drink") |
                                   (df["product_category_name_english"] == "food") |
                                   (df["product_category_name_english"] == "drinks"), "food", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "baby") |
                                   (df["product_category_name_english"] == "diapers_and_hygiene"), "baby", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "stationery", "cartoleria", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "tablets_printing_image") |
                                   (df["product_category_name_english"] == "office_furniture") , "ufficio", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "toys", "giocattoli", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "garden_tools") |
                                   (df["product_category_name_english"] == "costruction_tools_garden") |
                                   (df["product_category_name_english"] == "construction_tools_construction") |
                                   (df["product_category_name_english"] =="costruction_tools_tools") |
                                   (df["product_category_name_english"] =="home_construction") |
                                   (df["product_category_name_english"] =="construction_tools_lights") |
                                   (df["product_category_name_english"] =="construction_tools_safety") |
                                   (df["product_category_name_english"] =="flowers") |
                                   (df["product_category_name_english"] =="security_and_services") |
                                   (df["product_category_name_english"] =="signaling_and_security"), "edilizia e giardino", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "small_appliances") |
                                   (df["product_category_name_english"] == "small_appliances_home_oven_and_coffee"), "piccoli elettrodomestici", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "fashion_male_clothing") |
                                   (df["product_category_name_english"] == "fashion_underwear_beach") |
                                   (df["product_category_name_english"] == "fashio_female_clothing") |
                                   (df["product_category_name_english"] == "fashion_childrens_clothes"), "abbigliamento", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "consoles_games", "videogiochi", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "audio", "audio", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "cool_stuff", "idee regalo", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "air_conditioning") |
                                   (df["product_category_name_english"] == "home_appliances") |
                                   (df["product_category_name_english"] == "home_appliances_2") , "grandi elettrodomestici", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "pet_shop", "animali", df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "market_place", "usato", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "electronics") |
                                   (df["product_category_name_english"] == "art") |
                                   (df["product_category_name_english"] == "arts_and_craftmanship"), "bricolage", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "party_supplies") |
                                   (df["product_category_name_english"] == "christmas_supplies"), "seasonal" , df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "agro_industry_and_commerce") |
                                   (df["product_category_name_english"] == "industry_commerce_and_business") , "commercio", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "books_imported") |
                                   (df["product_category_name_english"] == "books_technical") |
                                   (df["product_category_name_english"] == "books_general_interest") ,"libri", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "musical_instruments") |
                                   (df["product_category_name_english"] == "music") |
                                   (df["product_category_name_english"] == "cds_dvds_musicals") ,"musica", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "computers"), "computer", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "dvds_blu_ray"), "dvd e blu-ray", df["category_name"])
    df["category_name"] = np.where((df["product_category_name_english"] == "cine_photo"), "fotografia e video", df["category_name"])

    print(df)
    categories_list = df["category_name"].unique()
    categories_list = pd.DataFrame(categories_list)
    print(categories_list)
    load_categories(categories_list)