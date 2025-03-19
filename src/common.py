import datetime
import pandas as pd
import psycopg
from dotenv import load_dotenv
import os

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

# Analogo del metodo extract
def read_file():
    isvalid = False
    df = pd.DataFrame()
    while not isvalid:
        path = input("Inserisci il path del file:\n").strip()
        try:
            path_list = path.split(".")

            if path_list[-1] == "csv" or path_list[-1] == "txt":
                df = pd.read_csv(path)
            elif path_list[-1] == "xlsx" or path_list[-1] == "xls":
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)

        #puoi usare una lista che è + DRY!
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente: procedo all'...")
            isvalid = True
    else:
        return df

# metodo percentuale
'''
def caricamento_percentuale(df, cur, sql):
    # eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
    print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
    perc_int = 0
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print(f"{round(perc)}% Completato")
            perc_int += 5
        cur.execute(sql, row.to_list())
        '''

# Metodo Grafico
'''
def caricamento_barra(df, cur, sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    print("┌──────────────────────────────────────────────────┐")
    print("│", end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("█", end="")
            # print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("│ 100% Completato!")
    print("└──────────────────────────────────────────────────┘")
'''

# Nuovo Metodo Grafico
def caricamento_barra(df, cur, sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    Tmax = 50
    if len(df) / 2 < 50:
        Tmax = len(df)
    print("┌" + "─" * Tmax + "┐")
    print("│", end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("\r│" + "█" * (perc_int // 2) + str(int(perc)) + "%", end="")
            # print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("\r│" + "█" * Tmax + "│ 100% Completato!")
    print("└" + "─" * Tmax + "┘")

def format_string(df, cols):
    #print(df[cols])
    for col in cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        # se avessi scritto [^0-9] avrebbe lasciato SOLO i caratteri numerici
        df[col] = df[col].str.replace("[$&+:;=?@#|<>.^*(/_)%!]", "", regex=True)
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    return df

def drop_duplicates(df):
    print("Valori duplicati rimossi:", df.duplicated().sum(), "\n")
    df.drop_duplicates(inplace=True)
    return df

def format_cap(df):
    # Converte in stringa e riempie con 0 se len non è 5
    #df["cap"] = df["cap"].fillna(0).astype(int).astype(str).str.zfill(5)
    df["cap"] = df["cap"].apply(lambda cap: str(int(cap)).zfill(5) if cap == cap else cap)
    return df

# Gestione valori nulli
def check_null(df, subset=""):
    print(f"Valori nulli per colonna:\n {df.isnull().sum()} \n")
    subset = df.columns.tolist()[0] if not subset else subset
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    #df = fill_null(df)
    #print(df)
    return df

def fill_null(df):
    # gestione del tipo di valore da aggiornare
    df.fillna(value="nd", axis=0, inplace=True)
    return df

# Salvataggio
def save_processed(df):
    name = input("Qual è il nome del file? ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = name + "_processed" + "_datetime" + timestamp + ".csv"
    print(file_name, end="\n\n")
    if __name__ == "__main__":
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index = False)

def format_region():
    print("Formattazione dei nome delle regioni per PowerBI")
    nome_tabella = input("Inserisci il nome della tabella da modificare: ").strip().lower()
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Emilia-Romagna'
            WHERE region = 'Emilia Romagna'
            RETURNING * 
            """

            cur.execute(sql)
            print("Record con city aggiornata\n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Friuli-Venezia Giulia'
            WHERE region = 'Friuli Venezia Giulia'
            RETURNING * 
            """

            cur.execute(sql)
            print("Record con city aggiornata\n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Trentino-Alto Adige'
            WHERE region = 'Trentino Alto Adige'
            RETURNING * 
            """

            cur.execute(sql)
            print("Record con city aggiornata\n")
            for record in cur:
                print(record)

if __name__ == "__main__":
    #df = read_file()
    #df = format_string(df, ["region", "city"])
    #print("Dati prima di format_cap")
    #print(df)
    #df = format_cap(df)
    #print("Dati dopo format_cap")
    #print(df)
    #check_null(df) #modifica di default la prima colonna a meno che non gli si dà la lista es. [customer_id]
    #save_processed(df)
    format_region()

