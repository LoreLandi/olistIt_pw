import src.customers as customers
import src.products as products
import src.orders as orders
import src.common as common
import src.categories as categories

if __name__ == "__main__":
    risposta = "-1"
    while risposta != "0":
        risposta = input("""Che cosa vuoi fare? 
        0 = esci dal programma
        1 = ETL customers
        2 = esegui integrazione dati regione e città
        3 = format region per PowerBI
        4 = ETL categories
        5 = ETL products
        """)
        if risposta == "1":
            df_customers = customers.extract()
            df_customers = customers.transform(df_customers)
            #print("Visualizza i dati dopo trasformazione")
            #print(df_customers)
            customers.load(df_customers)
        elif risposta == "2":
            customers.complete_city_region()
        elif risposta == "3":
            common.format_region()
        elif risposta == "4":
            df_categories = categories.extract()
            df_categories = categories.t_macro_category(df_categories)
            #categories.load(df_categories)
        elif risposta == "5":
            df_products = products.extract()
            products.convert_numbers(df_products)

            #df_products = products.transform()
            products.load(df_products)
        else: risposta = "0"

'''
    products.extract()
    products.transform()
    
    products.load()

    orders.extract()
    orders.transform()
    orders.load()
'''