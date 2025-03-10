# 3 metodi ETL per CUSTOMERS

def extract():
    print("Questo è il metodo EXTRACT dei clienti")

def transform():
    print("Questo è il metodo TRANSFORM dei clienti")

def load():
    print("Questo è il metodo LOAD dei clienti")

def main():
    print("Questo è il metodo MAIN dei clienti")
    extract()
    transform()
    load()

# Per usare questo file come fosse un modulo
# I metodi definiti sopra vanno importati per poter essere utilizzati

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()


