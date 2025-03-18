# TITOLO
## sottotitolo

**GRASSETTO**
_CORSIVO_
~~SOTTOLINEATO~~

Per scrivere tutte le istruzioni per collegarci al database

## Tables

**customers**
- pk_customer VARCHAR
- region VARCHAR
- city VARCHAR
- cap VARCHAR

**categories**
- pk_category SERIAL
- name VARCHAR

**products**
- pk_product VARCHAR
- fk_category INTEGER
- name_length INTEGER
- description_length INTEGER
- imgs_qty INTEGER

**orders**
- pk_order VARCHAR
- fk_customer VARCHAR
- status VARCHAR
- purchase_timestamp TIMESTAMP
- delivered_timestamp TIMESTAMP
- estimated_date DATE

**sellers** 
- pk_seller VARCHAR
- region VARCHAR

**orders_products**
- pk_order_product SERIAL
- fk_order VARCHAR
- fk_product VARCHAR
- fk_seller VARCHAR
- price FLOAT
- freight FLOAT (=costo spedizione)

## ToDo Opzionale
- copia del file in input alla cartella raw
- (fare in modo che il nome del file sia univoco, aggiungendo data e ora)
- prima di fare il load creare database da Python
- controllo di validità per cancellare la tabella
(con user e psw)
- metodo per controllo di validità degli input,
oltre a strip() e upper()/lower()
- colonna che tracci la data di inserimento dei dati
- check sui cap >5 cifre (formato 01234)
- check sulle 20 regioni ammesse
- integrare dati customer a partire dal cap
- gestione del tipo di valore da aggiornare in fillNulls
- formattazione stringhe: 
  * no caratteri speciali all'inizio e alla fine delle stringhe,
  - codifica utf8
  - maiuscole all'inizio
  - valide solo le lettere maiuscole e minuscole, apostrofo, spazio, trattino
- visualizziamo un menù delle opzioni disponibili eseguibili sul db; es: integrazione dati city e region
  - visualizzazione clienti per regione
  - visualizzazione prodotti per regione


Ed
- copia del file in input alla cartella raw 
(fare in modo che il nome del file sia univoco, 
con data e ora)
- prima di fare il load creare database da Python
- controllo di validità per cancellare la tabella
(con user e psw)
- metodo per controllo di validità degli input,
oltre a strip() e upper()/lower()
- colonna che tracci la data di inserimento dei dati
- check sui cap >5 cifre (formato 01234)
- check sulle 20 regioni ammesse
- integrare dati customer a partire dal cap
- gestione del tipo di valore da aggiornare in fillNulls ()