# Read Me
## Istruzioni collegamento DB
+ creare un file .env 
+ compilare il file con i propri dati seguendo il file .env_example

## Schema DB

###  **customers**
  + pk_customer, varchar
  + region, varchar
  + city, varchar
  + cap, varchar 

### **categories**
  + pk_category, serial
  + name, varchar

### **products**
  + pk_product, varchar
  + fk_category, int
  + name_length, int
  + description_length, int
  + imgs_qny, int

### **orders**
  + pk_order, varchar
  + fk_customer, varchar
  + status, varchar
  + purchase_timestamp, timestamp
  + delivered_timestamp, timestamp
  + estimated_date, date

### **seller** (al momento non disponibile)
  + pk_seller,varchar
  + region,varchar

### **orders_products**
  + pk_order_product, serial
  + fk_order, varchar
  + fk_product, varchar
  + fk_seller, varchar
  + price, float
  + freight, float
  


## to do opzionale
+ common py:
  + ~~copia del file in input alla cartella raw~~
  + ~~controllare che il nome del file sia univoco~~
  + ~~controllo di validità input~~
  + ~~conferma per eliminare una tabella~~
  + ~~aggiungere colonna per sapere quando i dati sono stati aggiunti in tabella~~
  + ~~integrare dati customers~~
  + gestione tipo di valore da aggiornare fill_null (print(df.select_dtypes("object")))
  + provare ad aggiustare il path per farlo funzionare da customers
  + ~~errore con una tabella non esistente nella modifica regione~~