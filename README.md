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