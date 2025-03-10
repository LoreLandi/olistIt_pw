import src.customers as customers
import src.products as products
import src.orders as orders

if __name__ == "__main__":
    customers.extract()
    customers.transform()
    customers.load()

    products.extract()
    products.transform()
    products.load()

    orders.extract()
    orders.transform()
    orders.load()