from .models import Product, Customer

PRODUCTS = [
    Product(product_sku=f"SKU{i:06d}", description=f"Product {i}",
            unit_amount=round(10 + i * 0.1, 2), supplier="AcmeCorp")
    for i in range(1, 201)
]

CUSTOMERS = [
    Customer(customer_id=f"CS{i:06d}",
             emails=[f"user{i}@example.com"],
             phone_numbers=[f"+33612{i:04d}"])
    for i in range(1, 51)
]
