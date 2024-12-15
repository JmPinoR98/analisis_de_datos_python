import socket

DATABASE_CONFIG = {
    "host": socket.gethostbyname(socket.gethostname()),
    "port": 3310,
    "user": "root",
    "password": "root",
    "database": "retail_db"
}

CSV_FILES = {
    'customers': {
        'path': r'./data/customers',
        'header': ["customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode"]
    },
    'departments': {
        'path': r'./data/departments',
        'header': ['department_id', 'department_name']
    },
    'order_items': {
        'path': r'./data/order_items',
        'header': ["order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price"]
    },
    'orders': {
        'path': r'./data/orders',
        'header': ["order_id","order_date","order_customer_id","order_status"]
    },
    'products': {
        'path': r'./data/products',
        'header': ["product_id","product_category_id","product_name","product_description","product_price","product_image"]
    },
    'categories': {
        'path': r'./data/categories',
        'header': ["category_id", "category_department_id", "category_name"]
    },
}

LOG_FILE = 'logs/pipeline.log'