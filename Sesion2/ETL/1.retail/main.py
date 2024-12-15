from config import DATABASE_CONFIG, CSV_FILES, LOG_FILE
from sqlalchemy import create_engine
import pandas as pd
import logging
import sys

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    This method provides the connection to the mysql Data Base.
    
    return MySQL connection object
    """
    try:
        engine = create_engine(f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        logging.info("Conexion a base de datos fue exitosa")
        return engine
    except Exception as e:
        logging.error(f'Error al conectar a la base de datos: {e}')
        sys.exit(1)
        

def read_csv(file_path, columns):
    """
    Lee un archivo CSV y devuelve un DataFrame
    
    return DataFrame Object
    """
    try:
        df = pd.read_csv(file_path, header=None, sep='|', names=columns)
        logging.info(f"Archivo {file_path} leido correctamente")
        return df    
    except Exception as e:
        logging.error(f'Error al leer el archivo {file_path}: {e}')
        sys.exit(1)

def validate_ids(df_retail, df, id_retail, id_df):
    try:
        valid_ids = set(df[id_df])
        
        if not df_retail[id_retail].isin(valid_ids).all():
            logging.info(f"El id {id_retail} no existen en el campo {id_df} de la tabla a validad")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Error a la hora de realizar las validaciones de id: {e}")
        sys.exit(1)
        

def transform_departments(df):
    """
    Realiza transformaciones en el dataframe departments
    """
    try:
        if df['department_name'].duplicated().any():
            logging.warning("Hay departamentos duplicados en el Dataframe")
            sys.exit(1)
        else:
            return df
    except Exception as e:
        logging.error(f"Error a la hora de realizar las transformaciones al dataframe departments: {e}")

def transform_customers(df):
    """
    Realiza transformaciones en el dataframe customers
    """
    try:
        # Validacion de Campos Obligatorios
        if df[['customer_fname','customer_lname','customer_email']].isnull().any().any():
            logging.warning("Datos Faltantes en el Dataframe")
            sys.exit(1)
        # Transformacion de campo customer_email
        df['customer_email'] =df['customer_email'].str.lower()
        
        return df
    except Exception as e:
        logging.error(f"Error a la hora de realizar las transformaciones al dataframe customers: {e}")

def transform_products(df, df_categories):
    """
    Realiza transformaciones en el dataframe products
    """
    try:
        # Asegurar que product_category_id exista en categories
        validate_ids(df,df_categories,'product_category_id','category_id')
                
        return df
    except Exception as e:
        logging.error(f"Error a la hora de realizar las transformaciones al dataframe products: {e}")

def transform_order_items(df, df_products, df_orders):
    """
    Realiza transformaciones en el dataframe order_items
    """
    try:
        # Asegurar que order_item_order_id exista en orders
        validate_ids(df,df_orders,'order_item_order_id','order_id')
        
        # Asegurar que order_item_product_id exista en product
        validate_ids(df,df_products,'order_item_product_id','product_id')
        
        # Asegurar que el subtotal si sea la multiplicacion de la cantidad por su precio unitario.
        
        calculate_subtotal = df['order_item_quantity'] * df['order_item_product_price']
        if not (df['order_item_subtotal'] == calculate_subtotal).all():
            df['order_item_subtotal'] = calculate_subtotal
        
        return df
    except Exception as e:
        logging.error(f"Error a la hora de realizar las transformaciones al dataframe order_items: {e}")

def transform_orders(df, df_customers):
    """
    Realiza transformaciones específicas en el DataFrame de orders.
    """
    try:
        # Convertir order_date a datetime
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

        if df['order_date'].isnull().any():
            logging.error("Hay valores inválidos en order_date.")
            sys.exit(1)
        # Asegurar que order_customer_id exista en customers
        validate_ids(df, df_customers, 'order_customer_id',  'customer_id' )

        return df
    except Exception as e:
        logging.error(f"Error a la hora de realizar las transformaciones al dataframe orders: {e}")

def load_data(engine, table_name, df):
    """
    Realiza la carga de los datos transformados a la base de datos de MySQL
    """
    try:
        df.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
        logging.info(f"Se cargo correctamente la informacion a la tabla {table_name}")
    except Exception as e:
        logging.error(f"Error a la hora de cargar datos a la tabla {table_name}: {e}")
        sys.exit(1)

if __name__ == '__main__':
    
    # Crear la conexion a la base de datos MySQL
    logging.info("Iniciando ejecucion de Pipeline")
    logging.info("Iniciando conexion a la base de datos de MySQL")
    engine = create_db_engine(DATABASE_CONFIG)
    
    # Cargar y validar todos los archivos de su respectivo CSV.
    logging.info("Iniciando lectura y validacion de archivos CSV")
    load_order = ['departments', 'categories', 'customers', 'products', 'orders', 'order_items']
    dataframes = {}
    
    df_categories = read_csv(CSV_FILES['categories']['path'],CSV_FILES['categories']['header'])
    dataframes['categories'] = df_categories
    
    df_customers = read_csv(CSV_FILES['customers']['path'],CSV_FILES['customers']['header'])
    dataframes['customers'] = transform_customers(df_customers)
    
    df_departments = read_csv(CSV_FILES['departments']['path'],CSV_FILES['departments']['header'])
    dataframes['departments'] = transform_departments(df_departments)
    
    df_orders = read_csv(CSV_FILES['orders']['path'],CSV_FILES['orders']['header'])
    dataframes['orders'] = transform_orders(df_orders, df_customers)
    
    df_products = read_csv(CSV_FILES['products']['path'],CSV_FILES['products']['header'])
    dataframes['products'] = transform_products(df_products, df_categories)
    
    df_order_items = read_csv(CSV_FILES['order_items']['path'],CSV_FILES['order_items']['header'])
    dataframes['order_items'] = transform_order_items(df_order_items,df_products,df_orders)
    logging.info("Terminada la lectura y validacion de archivos CSV")
    
    logging.info("Iniciada la carga de datos a la base de datos de MySQL")
    for table in load_order:
        load_data(engine, table, dataframes[table])
        
    logging.info("Terminada la carga de datos a la base de datos de MySQL")
    logging.info("Pipeline de datos se ejecuto correctamente")