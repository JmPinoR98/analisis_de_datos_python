from config import DATABASE_CONFIG, CSV_FILES, LOG_FILE, QUERY
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import logging
import random
import sys


logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    Establece la conexión con la base de datos MySQL utilizando SQLAlchemy.

    Parameters:
        config (dict): Configuración de la base de datos (host, port, user, password, database).

    Returns:
        sqlalchemy.engine.base.Engine: Objeto de conexión a la base de datos.
    """
    try:
        engine = create_engine(f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        logging.info(f"Conexion a base de datos {config['database']} fue exitosa")
        return engine
    except Exception as e:
        logging.error(f'Error al conectar a la base de datos {config['database']}: {e}')
        sys.exit(1)

def get_data_from_db(conn):
    """
    This method executes the query that has been created in the config.py file and return the query as a 
    dataframe.
    
    return Dataframe object
    """
    try:
        df = pd.read_sql(sql=QUERY, con = conn)
        logging.info("Se obtiene exitosamente los datos del query del archivo config.py")
        return df
    except Exception as e:
        logging.error(f"Error al obtener los datos de la base de datos: {e}")
        sys.exit(1)

def read_csv(file_path, sep=','):
    """
    Este método lee un archivo CSV y devuelve los datos como un DataFrame.

    Parameters:
        file_path (str): Ruta del archivo CSV.
        sep (str): Separador utilizado en el archivo CSV (por defecto, coma).

    Returns:
        pd.DataFrame: DataFrame con los datos del archivo CSV.
    """
    try:
        df = pd.read_csv(file_path, sep=sep)
        logging.info(f"El archivo {file_path} se ha leído correctamente.")
        return df    
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}: {e}")
        sys.exit(1)

def gen_rating():
    """
    Genera una calificación aleatoria entre 0 y 5 con un decimal.
    
    Returns:
        float: Calificación aleatoria de película.
    """
    return round(random.uniform(0, 5), 1)

def gen_timestamp():
    """
    Genera un timestamp aleatorio dentro del año 2024.
    
    Returns:
        datetime: Fecha y hora aleatoria.
    """
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    return random_date

def validate_ids(df_transact, df, id_transact, id_df):
    """
    Valida la existencia de los IDs de un DataFrame en otro DataFrame.
    
    Parameters:
        df_transact (pd.DataFrame): DataFrame de transacciones.
        df (pd.DataFrame): DataFrame con los IDs a validar.
        id_transact (str): Nombre de la columna en df_transact que contiene los IDs.
        id_df (str): Nombre de la columna en df que contiene los IDs válidos.
    
    Raises:
        SystemExit: Si algún ID no es válido, se muestra un mensaje de advertencia y se detiene la ejecución.
    """
    try:
        valid_ids = set(df[id_df])

        if not df_transact[id_transact].isin(valid_ids).all():
            logging.warning(f"Algunos IDs en '{id_transact}' no existen en '{id_df}' en el DataFrame de referencia.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Error al validar los IDs: {e}")
        sys.exit(1)

def validate_duplicates(df, column):
    """
    Valida si existen valores duplicados en la columna especificada de un DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame en el que se valida la columna.
        column (str): Nombre de la columna donde se buscarán los duplicados.
    
    Raises:
        SystemExit: Si se encuentran duplicados en la columna, se muestra un mensaje de advertencia y se detiene la ejecución.
    """
    try:
        if df[column].duplicated().any():
            logging.warning(f"Se encontraron movieID duplicados en la columna '{column}' del DataFrame.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Error al validar los duplicados: {e}")
        sys.exit(1)

def validate_columns(df, columns):
    """
    Valida si existen valores nulos en las columnas especificadas de un DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame donde se realizará la validación.
        columns (list): Lista con los nombres de las columnas a validar.
    
    Raises:
        SystemExit: Si se encuentran valores nulos, se muestra un mensaje de advertencia y se detiene la ejecución.
    """
    try:
        if df[columns].isnull().any().any():
            logging.warning(f"Se encontraron datos faltantes en las columnas {columns} del DataFrame.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Error al validar los datos en las columnas: {e}")
        sys.exit(1)

def transform_movie_award(df):
    """
    Realiza la validación y transformación de los datos en el dataframe 'movie_award'.
    Valida los duplicados, revisa los valores nulos y organiza los tipos de datos,
    transformando la columna 'movieID' a entero y renombrando 'Aware' a 'Award'.
    
    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    try:
        # Validación de duplicados en movieID
        validate_duplicates(df, 'movieID')

        # Validación de campos obligatorios
        validate_columns(df, ['movieID', 'IdAward', 'Aware'])

        # Transformación del tipo de dato de 'movieID' y renombramiento de la columna 'Aware'
        df['movieID'] = df['movieID'].astype('int')
        df.rename(columns={"Aware": "Award"}, inplace=True)

        return df
    except Exception as e:
        logging.error(f'Error al transformar el dataframe movie_award: {e}')
        sys.exit(1)

def transform_movie_data(df, df_movies_award):
    """
    Realiza la validación y transformación de los datos en el dataframe 'movie_data'.
    Valida los duplicados, revisa los valores nulos, organiza los tipos de datos,
    valida que los IDs de 'movieID' existan en 'movie_award', y realiza una unión
    de ambos dataframes, renombrando algunas columnas y eliminando 'IdAward'.
    
    Returns:
        pd.DataFrame: DataFrame transformado y combinado con movie_award.
    """
    try:
        # Validación de duplicados en movieID
        validate_duplicates(df, 'movieID')

        # Validación de campos obligatorios
        validate_columns(df, ['movieID', 'title', 'releaseDate', 'gender', 'participantName', 'roleparticipant'])

        # Transformación del tipo de dato de 'movieID'
        df['movieID'] = df['movieID'].astype('int')

        # Validación de que 'movieID' de movie_award exista en movie_data
        validate_ids(df_movies_award, df, 'movieID', 'movieID')

        # Unión de los DataFrames 'df' y 'df_movies_award'
        df_merge = pd.merge(df, df_movies_award, on='movieID')

        # Renombramiento de columnas y eliminación de 'IdAward'
        df_merge = df_merge.rename(columns={'releaseDate': 'releaseMovie', 'Award': 'awardMovie'})
        df_merge = df_merge.drop(columns=['IdAward'])

        return df_merge
    except Exception as e:
        logging.error(f'Error al transformar el dataframe movie_data: {e}')
        sys.exit(1)

def transform_users(df):
    """
    Gets the dataframe users, validates the columns of the dataframe and the data, and then renames
    the idUser column to userID.
    
    return Dataframe object
    """
    try:
        # Valida si existen Ids duplicados
        validate_duplicates(df,'idUser')

        # Validacion de Campos Obligatiorios
        validate_columns(df,['idUser','username','country','subscription'])

        # Renombrar el campo de userID
        df = df.rename(columns={'idUser': 'userID'})
        
        return df
    except Exception as e:
        logging.error(f'Error al transformar el dataframe users: {e}')
        sys.exit(1)

def transform_watch_data(df_users, df_movie_data):
    """
    Realiza la transformación de los datos para crear la tabla de hechos 'watch_data'.
    Se genera un cross join entre los IDs de usuarios y películas, simulando que 
    cada usuario vio todas las películas. Luego, se asignan calificaciones y 
    marcas de tiempo aleatorias a cada registro.
    
    Returns:
        pd.DataFrame: DataFrame transformado con los datos de watch_data.
    """
    try:
        # Obtener los ID de los usuarios y películas
        users_id = df_users["userID"]
        movies_id = df_movie_data["movieID"]
        
        # Realizar el cross join entre los ID de usuario y película para simular que cada usuario vio todas las películas
        df = pd.merge(users_id, movies_id, how="cross")

        # Asignar un rating aleatorio y un timestamp para cada combinación de usuario y película
        df["rating"] = df["movieID"].apply(lambda x: gen_rating())
        df["timestamp"] = df["userID"].apply(lambda x: gen_timestamp())
        
        return df
    except Exception as e:
        logging.error(f'Error al transformar el dataframe watch_data: {e}')
        sys.exit(1)

def load_data(engine, table_name, df):
    """
    Gets the data of a dataframe and loads it to the table in the MySQL Data Warehouse.
    """
    try:
        df.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
        logging.info(f"Se cargo correctamente la informacion a la tabla {table_name}")
    except Exception as e:
        logging.error(f"Error a la hora de cargar datos a la tabla {table_name}: {e}")
        sys.exit(1)

if __name__ == '__main__':
    
    logging.info("Iniciando ejecucion de Pipeline")
    # Crea la conexion a la base de datos transaccional de MySQL
    logging.info("Iniciando conexion a la base de datos transaccional de MySQL")
    transact_engine = create_db_engine(DATABASE_CONFIG['transact'])
    
    # Cargar y tranformacion todos los datos.
    logging.info("Iniciando lectura y transformacion de datos")
    dataframes = {}
    
    df_movie_awards = read_csv(CSV_FILES['award_movie'])
    dataframes['movie_awards'] = transform_movie_award(df_movie_awards)
    
    df_movie_data = get_data_from_db(transact_engine)
    dataframes['dimMovie'] = transform_movie_data(df_movie_data,df_movie_awards)
    
    df_users = read_csv(CSV_FILES['users'],sep='|')
    dataframes['dimUser'] = transform_users(df_users)
    
    dataframes['FactWatchs'] = transform_watch_data(dataframes['dimUser'],dataframes['dimMovie'])
    logging.info("Terminada la lectura y tranformacion de data")
    
    logging.info("Iniciando conexion a la Data Warehouse de MySQL")
    warehouse_engine = create_db_engine(DATABASE_CONFIG['warehouse'])
    load_order = ['dimMovie','dimUser','FactWatchs']
    for table in load_order:
        load_data(warehouse_engine, table, dataframes[table])
    
    logging.info("Terminada la carga de datos a la Data Warehouse de MySQL")
    logging.info("Pipeline de datos se ejecuto correctamente")