import logging
from sqlmodel import SQLModel, Session
from sqlalchemy import text
from scripts.db_config import DatabaseConfig
from scripts.models import Transaction
from datetime import datetime

# Configuración básica del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Inicialización de la conexión usando DatabaseConfig
try:
    logging.info("Inicializando conexión con la base de datos...")
    db_config = DatabaseConfig()
    engine = db_config.get_engine()
    logging.info("Conexión a la base de datos inicializada correctamente")
except Exception as e:
    logging.error(f"Error inicializando conexión con la base de datos: {e}")
    raise


def create_tables():
    """
    Crea las tablas definidas en los modelos de SQLModel.
    """
    try:
        logging.info("Creando tablas en la base de datos...")
        SQLModel.metadata.create_all(engine)
        logging.info("Tablas creadas con éxito ")
    except Exception as e:
        logging.error(f"Error creando tablas: {e}")
        raise


def get_session():
    """
    Devuelve una nueva sesión para interactuar con la base de datos.
    """
    try:
        logging.info("Obteniendo sesión de la base de datos...")
        return Session(engine)
    except Exception as e:
        logging.error(f"Error al abrir sesión de base de datos: {e}")
        raise

def parse_date(date_str):
    """
    Convierte una fecha de 'M/D/YYYY' a objeto datetime compatible con SQLModel.
    Si el valor es nulo o inválido, retorna None.
    """
    try:
        return datetime.strptime(date_str, '%m/%d/%Y')
    except Exception as e:
        logging.warning(f"Fecha inválida: '{date_str}' - {e}")
        return None


def truncate_table():
    """
    Elimina datos de la tabla 'data_test' de la base de datos.
    """
    try:
        logging.info("Intentando truncar la tabla 'data_test'...")
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE public.data_test"))
            conn.commit()
        logging.info("- Tabla 'data_test' truncada con éxito")
    except Exception as e:
        logging.error(f"!- Error al truncar la tabla 'data_test': {e}")
        raise

# Testing
if __name__ == "__main__":
    try:
        create_tables()
    except Exception as e:
        logging.error(f"No se pudo completar la creación de tablas: {e}")
