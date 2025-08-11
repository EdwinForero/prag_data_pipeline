import os
import logging
from sqlmodel import SQLModel, Session
from scripts.db_config import DatabaseConfig
from scripts.pipeline import process_file
from scripts.db_utils import truncate_table
from sqlalchemy import text

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Variable global con la ruta base donde están los CSV
BASE_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data"
)

# Diccionario global de estadísticas
stats = {
    "rows_in_file": 0,
    "rows_total": 0,
    "sum": 0.0,
    "min": float("inf"),
    "max": float("-inf")
}

# Inicializar conexión DB
try:
    db_config = DatabaseConfig()
    engine = db_config.get_engine()
    SQLModel.metadata.create_all(engine)
    logging.info("Conexión a BD inicializada y tablas creadas.")
except Exception as e:
    logging.error(f"!- Error inicializando base de datos: {e}")
    raise

def _print_db_stats(message: str):
    """
    Consulta e imprime estadísticas actuales directamente desde la BD.

    Args:
        message (str): Texto para mostrar como encabezado.
    """
    try:
        with Session(engine) as session:
            result = session.exec(
                text("""
                    SELECT 
                        COUNT(*) as count, 
                        AVG(price) as avg_price, 
                        SUM(price)/COUNT(*) real_avg_price,
                        MIN(price) as min_price, 
                        MAX(price) as max_price 
                    FROM public.data_test
                """)
            ).one()

        logging.info(
            f"{message} -> Count: {result.count}, "
            f"Avg: {result.avg_price:.4f}, "
            f"Real Avg: {result.real_avg_price:.4f}, "
            f"Min: {result.min_price}, "
            f"Max: {result.max_price}"
        )

    except Exception as e:
        logging.error(f"!- Error consultando estadísticas en BD: {e}")
        raise

def run_pipeline(stats_update_mode: str = "file"):
    """
    Procesa los archivos principales del reto y actualiza estadísticas.

    Args:
        stats_update_mode (str): 'row' para actualizar stats fila a fila,
                           'file' para actualizar por archivo completo.
    """
    try:
        logging.info("****** Iniciando pipeline principal ******")
        files = [
            "2012-1.csv",
            "2012-2.csv",
            "2012-3.csv",
            "2012-4.csv",
            "2012-5.csv"
        ]

        for file_name in files:
            full_path = os.path.join(BASE_DATA_PATH, file_name)
            process_file(full_path, stats, stats_update_mode)

        _print_db_stats("+++ Estadísticas de la DB")

    except Exception as e:
        logging.error(f"-! Error ejecutando pipeline principal: {e}")
        raise


def run_validation(stats_update_mode: str = "file"):
    """
    Procesa el archivo de validación y actualiza estadísticas.

    Args:
        stats_update_mode (str): 'row' o 'file' según el método de actualización.
    """
    try:
        logging.info("****** Ejecutando archivo de validación ******")
        validation_file = os.path.join(BASE_DATA_PATH, "validation.csv")
        process_file(validation_file, stats, stats_update_mode)
        _print_db_stats("+++ Estadísticas de la DB después del validation.csv")

    except Exception as e:
        logging.error(f"-! Error ejecutando validación: {e}")
        raise


if __name__ == "__main__":
    try:
        truncate_table()
        run_pipeline(stats_update_mode="row")
        run_validation(stats_update_mode="row")
    except Exception as e:
        logging.critical(f"!- Error crítico en la ejecución principal: {e}")
