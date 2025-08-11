import logging
import polars as pl
from scripts.db_utils import get_session, parse_date
from scripts.models import Transaction

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def update_stats(stats: dict, price, mode: str, df=None):
    """
    Actualiza las estadísticas 'count', 'sum', 'min', 'max' en memoria y lleva
    contadores de filas leídas por archivo y totales del pipeline.

    Args:
        stats (dict): Diccionario de estadísticas con claves:
            - count: filas con price válido
            - sum: suma de prices válidos
            - min: precio mínimo visto (global)
            - max: precio máximo visto (global)
            - rows_total: todas las filas leídas en el proceso
            - rows_in_file: filas leídas en el archivo actual
        price (float|None): Precio actual (solo para modo 'row').
        mode (str): 'row' → registro a registro, 'file' → archivo completo.
        df (polars.DataFrame|None): DataFrame completo (solo para modo 'file').
    """
    try:
        # ----------- Modo fila a fila -----------
        if mode == "row":
            stats["rows_in_file"] += 1
            stats["rows_total"] += 1

            is_null = price is None

            if not is_null:
                stats["sum"] += price
                stats["min"] = min(stats["min"], price)
                stats["max"] = max(stats["max"], price)

            avg_price = stats["sum"] / stats["rows_total"] if stats["rows_total"] else 0

            logging.info(
                f"[Fila {stats['rows_in_file']} archivo | {stats['rows_total']} total global] "
                f"Precio leído: {price} {'(nulo)' if is_null else ''} | "
                f"Avg global: {avg_price:.4f} | "
                f"Min global: {stats['min']} | Max global: {stats['max']}"
            )

        # ----------- Modo archivo completo -----------
        elif mode == "file" and df is not None:
            stats["rows_in_file"] = len(df)
            stats["rows_total"] += len(df)

            valid_prices = [p for p in df["price"] if p is not None]
            null_count = len(df) - len(valid_prices)

            stats["sum"] += sum(valid_prices)

            if valid_prices:
                stats["min"] = min(stats["min"], min(valid_prices))
                stats["max"] = max(stats["max"], max(valid_prices))

            avg_price = stats["sum"] / stats["rows_total"] if stats["rows_total"] else 0

            logging.info(
                f"[Archivo completado | {stats['rows_in_file']} filas en archivo | {null_count} precios nulos] "
                f"Avg global: {avg_price:.4f} | "
                f"Min global: {stats['min']} | Max global: {stats['max']} | "
                f"Filas totales globales: {stats['rows_total']}"
            )

        else:
            logging.warning(f"Método update_stats: modo '{mode}' no válido o falta DataFrame.")

    except Exception as e:
        logging.error(f"Error en update_stats: {e}")
        raise

def process_file(file_path: str, stats: dict, stats_update_mode: str = "file") -> dict:
    """
    Procesa un archivo CSV, lo inserta en la base y actualiza estadísticas.

    Args:
        file_path (str): Ruta del archivo CSV a procesar.
        stats (dict): Diccionario de estadísticas con claves: count, sum, min, max.
        stats_update_mode (str): Forma de actualización de estadísticas:
                           'row' -> fila a fila,
                           'file' -> solo después de procesar todo el archivo.

    Returns:
        dict: Estadísticas actualizadas.
    """
    logging.info(f"----- Procesando archivo: {file_path} -----")

    try:
        df = pl.read_csv(file_path)
    except Exception as e:
        logging.error(f"!- Error leyendo el archivo {file_path}: {e}")
        raise

    try:
        with get_session() as session:
            if stats_update_mode == "row":
                # Reset del contador por archivo
                stats["rows_in_file"] = 0

                for row in df.iter_rows(named=True):
                    ts = parse_date(row["timestamp"])

                    transaction = Transaction(
                        timestamp=ts,
                        price=row["price"],
                        user_id=row["user_id"]
                    )
                    session.add(transaction)
                    # Actualizar estadísticas por fila
                    update_stats(stats, row["price"], mode=stats_update_mode)

            elif stats_update_mode == "file":
                for row in df.iter_rows(named=True):
                    ts = parse_date(row["timestamp"])

                    transaction = Transaction(
                        timestamp=ts,
                        price=row["price"],
                        user_id=row["user_id"]
                    )
                    session.add(transaction)
                # Actualizar estadísticas por archivo
                update_stats(stats, None, mode=stats_update_mode, df=df)

            else:
                raise ValueError(f"Modo de actualización no válido: {stats_update_mode}")

            session.commit()
            logging.info("Datos cargados en PostgreSQL.")

    except Exception as e:
        logging.error(f"Error procesando e insertando datos de {file_path}: {e}")
        raise

    return stats