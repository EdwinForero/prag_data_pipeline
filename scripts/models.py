import logging
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import date  # solo fecha

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

try:
    class Transaction(SQLModel, table=True):
        """
        Modelo ORM que representa las transacciones en la base de datos `pagme_db`.

        Atributos:
            id: Clave primaria autoincremental (único campo con NOT NULL).
            timestamp: Solo fecha (puede ser nula).
            price: Valor numérico de la transacción (puede ser nulo).
            user_id: ID del usuario que realizó la transacción (puede ser nulo).
        """
        __tablename__ = "data_test"

        id: Optional[int] = Field(default=None, primary_key=True)  # NOT NULL
        timestamp: Optional[date] = None  # Solo fecha
        price: Optional[float] = None
        user_id: Optional[int] = None

    logging.info("- Modelo Transaction definido correctamente con tabla 'data_test'.")

except Exception as e:
    logging.error(f"!- Error al definir el modelo Transaction: {e}")
    raise
