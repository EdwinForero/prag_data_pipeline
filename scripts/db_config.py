import os
import sys
import logging
from sqlmodel import create_engine
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class DatabaseConfig:
    def __init__(self):
        try:
            logging.info("Inicializando configuración de base de datos...")
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.local_run = "Users" in os.path.dirname(os.path.abspath(sys.argv[0]))
            self.env_file = ".env.local" if self.local_run else ".env"

            self._load_environment()

            # Variables de conexión (con valores por defecto)
            self.user = os.getenv("DATABASE_USER", "postgres")
            self.password = os.getenv("DATABASE_PASSWORD", "postgres")
            self.host = os.getenv("DATABASE_HOST", "localhost")
            self.port = os.getenv("DATABASE_PORT", "5432")
            self.database = os.getenv("DATABASE_NAME", "pragma_db")  # default pragma_db

            self.database_url = self._build_database_url()
            self.engine = self._create_engine()

        except Exception as e:
            logging.error(f"Error inicializando DatabaseConfig: {e}")
            raise

    def _load_environment(self):
        """Carga el archivo .env adecuado según entorno"""
        try:
            env_path = os.path.join(self.base_dir, self.env_file)
            if not os.path.exists(env_path):
                raise FileNotFoundError(f"No se encontró el archivo {env_path}")

            load_dotenv(env_path, override=True)
            logging.info(f"Variables de entorno cargadas desde: {env_path}")

        except Exception as e:
            logging.error(f"Error cargando archivo .env: {e}")
            raise

    def _build_database_url(self) -> str:
        """Construye la URL de conexión a PostgreSQL"""
        try:
            url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            logging.info(f"URL de conexión: {url}")
            return url
        except Exception as e:
            logging.error(f"Error construyendo DATABASE_URL: {e}")
            raise

    def _create_engine(self):
        """Crea y devuelve el engine de SQLModel"""
        try:
            logging.info("Creando engine de conexión a base de datos...")
            engine = create_engine(self.database_url)
            logging.info("Engine creado con éxito.")
            return engine
        except Exception as e:
            logging.error(f"Error creando engine: {e}")
            raise

    def get_engine(self):
        """Devuelve el engine"""
        return self.engine