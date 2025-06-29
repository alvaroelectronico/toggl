from sqlalchemy import create_engine
import warnings
import pandas as pd
from datetime import datetime, timedelta
import unidecode
# import admin_report.data_io.aux_functions as aux
# from admin_report.data.config import (
#     TOGGL_CACHE_PATH,
#     ASGINACION_CACHE_PATH,
#     TIPOS_ACTIVIDADES
# )
import psycopg2
from dotenv import load_dotenv
import os
from typing import Tuple, Optional
import logging
# from admin_report.toggl_read.config import TIMEFORK_CACHE_PATH # TODO: cambiar para que todo el config.py esté a un nivel superior

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Clase para manejar las conexiones a la base de datos"""
    
    def __init__(self):
        self._load_env_variables()
        self._create_connections()
    
    def _load_env_variables(self):
        """Carga las variables de entorno necesarias"""
        load_dotenv()
        self.host = os.environ.get('host_timefork')
        self.database_timefork = os.environ.get('database_timefork')
        self.user = os.environ.get('user_timefork')
        self.password = os.environ.get('password_timefork')
        self.port = os.environ.get('port_timefork')
        
        # Validar que todas las variables necesarias existen
        required_vars = ['host_timefork', 'database_timefork', 'user_timefork', 
                        'password_timefork', 'port_timefork']
        missing_vars = [var for var in required_vars 
                       if not os.environ.get(var)]
        if missing_vars:
            raise EnvironmentError(
                f"Faltan las siguientes variables de entorno: {missing_vars}")
    
    def _create_connections(self):
        """Crea las conexiones a las bases de datos"""
        self.db_timefork_uri = (f"postgresql://{self.user}:{self.password}@"
                            f"{self.host}:{self.port}/{self.database_timefork}")
        
        self.engine_timefork = create_engine(self.db_timefork_uri)

def _convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas de fecha al formato correcto.
    Solo convierte las columnas que existen en el DataFrame.
    
    Args:
        df: DataFrame a procesar
        
    Returns:
        DataFrame con las columnas de fecha convertidas
    """
    date_columns = ["date", "lunes_semana", "mes", "updated", "principio"]
    # Solo convertir las columnas que existen en el DataFrame
    existing_date_columns = [col for col in date_columns if col in df.columns]
    
    if existing_date_columns:
        return aux.convert_columns_to_datetime(df, existing_date_columns)
    return df

def get_timefork(start_date: str, end_date: str, use_cache: bool) -> pd.DataFrame:
    try:
        # if os.path.exists(TIMEFORK_CACHE_PATH) and use_cache:
        #     logger.info(f"Leyendo caché de Timefork: {start_date} a {end_date}")
        #     df = pd.read_csv(TIMEFORK_CACHE_PATH)
        #     df = _convert_date_columns(df)
        # else:
        logger.info(f"Leyendo Timefork de BBDD: {start_date} a {end_date}")
        db = DatabaseConnection()
        df = read_bbdd_timefork(start_date, end_date, db.engine_timefork)
        # _save_to_cache(df, TOGGL_CACHE_PATH)
            
        return _filter_date_range(df, start_date, end_date)
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Timefork: {str(e)}")
        raise

def _save_to_cache(df: pd.DataFrame, cache_path: str) -> None:
    """Guarda el DataFrame en caché"""
    try:
        df.to_csv(cache_path, index=False)
    except Exception as e:
        logger.warning(f"Error al guardar caché en {cache_path}: {str(e)}")

def _filter_date_range(df: pd.DataFrame, start_date: str, 
                      end_date: str) -> pd.DataFrame:
    """Filtra el DataFrame por rango de fechas"""
    return df[(df.date >= start_date) & (df.date <= end_date)]

def read_bbdd_timefork(start_date: str, end_date: str, engine) -> pd.DataFrame:
    """
    Lee datos de Toggl desde la base de datos.
    
    Args:
        start_date: Fecha de inicio en formato 'YYYY-MM-DD'
        end_date: Fecha de fin en formato 'YYYY-MM-DD'
        engine: Conexión SQLAlchemy a la base de datos
        
    Returns:
        DataFrame con los datos de Toggl procesados
    """
    try:
        # Consulta SQL
        query_str = f"""
            SELECT * 
            FROM timefork_time_entries 
            WHERE start_date >= '{start_date}' 
            AND end_date <= '{end_date}'
        """
        # AND user_email = 'alvaro.garcia@baobabsoluciones.es'
        
        logger.info(f"Ejecutando consulta Timefork: {query_str}")
        df_entries = pd.read_sql(query_str, engine)
        
        # Renombrar columnas
        df_entries = df_entries[['start_date', 'client', 'project', 'end_date', 'description']]

        df_entries['date'] = df_entries['start_date'].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
        df_entries['h_toggl'] = df_entries['end_date'] - df_entries['start_date']
        df_entries['h_toggl'] = df_entries['h_toggl'].apply(lambda x: x.total_seconds() / 3600)
        df_entries['lunes_semana'] = df_entries['date'].apply(
            lambda x: pd.to_datetime(x)
                      - timedelta(days=pd.to_datetime(x).weekday() % 7)
        )
        df_entries['workspace'] = 'Socios'
        df_entries.rename(columns={'start_date': 'start'}, inplace=True)
        
       
        # Eliminar acentos
        df_entries = df_entries.apply(lambda x: x.map(lambda x: unidecode.unidecode(x) if isinstance(x, str) else x))
        df_entries = df_entries[['date', 'client', 'project', 'h_toggl', 'description', 'start', 'lunes_semana', 'workspace']]

        
        return df_entries
        
    except Exception as e:
        logger.error(f"Error al leer datos de Timefork: {str(e)}")
        raise


if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    start_date = "2024-09-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    use_cache = True 
    
    try:
        # Inicializar conexión a la base de datos
        logger.info("Iniciando pruebas de bbdd.py")
        db = DatabaseConnection()
        
        
        logger.info("Probando obtención de datos Timefork")
        df_timefork = get_timefork(start_date, end_date, use_cache)
        print("\nMuestra de datos Timefork:")
        print("Forma:", df_timefork.shape)
        print("Columnas:", df_timefork.columns.tolist())
        print("\nPrimeras 5 filas:")
        print(df_timefork.head())
        print("\nEstadísticas de horas registradas:")
        print(df_timefork['h_toggl'].describe())
        
        logger.info("Pruebas completadas exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante las pruebas: {str(e)}")
        raise
