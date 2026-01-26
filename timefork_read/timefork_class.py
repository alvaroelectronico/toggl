from sqlalchemy import create_engine
import warnings
import pandas as pd
from datetime import datetime, timedelta
import unidecode
from timefork_read.config import TIMEFORK_CACHE_PATH
import psycopg2
from dotenv import load_dotenv
import os
from typing import Tuple, Optional
import logging


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


class TimeforkObj:
    def __init__(self, 
        start_date: str, 
        end_date: str, 
        days_no_cache: int = 3,
        export_cache_to_json: bool = True,
        db: DatabaseConnection = None, 
        timefork_cache_path: str = TIMEFORK_CACHE_PATH,
        get_entries: bool = True
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.db = db
        self.days_no_cache = days_no_cache
        self.export_cache_to_json = export_cache_to_json
        self.timefork_cache_path = timefork_cache_path
        self.get_entries = get_entries
        
        self.dates_cache = list()
        self.dates_no_cache = list()
        self._get_dates_cache_no_cache(self.start_date, self.end_date)

        if self.get_entries:
            self.get_df_timefork()

        
    def _convert_date_columns(self, df: pd.DataFrame) -> pd.DataFrame:
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
            # Comentado porque aux no está importado
            # return aux.convert_columns_to_datetime(df, existing_date_columns)
            return df
        return df

    def read_cache_day(self, date):
        """ "
        Returning a dataframe with Timefork information extracted from cache json files
        """

        file_path = "{}\{}.json".format(self.timefork_cache_path, date)
        try:
            # with open(file_path, "r") as f:
            #     json_string = StringIO(f.read())
            # df_toggl = pd.read_json(json_string, orient="records", lines=True)

            df_timefork = pd.read_json(file_path, orient="records", lines=True)
        except:
            return None

        try:
            df_timefork.date = df_timefork.date.apply(lambda x: pd.to_datetime(x))
            df_timefork.start = df_timefork.start.apply(lambda x: pd.to_datetime(x))
            df_timefork.lunes_semana = df_timefork.lunes_semana.apply(
                lambda x: pd.to_datetime(x)
            )
        except:
            return None

        return df_timefork

    def read_cache(self, start_date, end_date):
        if os.path.exists(self.timefork_cache_path):
            dates = pd.date_range(start_date, end_date)
            dates = [d.strftime("%Y-%m-%d") for d in dates]
            df_timefork = pd.DataFrame()
            for d in dates:
                df = self.read_cache_day(d)

                if df is not None:
                    df_timefork = pd.concat([df_timefork, df])
            df_timefork.reset_index(inplace=True)
            df_timefork.drop("index", axis=1, inplace=True)
            return df_timefork

    def get_df_timefork(self) -> pd.DataFrame:
        try:
            if len(self.dates_cache) > 0:
                start_date = self.dates_cache[0]
                end_date = self.dates_cache[len(self.dates_cache) - 1]
                logger.info(f"Leyendo Timefork de cache: {start_date} a {end_date}")
                df = self.read_cache(start_date, end_date)
            else:
                df = pd.DataFrame()

            if len(self.dates_no_cache) > 0:
                start_date = self.dates_no_cache[0]
                end_date = self.dates_no_cache[len(self.dates_no_cache) - 1 ]
                logger.info(f"Leyendo Timefork de BBDD: {start_date} a {end_date}")
                df_no_cache = self.read_bbdd_timefork(start_date, end_date, self.db.engine_timefork)
                df = pd.concat([df, df_no_cache])

            df.reset_index(inplace=True)
            df.drop("index", axis=1, inplace=True)
            self.df_timefork = df

        except Exception as e:
            logger.error(f"Error al obtener datos de Timefork: {str(e)}")
            raise

    def df_timefork_to_json_files(self, df_timefork):
        """
        Given a dataframe with Toggl info, toggl_read is stored in as many json files as days
        """
        if self.timefork_cache_path is not None:
            df = df_timefork.copy(deep=True)
            # Todos los valores en la columna 'date' son objetos datetime
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Ahora se puede aplicar strftime para formatear las fechas
            df['date'] = df['date'].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else '')
            df.lunes_semana = df.lunes_semana.apply(lambda x: x.strftime("%Y-%m-%d"))
            df['start'] = pd.to_datetime(df['start'], errors='coerce', utc=True)
            df['start'] = df['start'].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else '')


            dates = pd.date_range(df.date.min(), df.date.max()).tolist()
            dates = [d.strftime("%Y-%m-%d") for d in dates]

            for d in dates:
                df_to_json = df[df.date == d]
                if df_to_json.shape[0] > 0:
                    entries = df_to_json.to_json(
                        orient="records", lines=True, date_format="iso"
                    )
                    file_path = "{}/{}.json".format(self.timefork_cache_path, d)
                    with open(file_path, "w") as f:
                        f.write(entries)
        else:
            print("no json files generated (no cache path given)")

    def df_timefork_to_csv(self, file_name):
        self.df_timefork.to_csv(file_name)

    def _save_to_cache(self, df: pd.DataFrame, cache_path: str) -> None:
        """Guarda el DataFrame en caché"""
        try:
            df.to_csv(cache_path, index=False)
        except Exception as e:
            logger.warning(f"Error al guardar caché en {cache_path}: {str(e)}")

    def _filter_date_range(self, df: pd.DataFrame, start_date: str, 
                        end_date: str) -> pd.DataFrame:
        """Filtra el DataFrame por rango de fechas"""
        return df[(df.date >= start_date) & (df.date <= end_date)]

    def read_bbdd_timefork(self, start_date: str, end_date: str, engine) -> pd.DataFrame:
        """
        Lee datos de Timefork desde la base de datos.
        
        Args:
            start_date: Fecha de inicio en formato 'YYYY-MM-DD'
            end_date: Fecha de fin en formato 'YYYY-MM-DD'
            engine: Conexión SQLAlchemy a la base de datos
            
        Returns:
            DataFrame con los datos de Timefork procesados
        """
        try:
            # Consulta SQL
            query_str = f"""
                SELECT * 
                FROM timefork_time_entries 
                WHERE start_date >= '{start_date}' 
                AND end_date <= '{end_date}'
                AND user_email = 'alvaro.garcia@baobabsoluciones.es'
            """
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


            if self.export_cache_to_json:
                self.df_timefork_to_json_files(df_entries)

            return df_entries
            
        except Exception as e:
            logger.error(f"Error al leer datos de Timefork: {str(e)}")
            raise

    def _get_dates_cache_no_cache(self, start_date, end_date):
            dates = pd.date_range(start_date, end_date)
            self.days_cache = max(0, len(dates) - self.days_no_cache)
            self.dates_cache = dates[0 : self.days_cache]
            self.dates_no_cache = dates[self.days_cache :]
    
if __name__ == "__main__":
    from datetime import datetime, timedelta
    days_no_cache = 100
    start_date = "2025-09-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    get_entries = True
    export_cache_to_json = True
    
    try:
        # Inicializar conexión a la base de datos
        logger.info("Iniciando pruebas de bbdd.py")
        db = DatabaseConnection()
        
        logger.info("Probando obtención de datos Timefork")
        timefork_obj = TimeforkObj(
            start_date=start_date, 
            end_date=end_date, 
            days_no_cache=days_no_cache, 
            export_cache_to_json=export_cache_to_json, 
            db=db, 
            timefork_cache_path=TIMEFORK_CACHE_PATH,
            get_entries=get_entries
        )
        timefork_obj.get_df_timefork()
        df_timefork = timefork_obj.df_timefork
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
