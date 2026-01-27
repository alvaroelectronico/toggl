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
        Lee datos de Timefork desde la base de datos y hace joins con las tablas de referencia
        para reemplazar IDs con nombres.
        
        Args:
            start_date: Fecha de inicio en formato 'YYYY-MM-DD'
            end_date: Fecha de fin en formato 'YYYY-MM-DD'
            engine: Conexión SQLAlchemy a la base de datos (mantenido por compatibilidad)
            
        Returns:
            DataFrame con los datos de Timefork procesados y nombres en lugar de IDs
        """
        try:
            # Leer las tablas de referencia
            logger.info("Leyendo tablas de referencia...")
            df_clients = self.get_df_clients()
            df_projects = self.get_df_projects()
            df_phases = self.get_df_phases()
            df_blocks = self.get_df_blocks()
            
            # Leer las entradas de tiempo
            df_entries = self.get_df_time_entries(start_date, end_date)
            
            if df_entries.empty:
                logger.warning("No se encontraron entradas en el rango de fechas especificado")
                return pd.DataFrame()
            
            # Detectar automáticamente las columnas de ID en df_entries
            # Buscar columnas que puedan ser IDs
            client_id_col = None
            project_id_col = None
            phase_id_col = None
            block_id_col = None
            
            for col in df_entries.columns:
                col_lower = col.lower()
                if col_lower in ['client_id', 'clientid'] and client_id_col is None:
                    client_id_col = col
                elif col_lower in ['project_id', 'projectid'] and project_id_col is None:
                    project_id_col = col
                elif col_lower in ['phase_id', 'phaseid'] and phase_id_col is None:
                    phase_id_col = col
                elif col_lower in ['block_id', 'blockid'] and block_id_col is None:
                    block_id_col = col
            
            # Hacer joins para reemplazar IDs con nombres
            # Join con clients
            if client_id_col:
                df_entries = df_entries.merge(
                    df_clients,
                    left_on=client_id_col,
                    right_on='client_id',
                    how='left',
                    suffixes=('', '_client')
                )
                # Eliminar la columna de ID y mantener solo el nombre
                if client_id_col in df_entries.columns:
                    df_entries = df_entries.drop(columns=[client_id_col])
                if 'client_id' in df_entries.columns:
                    df_entries = df_entries.drop(columns=['client_id'])
                logger.info("Join con tabla de clientes completado")
            
            # Join con projects
            if project_id_col:
                df_entries = df_entries.merge(
                    df_projects,
                    left_on=project_id_col,
                    right_on='project_id',
                    how='left',
                    suffixes=('', '_project')
                )
                # Eliminar la columna de ID y mantener solo el nombre
                if project_id_col in df_entries.columns:
                    df_entries = df_entries.drop(columns=[project_id_col])
                if 'project_id' in df_entries.columns:
                    df_entries = df_entries.drop(columns=['project_id'])
                logger.info("Join con tabla de proyectos completado")
            
            # Join con phases
            if phase_id_col:
                df_entries = df_entries.merge(
                    df_phases,
                    left_on=phase_id_col,
                    right_on='phase_id',
                    how='left',
                    suffixes=('', '_phase')
                )
                # Eliminar la columna de ID y mantener solo el nombre
                if phase_id_col in df_entries.columns:
                    df_entries = df_entries.drop(columns=[phase_id_col])
                if 'phase_id' in df_entries.columns:
                    df_entries = df_entries.drop(columns=['phase_id'])
                logger.info("Join con tabla de fases completado")
            
            # Join con blocks
            if block_id_col:
                df_entries = df_entries.merge(
                    df_blocks,
                    left_on=block_id_col,
                    right_on='block_id',
                    how='left',
                    suffixes=('', '_block')
                )
                # Eliminar la columna de ID y mantener solo el nombre
                if block_id_col in df_entries.columns:
                    df_entries = df_entries.drop(columns=[block_id_col])
                if 'block_id' in df_entries.columns:
                    df_entries = df_entries.drop(columns=['block_id'])
                logger.info("Join con tabla de bloques completado")
            
            # Procesar las columnas de fecha y tiempo
            # Asegurarse de que start_date y end_date existen
            if 'start_date' in df_entries.columns:
                df_entries['date'] = df_entries['start_date'].apply(lambda x: datetime.strftime(x, "%Y-%m-%d") if pd.notna(x) else None)
                df_entries['h_toggl'] = None
                if 'end_date' in df_entries.columns:
                    df_entries['h_toggl'] = df_entries['end_date'] - df_entries['start_date']
                    df_entries['h_toggl'] = df_entries['h_toggl'].apply(
                        lambda x: x.total_seconds() / 3600 if pd.notna(x) else None
                    )
                
                df_entries['lunes_semana'] = df_entries['date'].apply(
                    lambda x: pd.to_datetime(x) - timedelta(days=pd.to_datetime(x).weekday() % 7) 
                    if pd.notna(x) else None
                )
                df_entries['workspace'] = 'Socios'
                df_entries.rename(columns={'start_date': 'start'}, inplace=True)
            
            # Eliminar columnas no deseadas
            cols_to_drop = ['id', 'user_email', 'user_id', 'end_date']
            df_entries = df_entries.drop(columns=[col for col in cols_to_drop if col in df_entries.columns])
            
            # Seleccionar y ordenar columnas principales
            # Intentar mantener las columnas esperadas, pero ser flexible si no existen
            expected_cols = ['date', 'client', 'project', 'phase', 'block', 'h_toggl', 'description', 'start', 'lunes_semana', 'workspace']
            available_cols = [col for col in expected_cols if col in df_entries.columns]
            # Agregar cualquier otra columna que no esté en la lista esperada
            other_cols = [col for col in df_entries.columns if col not in expected_cols]
            df_entries = df_entries[available_cols + other_cols]
            
            # Eliminar acentos de todas las columnas de texto
            for col in df_entries.columns:
                if df_entries[col].dtype == 'object':
                    df_entries[col] = df_entries[col].apply(
                        lambda x: unidecode.unidecode(x) if isinstance(x, str) else x
                    )
            
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
    
    def get_df_time_entries(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Lee la tabla timefork_time_entries desde la base de datos.
        
        Args:
            start_date: Fecha de inicio en formato 'YYYY-MM-DD'
            end_date: Fecha de fin en formato 'YYYY-MM-DD'
            
        Returns:
            DataFrame con todas las columnas de timefork_time_entries
        """
        if self.db is None:
            raise ValueError("No se ha proporcionado una conexión a la base de datos")
        
        try:
            table_name = 'timefork_time_entries'
            query_str = f"""
                SELECT * 
                FROM {table_name} 
                WHERE start_date >= '{start_date}' 
                AND end_date <= '{end_date}'
                AND user_email = 'alvaro.garcia@baobabsoluciones.es'
            """
            logger.info(f"Leyendo tabla {table_name}")
            df_entries = pd.read_sql(query_str, self.db.engine_timefork)
            
            logger.info(f"Se leyeron {len(df_entries)} entradas de la tabla {table_name}")
            return df_entries
            
        except Exception as e:
            logger.error(f"Error al leer datos de {table_name}: {str(e)}")
            raise
    
    def get_df_clients(self) -> pd.DataFrame:
        """
        Lee la tabla timefork_clients desde la base de datos.
        
        Returns:
            DataFrame con las columnas 'client_id' y 'client'
        """
        if self.db is None:
            raise ValueError("No se ha proporcionado una conexión a la base de datos")
        
        try:
            table_name = 'timefork_clients'
            query_str = f"SELECT * FROM {table_name}"
            logger.info(f"Leyendo tabla {table_name}")
            df_clients = pd.read_sql(query_str, self.db.engine_timefork)
            
            # Detectar automáticamente las columnas de ID y nombre
            # Buscar columna de ID (puede ser 'id', 'client_id', etc.)
            id_column = None
            name_column = None
            
            for col in df_clients.columns:
                col_lower = col.lower()
                if col_lower in ['id', 'client_id'] and id_column is None:
                    id_column = col
                elif col_lower in ['name', 'client', 'client_name'] and name_column is None:
                    name_column = col
            
            # Si no se encontraron, usar las primeras dos columnas
            if id_column is None:
                id_column = df_clients.columns[0]
                logger.warning(f"No se encontró columna de ID, usando: {id_column}")
            
            if name_column is None:
                name_column = df_clients.columns[1] if len(df_clients.columns) > 1 else df_clients.columns[0]
                logger.warning(f"No se encontró columna de nombre, usando: {name_column}")
            
            # Crear DataFrame con las columnas estandarizadas
            df_result = pd.DataFrame({
                'client_id': df_clients[id_column],
                'client': df_clients[name_column]
            })
            
            # Eliminar acentos de la columna client
            df_result['client'] = df_result['client'].apply(
                lambda x: unidecode.unidecode(x) if isinstance(x, str) else x
            )
            
            logger.info(f"Se leyeron {len(df_result)} clientes de la tabla {table_name}")
            return df_result
            
        except Exception as e:
            logger.error(f"Error al leer datos de {table_name}: {str(e)}")
            raise
    
    def get_df_projects(self) -> pd.DataFrame:
        """
        Lee la tabla timefork_projects desde la base de datos.
        
        Returns:
            DataFrame con las columnas 'project_id' y 'project'
        """
        if self.db is None:
            raise ValueError("No se ha proporcionado una conexión a la base de datos")
        
        try:
            table_name = 'timefork_projects'
            query_str = f"SELECT * FROM {table_name}"
            logger.info(f"Leyendo tabla {table_name}")
            df_projects = pd.read_sql(query_str, self.db.engine_timefork)
            
            # Detectar automáticamente las columnas de ID y nombre
            id_column = None
            name_column = None
            
            for col in df_projects.columns:
                col_lower = col.lower()
                if col_lower in ['id', 'project_id'] and id_column is None:
                    id_column = col
                elif col_lower in ['name', 'project', 'project_name'] and name_column is None:
                    name_column = col
            
            # Si no se encontraron, usar las primeras dos columnas
            if id_column is None:
                id_column = df_projects.columns[0]
                logger.warning(f"No se encontró columna de ID, usando: {id_column}")
            
            if name_column is None:
                name_column = df_projects.columns[1] if len(df_projects.columns) > 1 else df_projects.columns[0]
                logger.warning(f"No se encontró columna de nombre, usando: {name_column}")
            
            # Crear DataFrame con las columnas estandarizadas
            df_result = pd.DataFrame({
                'project_id': df_projects[id_column],
                'project': df_projects[name_column]
            })
            
            # Eliminar acentos de la columna project
            df_result['project'] = df_result['project'].apply(
                lambda x: unidecode.unidecode(x) if isinstance(x, str) else x
            )
            
            logger.info(f"Se leyeron {len(df_result)} proyectos de la tabla {table_name}")
            return df_result
            
        except Exception as e:
            logger.error(f"Error al leer datos de {table_name}: {str(e)}")
            raise
    
    def get_df_phases(self) -> pd.DataFrame:
        """
        Lee la tabla timefork_phase desde la base de datos.
        
        Returns:
            DataFrame con las columnas 'phase_id' y 'phase'
        """
        if self.db is None:
            raise ValueError("No se ha proporcionado una conexión a la base de datos")
        
        try:
            table_name = 'timefork_phase'
            query_str = f"SELECT * FROM {table_name}"
            logger.info(f"Leyendo tabla {table_name}")
            df_phases = pd.read_sql(query_str, self.db.engine_timefork)
            
            # Detectar automáticamente las columnas de ID y nombre
            id_column = None
            name_column = None
            
            for col in df_phases.columns:
                col_lower = col.lower()
                if col_lower in ['id', 'phase_id'] and id_column is None:
                    id_column = col
                elif col_lower in ['name', 'phase', 'phase_name'] and name_column is None:
                    name_column = col
            
            # Si no se encontraron, usar las primeras dos columnas
            if id_column is None:
                id_column = df_phases.columns[0]
                logger.warning(f"No se encontró columna de ID, usando: {id_column}")
            
            if name_column is None:
                name_column = df_phases.columns[1] if len(df_phases.columns) > 1 else df_phases.columns[0]
                logger.warning(f"No se encontró columna de nombre, usando: {name_column}")
            
            # Crear DataFrame con las columnas estandarizadas
            df_result = pd.DataFrame({
                'phase_id': df_phases[id_column],
                'phase': df_phases[name_column]
            })
            
            # Eliminar acentos de la columna phase
            df_result['phase'] = df_result['phase'].apply(
                lambda x: unidecode.unidecode(x) if isinstance(x, str) else x
            )
            
            logger.info(f"Se leyeron {len(df_result)} fases de la tabla {table_name}")
            return df_result
            
        except Exception as e:
            logger.error(f"Error al leer datos de {table_name}: {str(e)}")
            raise
    
    def get_df_blocks(self) -> pd.DataFrame:
        """
        Lee la tabla timefork_block desde la base de datos.
        
        Returns:
            DataFrame con las columnas 'block_id' y 'block'
        """
        if self.db is None:
            raise ValueError("No se ha proporcionado una conexión a la base de datos")
        
        try:
            table_name = 'timefork_block'
            query_str = f"SELECT * FROM {table_name}"
            logger.info(f"Leyendo tabla {table_name}")
            df_blocks = pd.read_sql(query_str, self.db.engine_timefork)
            
            # Detectar automáticamente las columnas de ID y nombre
            id_column = None
            name_column = None
            
            for col in df_blocks.columns:
                col_lower = col.lower()
                if col_lower in ['id', 'block_id'] and id_column is None:
                    id_column = col
                elif col_lower in ['name', 'block', 'block_name'] and name_column is None:
                    name_column = col
            
            # Si no se encontraron, usar las primeras dos columnas
            if id_column is None:
                id_column = df_blocks.columns[0]
                logger.warning(f"No se encontró columna de ID, usando: {id_column}")
            
            if name_column is None:
                name_column = df_blocks.columns[1] if len(df_blocks.columns) > 1 else df_blocks.columns[0]
                logger.warning(f"No se encontró columna de nombre, usando: {name_column}")
            
            # Crear DataFrame con las columnas estandarizadas
            df_result = pd.DataFrame({
                'block_id': df_blocks[id_column],
                'block': df_blocks[name_column]
            })
            
            # Eliminar acentos de la columna block
            df_result['block'] = df_result['block'].apply(
                lambda x: unidecode.unidecode(x) if isinstance(x, str) else x
            )
            
            logger.info(f"Se leyeron {len(df_result)} bloques de la tabla {table_name}")
            return df_result
            
        except Exception as e:
            logger.error(f"Error al leer datos de {table_name}: {str(e)}")
            raise
    
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
