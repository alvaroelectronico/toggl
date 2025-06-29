import numpy as np
from time import *
import pandas as pd
from datetime import *
import os
import sys
import logging
from typing import Optional, Dict, Any

# Agregar el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importaciones de las clases principales
from data_io import drive_io as dr
from toggl_read.toggl_class import ToggleObj
from timefork_read.timefork_class import TimeforkObj, DatabaseConnection
from timelog_read.config import ID_GSHEET_2425, ID_SHEET_TOGGL_WEEKLY, ID_SHEET_TOGGL_DAILY, ID_SHEET_TOGGL_ALL

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TimelogObj:
    """
    Clase que combina las funcionalidades de ToggleObj y TimeforkObj
    para manejar datos de registro de tiempo desde múltiples fuentes.
    """
    
    def __init__(self, 
                 start_date: str, 
                 end_date: str,
                 use_cache: bool = True,
                 days_no_cache: int = 3,
                 export_cache_to_json: bool = True,
                 export_to_gsheet: bool = False,
                 id_gsheet: Optional[str] = None):
        """
        Inicializa la clase TimelogObj.
        
        Args:
            start_date: Fecha de inicio en formato 'YYYY-MM-DD'
            end_date: Fecha de fin en formato 'YYYY-MM-DD'
            use_cache: Si usar caché para los datos
            days_no_cache: Días sin caché para Toggl
            export_to_gsheet: Si exportar a Google Sheets
            id_gsheet: ID de la hoja de Google Sheets
        """
        self.start_date = start_date
        self.end_date = end_date
        self.use_cache = use_cache
        self.days_no_cache = days_no_cache
        self.export_cache_to_json = export_cache_to_json
        self.export_to_gsheet = export_to_gsheet
        self.id_gsheet = id_gsheet
        
        # DataFrames para almacenar los datos
        self.df_toggl = pd.DataFrame()
        self.df_timefork = pd.DataFrame()
        self.df_combined = pd.DataFrame()

        # DataFrames para almacenar los resúmenes y exportar a Google Sheets
        self.df_toggl_summary_all = pd.DataFrame()
        self.df_summary_day = pd.DataFrame()
        self.df_summary_to_xlsx = pd.DataFrame()
        self.df_summary_week = pd.DataFrame()
        self.id_gsheet = id_gsheet
        self.export_to_gsheet = export_to_gsheet

        # Inicializar conexiones y objetos
        self._initialize_objects()

        # Obtener datos
        self.get_toggl_data()
        self.get_timefork_data()
        self.get_combined_data()
        
        # Exportar a Google Sheets
        if self.export_to_gsheet and self.id_gsheet is not None:
            self._get_agg_dfs()
            self._write_gsheet()
        
    def _initialize_objects(self):
        """Inicializa los objetos ToggleObj y TimeforkObj"""
        try:
            logger.info("Inicializando objetos de registro de tiempo...")
            
            # Inicializar conexión a base de datos para Timefork
            self.db_connection = DatabaseConnection()
            
            # Inicializar objeto Toggl
            self.toggl_obj = ToggleObj(
                start_date=self.start_date,
                end_date=self.end_date,
                days_no_cache=self.days_no_cache,
                export_cache_to_json=self.export_cache_to_json,
                get_entries=True
            )
            
            # Inicializar objeto Timefork
            self.timefork_obj = TimeforkObj(
                start_date=self.start_date,
                end_date=self.end_date,
                days_no_cache=self.days_no_cache,
                export_cache_to_json=self.export_cache_to_json,
                db=self.db_connection
            )
            
            logger.info("Objetos inicializados correctamente")
            
        except Exception as e:
            logger.error(f"Error al inicializar objetos: {str(e)}")
            raise
    
    def get_toggl_data(self) -> pd.DataFrame:
        """
        Obtiene datos de Toggl.
        
        Returns:
            DataFrame con los datos de Toggl
        """
        try:
            logger.info("Obteniendo datos de Toggl...")
            self.df_toggl = self.toggl_obj.df_toggl
            logger.info(f"Datos de Toggl obtenidos: {len(self.df_toggl)} registros")
            return self.df_toggl
        except Exception as e:
            logger.error(f"Error al obtener datos de Toggl: {str(e)}")
            raise
    
    def get_timefork_data(self) -> pd.DataFrame:
        """
        Obtiene datos de Timefork.
        
        Returns:
            DataFrame con los datos de Timefork
        """
        try:
            logger.info("Obteniendo datos de Timefork...")
            self.df_timefork = self.timefork_obj.df_timefork
            if self.df_timefork is None:
                self.df_timefork = pd.DataFrame()
            logger.info(f"Datos de Timefork obtenidos: {len(self.df_timefork)} registros")
            return self.df_timefork
        except Exception as e:
            logger.error(f"Error al obtener datos de Timefork: {str(e)}")
            self.df_timefork = pd.DataFrame()
            return self.df_timefork
    
    def get_combined_data(self) -> pd.DataFrame:
        """
        Combina los datos de Toggl y Timefork.
        
        Returns:
            DataFrame combinado con datos de ambas fuentes
        """
        try:
            logger.info("Combinando datos de Toggl y Timefork...")
            
            # Obtener datos si no están disponibles
            if self.df_toggl.empty:
                self.get_toggl_data()
            if self.df_timefork.empty:
                self.get_timefork_data()
            
            # Agregar columna de fuente para identificar el origen
            df_toggl = self.df_toggl.copy()
            df_timefork = self.df_timefork.copy()
            
            df_toggl['source'] = 'toggl'
            df_timefork['source'] = 'timefork'
            
            # Combinar los DataFrames
            self.df_combined = pd.concat([df_toggl, df_timefork], ignore_index=True)
            
            # Asegurar que la columna date esté en formato datetime antes de ordenar
            if not self.df_combined.empty and 'date' in self.df_combined.columns:
                self.df_combined['date'] = pd.to_datetime(self.df_combined['date'])
                self.df_combined = self.df_combined.sort_values('date', ascending=False)
            
            # Eliminar duplicados
            self.df_combined = self.df_combined.drop_duplicates()
            
            logger.info(f"Datos combinados: {len(self.df_combined)} registros totales")
            return self.df_combined
            
        except Exception as e:
            logger.error(f"Error al combinar datos: {str(e)}")
            raise
    
    def _get_agg_dfs(self):
        df_summary_week = self.df_toggl.groupby(['lunes_semana', 'client', 'project'], as_index=False)['h_toggl'].sum()

        df_summary_to_xlsx = df_summary_week.copy(deep=True)

        # Agreggating info by date, client, project
        df_summary_day = (
            self.df_toggl[["date", "client", "project", "h_toggl"]]
            .groupby(by=["date", "client", "project"], as_index=False)
            .sum()
        )

        df_summary_all = (
            self.df_toggl[["date", "client", "project", "h_toggl"]]
            .groupby(by=["client", "project"], as_index=False)
            .sum(numeric_only=True)
        )
        df_summary_day.sort_values(by=["date"], ascending=False, inplace=True)
        df_summary_week.sort_values(by=["lunes_semana"], ascending=False, inplace=True)
        self.df_toggl.sort_values(by=["date"], ascending=False, inplace=True)
        df_summary_to_xlsx.sort_values(
            by=["lunes_semana"], ascending=False, inplace=True
        )

        df_summary_day.date = df_summary_day.date.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )
        df_summary_week.lunes_semana = df_summary_week.lunes_semana.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )
        df_summary_to_xlsx.lunes_semana = df_summary_to_xlsx.lunes_semana.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )

        (
            self.df_summary_all,
            self.df_summary_day,
            self.df_summary_to_xlsx,
            self.df_summary_week,
        ) = (df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week)

    def _write_gsheet(self):
        client = dr.get_client()
        gsheet = dr.get_gsheet(client, self.id_gsheet)

        sheet_all = dr.get_sheet(gsheet, ID_SHEET_TOGGL_ALL)
        sheet_all.clear()
        dr.df_to_gsheet(self.df_summary_all, sheet_all)

        sheet_daily = dr.get_sheet(gsheet, ID_SHEET_TOGGL_DAILY)
        sheet_daily.clear()
        dr.df_to_gsheet(self.df_summary_day, sheet_daily)

        sheet_weekly = dr.get_sheet(gsheet, ID_SHEET_TOGGL_WEEKLY)
        sheet_weekly.clear()
        dr.df_to_gsheet(self.df_summary_week, sheet_weekly)

        date_time = datetime.now().strftime("%H:%M %d-%m-%Y")
        print("Info exported to ghseets\n{}".format(date_time))
        # print("Info exported to ghseets")
    
    def get_summary_by_source(self) -> Dict[str, pd.DataFrame]:
        """
        Obtiene resúmenes de datos por fuente.
        
        Returns:
            Diccionario con resúmenes por fuente
        """
        try:
            if self.df_combined.empty:
                self.get_combined_data()
            
            summary = {}
            
            # Resumen por fuente
            for source in ['toggl', 'timefork']:
                df_source = self.df_combined[self.df_combined['source'] == source]
                if not df_source.empty:
                    summary[source] = {
                        'total_records': len(df_source),
                        'total_hours': df_source['h_toggl'].sum(),
                        'date_range': f"{df_source['date'].min()} - {df_source['date'].max()}",
                        'unique_clients': df_source['client'].nunique(),
                        'unique_projects': df_source['project'].nunique()
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error al generar resumen: {str(e)}")
            raise
    
    def export_to_csv(self, filepath: str, source: str = 'combined'):
        """
        Exporta los datos a un archivo CSV.
        
        Args:
            filepath: Ruta del archivo CSV
            source: Fuente de datos ('toggl', 'timefork', 'combined')
        """
        try:
            if source == 'toggl':
                data_to_export = self.df_toggl
            elif source == 'timefork':
                data_to_export = self.df_timefork
            else:
                data_to_export = self.df_combined
            
            if not data_to_export.empty:
                data_to_export.to_csv(filepath, index=False)
                logger.info(f"Datos exportados a {filepath}")
            else:
                logger.warning(f"No hay datos para exportar de la fuente: {source}")
                
        except Exception as e:
            logger.error(f"Error al exportar datos: {str(e)}")
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas generales de los datos.
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            if self.df_combined.empty:
                self.get_combined_data()
            
            stats = {
                'total_records': len(self.df_combined),
                'total_hours': self.df_combined['h_toggl'].sum(),
                'avg_hours_per_day': self.df_combined.groupby('date')['h_toggl'].sum().mean(),
                'date_range': f"{self.df_combined['date'].min()} - {self.df_combined['date'].max()}",
                'unique_clients': self.df_combined['client'].nunique(),
                'unique_projects': self.df_combined['project'].nunique(),
                'unique_workspaces': self.df_combined['workspace'].nunique() if 'workspace' in self.df_combined.columns else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error al calcular estadísticas: {str(e)}")
            raise

    def _get_agg_dfs(self):
        df_summary_week = self.df_combined.groupby(['lunes_semana', 'client', 'project'], as_index=False)['h_toggl'].sum()

        df_summary_to_xlsx = df_summary_week.copy(deep=True)

        # Agreggating info by date, client, project
        df_summary_day = (
            self.df_combined[["date", "client", "project", "h_toggl"]]
            .groupby(by=["date", "client", "project"], as_index=False)
            .sum()
        )

        df_summary_all = (
            self.df_combined[["date", "client", "project", "h_toggl"]]
            .groupby(by=["client", "project"], as_index=False)
            .sum(numeric_only=True)
        )
        df_summary_day.sort_values(by=["date"], ascending=False, inplace=True)
        df_summary_week.sort_values(by=["lunes_semana"], ascending=False, inplace=True)
        self.df_combined.sort_values(by=["date"], ascending=False, inplace=True)
        df_summary_to_xlsx.sort_values(
            by=["lunes_semana"], ascending=False, inplace=True
        )

        df_summary_day.date = df_summary_day.date.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )
        df_summary_week.lunes_semana = df_summary_week.lunes_semana.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )
        df_summary_to_xlsx.lunes_semana = df_summary_to_xlsx.lunes_semana.apply(
            lambda x: x.strftime("%d/%m/%Y")
        )
        (
            self.df_summary_all,
            self.df_summary_day,
            self.df_summary_to_xlsx,
            self.df_summary_week,
        ) = (df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week)

        def _write_gsheet(self):
            client = dr.get_client()
            gsheet = dr.get_gsheet(client, self.id_gsheet)

            sheet_all = dr.get_sheet(gsheet, ID_SHEET_TOGGL_ALL)
            sheet_all.clear()
            dr.df_to_gsheet(self.df_summary_all, sheet_all)

            sheet_daily = dr.get_sheet(gsheet, ID_SHEET_TOGGL_DAILY)
            sheet_daily.clear()
            dr.df_to_gsheet(self.df_summary_day, sheet_daily)

            sheet_weekly = dr.get_sheet(gsheet, ID_SHEET_TOGGL_WEEKLY)
            sheet_weekly.clear()
            dr.df_to_gsheet(self.df_summary_week, sheet_weekly)

            date_time = datetime.now().strftime("%H:%M %d-%m-%Y")
            print("Info exported to ghseets\n{}".format(date_time))


if __name__ == "__main__":
    
    start_date = "2024-09-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    days_no_cache = 5
    export_cache_to_json = True
    gsheet_id = ID_GSHEET_2425
    export_to_gsheet = True

    try:
        logger.info("Iniciando pruebas de TimelogObj")
        
        # Crear instancia de TimelogObj
        timelog = TimelogObj(
            start_date=start_date,
            end_date=end_date,
            use_cache=True,
            days_no_cache=days_no_cache,
            export_cache_to_json=export_cache_to_json,
            export_to_gsheet=export_to_gsheet,
            id_gsheet=gsheet_id    
        )
        
        # Obtener datos combinados
        df_combined = timelog.get_combined_data()
        
        print("\n=== DATOS COMBINADOS ===")
        print("Forma:", df_combined.shape)
        print("Columnas:", df_combined.columns.tolist())
        print("\nPrimeras 5 filas:")
        print(df_combined.head())
        
        # Obtener estadísticas
        stats = timelog.get_statistics()
        print("\n=== ESTADÍSTICAS ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Obtener resumen por fuente
        summary = timelog.get_summary_by_source()
        print("\n=== RESUMEN POR FUENTE ===")
        for source, data in summary.items():
            print(f"\n{source.upper()}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
        
        logger.info("Pruebas completadas exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante las pruebas: {str(e)}")
        raise


