import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

class LoadCurrentWeather:
    def __init__(
        self,
        raw_data: list[dict],
        conn_string: str,
        schema: str,
        table_name: str
    ) -> None:
        """
        Método construtor da classe LoadCurrentWeather, para fazer a carga dos dados
        da OpenWeather no banco de dados.

        Args:
            raw_data:
                Lista de dicionários contendo os dados extraídos da OpenWeather.
            conn_string: 
                String de conexão com o banco de dados.
            schema:
                Esquema do banco de dados onde os dados serão carregados.
            table_name:
                Nome da tabela onde os dados serão carregados.
        """
        self.raw_data = raw_data
        self.conn_string = conn_string
        self.schema = schema
        self.table_name = table_name
        
        self.date_format = '%Y-%m-%d %H:%M:%S'

    def load_data(self) -> None:
        """
        Método para carregar os dados extraídos no banco de dados.
        """
        df = self._extract_data()
        logger.info(f'Extracted data: {len(df)} records.')
        if df.empty:
            logger.warning('No data to load.')
            return
        df = self._add_extract_date(df)

        engine = sa.create_engine(self.conn_string)
        try:
            logger.info('Starting data load.')
            with engine.begin() as connection:
                self._create_new_columns(df, connection)
                df.to_sql(self.table_name, con=connection, if_exists='append', index=False, schema=self.schema)
            logger.info(f'{len(df)} records successfully loaded.')
        except Exception as exc:
            logger.error('Error loading data: %s', exc, exc_info=True)
        finally:
            try:
                engine.dispose()
                logger.info('Database engine disposed successfully.')
            except Exception as dispose_e:
                logger.warning(f'Failed to dispose connection pool: {dispose_e}')

    def _extract_data(self) -> pd.DataFrame:
        """
        Método para extrair os dados da lista de dicionários e normalizar em um DataFrame.

        Returns:
            df:
                DataFrame contendo os dados normalizados.
        """
        df = pd.json_normalize(self.raw_data, sep='_').astype(str, errors='ignore')
        return df

    def _add_extract_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Método para adicionar a data de extração dos dados.

        Args:
            df:
                DataFrame contendo os dados extraídos.
        Returns:
            df:
                DataFrame com a coluna 'extract_date' adicionada.
        """
        df['extract_date'] = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime(self.date_format)
        return df
    
    def _create_new_columns(self, df: pd.DataFrame, connection) -> None:
        """
        Método para adicionar novas colunas a tabela no banco de dados, caso não existam.

        Args:
            df:
                DataFrame contendo os dados extraídos.
            connection:
                Conexão com o banco de dados.
        """

        try:
            inspector = inspect(connection)
            if inspector.has_table(self.table_name, schema=self.schema):
                existing_columns = {col['name'] for col in inspector.get_columns(self.table_name, schema=self.schema)}
                missing_columns = set(df.columns) - existing_columns

                if not missing_columns:
                    logger.info('No new columns to add.')
                    return None
                
                logger.info(f'Adding {len(missing_columns)} new columns.')
                with connection.begin():
                    for column in missing_columns:
                        connection.execute(
                            sa.text(f'ALTER TABLE {self.schema}.{self.table_name} ADD COLUMN {column} TEXT')
                        )
                logger.info(f'Added new columns: {", ".join(missing_columns)} to table {self.table_name}.')
        except Exception as e:
            logger.error(f'Error creating new columns: {e}')
            raise