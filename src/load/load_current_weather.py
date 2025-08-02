import logging
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

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
                String de conexão com o banco de dados.s
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
        df['extract_date'] = datetime.now().strftime(self.date_format)
        return df