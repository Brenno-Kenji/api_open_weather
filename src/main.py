import os
import logging

from dotenv import find_dotenv, load_dotenv

from extract import ExtractDataCurrentWeather
from load import LoadCurrentWeather
from tools import LatLong

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

dotenv_file = find_dotenv()
if dotenv_file:
    load_dotenv(dotenv_file)
else:
    logger.warning("No .env file found.")

class WeatherApp:
    def __init__(self) -> None:
        """
        Método construtor da classe WeatherApp, para realizar o processo de extração e carga dos dados
        da API da OpenWeather.
        """
        self.api_key = os.getenv("API_KEY")

        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_user = os.getenv('DB_USER_NAME')
        self.db_password = os.getenv('DB_USER_PASSWORD')
        self.db_name = os.getenv('DB_NAME')
        self.db_schema = os.getenv('DB_SCHEMA')
        
        self.file_path = "data/raw_data/municipios.csv"
        self.cities = [
            "Campinas", "Xaxim"
        ]

        self.conn_string = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'
        self.table = 'raw_current_weather'

    def start(self) -> None:
        """
        Método principal para iniciar o processo de extração e carga dos dados.
        """
        if not all(
            [
                self.db_host, 
                self.db_port, 
                self.db_user, 
                self.db_password, 
                self.db_name, 
                self.db_schema, 
                self.api_key
            ]
        ):
            logger.error('Environment variables are not set.')
            return

        logger.info('Starting weather update...')
        try:
            try:
                self._extract_and_load()
                logger.info('Weather update completed successfully.')
            except Exception as inner_e:
                logger.exception(f'Error during weather update (inner): {inner_e}')
                raise
        except Exception as e:
            logger.exception(f'Error during weather update: {e}')
    
    def _extract_and_load(self) -> None:
        """
        Método para extrair e carregar os dados da OpenWeather.
        """
        lat_long_list = LatLong(self.file_path).get_lat_long(self.cities)
        try:
            extractor = ExtractDataCurrentWeather(
                lat_long_list=lat_long_list,
                api_key=self.api_key
            )
            raw_data = extractor.extract_data()
            loader = LoadCurrentWeather(raw_data, self.conn_string, self.db_schema, self.table)
            loader.load_data()
            logger.info('Data extraction and loading completed.')
        except Exception as e:
            logger.exception(f"Error during extract and load: {e}")

if __name__ == "__main__":
    app = WeatherApp()
    app.start()