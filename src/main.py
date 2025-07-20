import os
import logging

from dotenv import find_dotenv, load_dotenv

from extract import ExtractDataCurrentWeather
from tools import LatLong

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Carregando as variáveis de ambiente do arquivo .env
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
        
        self.file_path = "../data/raw_data/municipios.csv"
        self.cities = [
            "Campinas", "Xaxim"
        ]

    def start(self) -> list[dict]:
        lat_long_list = LatLong(self.file_path).get_lat_long(self.cities)
        raw_data = ExtractDataCurrentWeather(
            lat_long_list=lat_long_list,
            api_key=self.api_key
        ).extract_data()
        
        return raw_data

if __name__ == "__main__":
    app = WeatherApp()
    weather_data = app.start()
    print(weather_data)