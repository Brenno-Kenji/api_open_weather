import time
import logging

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

class ExtractDataCurrentWeather:
    def __init__(
        self,
        lat_long_list: list[str] = None,
        api_key: str = None,
        mode: str = 'json',
        units: str = 'metric',
        lang: str = 'pt_br'
    ) -> None:
        """
        Método construtor da classe ExtractDataCurrentWeather, para extrair os dados da OpenWeather.

        Args:
            lat_long_list:
                Lista contendo a latitude e longitude dos municípios.
            api_key: 
                Chave de API para acessar os dados do OpenWeather.
            mode:
                Formato que os dados serão recebidos. Padrão é 'json'.
                Lista de formatos disponíveis: 'xml', 'html', 'json'.
            units:
                Unidade de medida para os dados retornados. Padrão é 'metric'.
            lang:
                Idioma dos dados retornados. Padrão é 'pt_br'.
        """
        self.lat_long_list = lat_long_list
        self.api_key = api_key
        self.mode = mode
        self.units = units
        self.lang = lang

        self.url = "https://api.openweathermap.org/data/2.5"
        self.endpoint = "weather"

    def extract_data(self) -> list[dict]:
        """
        Método para extrair os dados da OpenWeather.

        Returns:
            all_weathers:
                Lista de dicionários contendo os dados da requisição.
        """
        all_weathers = []
        request_interval = 1 # 60 segundos / 60 requisições por minuto = 1 segundo por requisição
        
        if not self.lat_long_list:
            logger.warning("No latitude and longitude data found for the specified cities.")
            return None
        
        for lat_long in self.lat_long_list:
            lat, long = lat_long
            params = {
                'lat': lat,
                'lon': long,
                'appid': self.api_key,
                'mode': self.mode,
                'units': self.units,
                'lang': self.lang
            }
            try:
                response = requests.get(
                    url=f"{self.url}/{self.endpoint}",
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                logging.info(f"Requesting weather data for coordinates: {lat}, {long}")
                all_weathers.append(data)

                time.sleep(request_interval)  # Atraso entre as requisições para evitar pagamento de limite
            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Request error occurred: {req_err}")
            except Exception as e:
                logger.error(f"An error occurred while processing the request: {e}")

        return all_weathers