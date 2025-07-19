# Importando bibliotecas/módulos
import pandas as pd

import os

class GetLatLong:
    def __init__(self, file_path: str) -> None:
        """
        Método construtor da classe GetLatLong, para extrair a latitude e longitude de um arquivo CSV.

        Args:
            file_path: Caminho do arquivo csv.
        """
        self.file_path = file_path

    def get_lat_long(self, cities: list[str]=None) -> list:
        """
        Método construtor da classe GetLatLong, para extrair a latitude e longitude de um arquivo CSV.

        Args:
            cities: 
                Lista contendo o nome dos municipios que se deseja obter a latitude e longitude.
                Se None, retorna a latitude e longitude de todos os municipios do arquivo.

        Returns:
            lat_long_list: Lista contendo a latitude e longitude das cidades do parâmetro.
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"The file {self.file_path} does not exist.")
        
        lat_long_list = []
        try:
            df = pd.read_csv(self.file_path)
            if 'latitude' not in df.columns or 'longitude' not in df.columns:
                raise ValueError("The CSV file must contain 'latitude' and 'longitude' columns.")
            if cities is None:
                lat_long_list.append(df[['latitude', 'longitude']].values.tolist())
            else:
                for city in cities:
                    lat_long = df[df['nome'] == city][['latitude', 'longitude']].values.tolist()
                    if lat_long:
                        lat_long_list.append(lat_long[0])
                    else:
                        print(f"City '{city}' not found in the file.")
        except pd.errors.EmptyDataError:
            print(f"The file {self.file_path} is empty.")
            return lat_long_list
        except pd.errors.ParserError:
            print(f"The file {self.file_path} could not be parsed. Please check the file format.")
            return lat_long_list
        except Exception as e:
            print(f"An error occurred while processing the file: {e}")
            return lat_long_list
        
        return lat_long_list