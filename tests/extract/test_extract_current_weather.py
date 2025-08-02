from unittest.mock import patch, MagicMock

import pytest
import requests

from src.extract.extract_current_weather import ExtractDataCurrentWeather

@pytest.fixture
def instance() -> ExtractDataCurrentWeather:
    """
    Cria e retorna uma instância de ExtractDataCurrentWeather para uso nos testes.

    Returns:
        ExtractDataCurrentWeather: 
            Instância configurada para testes.
    """
    return ExtractDataCurrentWeather(
        lat_long_list=[[-23.5505, -46.6333]],
        api_key="dummy_api_key"
    )

def mock_response(
    json_data: dict = None,
    status_code: int = 200,
    raise_exception: Exception = None
) -> MagicMock:
    """
    Retorna um objeto MagicMock simulando a resposta de requests.get (Requisição HTTP).

    Args:
        json_data: 
            Dados JSON que serão retornados pelo método .json(). Padrão é None.
        status_code: 
            Código de status HTTP da resposta. Padrão é 200.
        raise_exception:
            Exceção a ser levantada ao chamar .raise_for_status(). Padrão é None.

    Returns:
        MagicMock: 
            Mock configurado para simular uma resposta HTTP.
    """
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    if raise_exception:
        mock_resp.raise_for_status.side_effect = raise_exception
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp

@patch('src.extract.extract_current_weather.requests.get')
def test_extract_data_response(
    mock_get: MagicMock, instance: ExtractDataCurrentWeather
) -> None:
    """
    Testa se o método extract_data retorna os dados corretos.

    Args:
        mock_get: 
            Mock do método requests.get.
        instance: 
            Instância de ExtractDataCurrentWeather para o teste.
    """
    # Given
    mock_get.return_value = mock_response(
        {
            "id": "1",
            "name": "São Paulo",
            "main_temp": "25.0",
            "wheater": "[{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}]"
        }
    )

    # When
    result = instance.extract_data()

    # Then
    assert isinstance(result, list)
    assert "id" in result[0]
    assert "name" in result[0]
    assert "main_temp" in result[0]
    assert "wheater" in result[0]
    mock_get.assert_called_once_with(
        url="https://api.openweathermap.org/data/2.5/weather",
        params={
            "lat": -23.5505,
            "lon": -46.6333,
            "appid": "dummy_api_key",
            "mode": "json",
            "units": "metric",
            "lang": "pt_br"   
        }
    )

@patch('src.extract.extract_current_weather.requests.get')
def test_extract_data_client_error(
    mock_get: MagicMock, instance: ExtractDataCurrentWeather
) -> None:
    """
    Testa se o método extract_data retorna uma lista vazia 
    em caso de erro no cliente.

     Args:
        mock_get: 
            Mock do método requests.get.
        instance: 
            Instância de ExtractDataCurrentWeather para o teste.
    """
    # Given
    mock_get.return_value = mock_response(
        status_code=400,
        raise_exception=requests.exceptions.HTTPError("400 Client Error")
    )

    # When
    result = instance.extract_data()

    # Then
    assert result == []
    mock_get.assert_called_once_with(
        url="https://api.openweathermap.org/data/2.5/weather",
        params={
            "lat": -23.5505,
            "lon": -46.6333,
            "appid": "dummy_api_key",
            "mode": "json",
            "units": "metric",
            "lang": "pt_br"
        }
    )

@patch('src.extract.extract_current_weather.requests.get')
def test_extract_data_server_error(
    mock_get: MagicMock, instance: ExtractDataCurrentWeather
) -> None:
    """
    Testa se o método extract_data retorna uma lista vazia 
    em caso de erro no servidor.

     Args:
        mock_get: 
            Mock do método requests.get.
        instance: 
            Instância de ExtractDataCurrentWeather para o teste.
    """
    # Given
    mock_get.return_value = mock_response(
        status_code=500,
        raise_exception=requests.exceptions.HTTPError("500 Server Error")
    )

    # When
    result = instance.extract_data()

    # Then
    assert result == []
    mock_get.assert_called_once_with(
        url="https://api.openweathermap.org/data/2.5/weather",
        params={
            "lat": -23.5505,
            "lon": -46.6333,
            "appid": "dummy_api_key",
            "mode": "json",
            "units": "metric",
            "lang": "pt_br"
        }
    )

@patch('src.extract.extract_current_weather.requests.get')
def test_extract_data_missing_fields(
    mock_get: MagicMock, instance: ExtractDataCurrentWeather
) -> None:
    """
    Testa se o método extract_data retorna uma lista de dicionário com 
    chaves inesperadas sem quebrar.

     Args:
        mock_get: 
            Mock do método requests.get.
        instance: 
            Instância de ExtractDataCurrentWeather para o teste.
    """
    # Given
    mock_get.return_value = mock_response(
        {
            "random_key": "random_value"
        }
    )

    # When
    result = instance.extract_data()

    # Then
    assert isinstance(result, list)
    assert "id" not in result[0]
    assert "name" not in result[0]
    assert "main_temp" not in result[0]
    assert "wheater" not in result[0]
    mock_get.assert_called_once_with(
        url="https://api.openweathermap.org/data/2.5/weather",
        params={
            "lat": -23.5505,
            "lon": -46.6333,
            "appid": "dummy_api_key",
            "mode": "json",
            "units": "metric",
            "lang": "pt_br"
        }
    )

@patch('src.extract.extract_current_weather.requests.get')
def test_extract_data_connection_error_excepetion(
    mock_get: MagicMock, instance: ExtractDataCurrentWeather
) -> None:
    """
    Testa se uma exceção de conexão (ConnectionError) é tratada 
    corretamente e retorna uma lista vazia.

     Args:
        mock_get: 
            Mock do método requests.get.
        instance: 
            Instância de ExtractDataCurrentWeather para o teste.
    """
    # Given
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection Error")

    # When
    result = instance.extract_data()

    # Then
    assert result == []
    mock_get.assert_called_once_with(
        url="https://api.openweathermap.org/data/2.5/weather",
        params={
            "lat": -23.5505,
            "lon": -46.6333,
            "appid": "dummy_api_key",
            "mode": "json",
            "units": "metric",
            "lang": "pt_br"
        }
    )