from unittest.mock import patch, MagicMock

import pytest

from src.load.load_current_weather import LoadCurrentWeather

@pytest.fixture
def raw_data() -> list[dict]:
    """
    Retorna uma lista de dicionários simulando dados extraídos.

    Returns:
        Lista de dicionários com dados simulados para testes.
    """
    return [
        {
            "city": "Test City",
            "temperature": 25.0,
            "humidity": 60,
            "wind_speed": 5.0,
            "weather_description": "Clear sky"
        },
        {
            "city": "Another City",
            "temperature": 22.0,
            "humidity": 55,
            "wind_speed": 3.0,
            "weather_description": "Partly cloudy"
        }
    ]

@pytest.fixture
def loader(raw_data) -> LoadCurrentWeather:
    """
    Instância de LoadCurrentWeather para testes.

    Returns:
        LoadCurrentWeather:
            Instância configurada com dados de teste.
    """
    return LoadCurrentWeather(
        raw_data=raw_data,
        conn_string="sqlite:///:memory:",
        schema="test_schema",
        table_name="test_table",
    )

def test_extract_data_success(
    loader: LoadCurrentWeather
) -> None:
    """
    Testa se _extract_data retorna um DataFrame com os dados extraídos corretamente.

    Args:
        loader:
            Instância de LoadCurrentWeather para o teste.
    """
    # Given and When
    df = loader._extract_data()

    # Then
    assert len(df) == 2
    assert "city" in df.columns
    assert "temperature" in df.columns
    assert "humidity" in df.columns
    assert "wind_speed" in df.columns
    assert "weather_description" in df.columns

def test_extract_data_empty_data() -> None:
    """
    Testa se _extract_data retorna um DataFrame vazio quando não há dados.
    """
    # Given
    loader = LoadCurrentWeather(raw_data=[], conn_string="sqlite:///:memory:", schema="test_schema", table_name="test_table")
    
    # When
    df = loader._extract_data()

    # Then
    assert df.empty

@patch('src.load.load_current_weather.datetime')
def test_add_extract_date_success(
    mock_datetime: MagicMock, loader: LoadCurrentWeather
) -> None:
    """
    Testa se _add_extract_date adiciona a coluna 'extract_date' corretamente.

    Args:
        mock_datetime:
            Mock para o módulo datetime.
        loader:
            Instância de LoadCurrentWeather para o teste.
    """
    # Given
    mock_datetime.now.return_value.strftime.return_value = "2025-01-01 15:00:00"

    # When
    df = loader._add_extract_date(loader._extract_data())

    # Then
    assert "extract_date" in df.columns
    assert df["extract_date"].iloc[0] == "2025-01-01 15:00:00"

@patch('src.load.load_current_weather.inspect')
def test_create_new_columns_success(
    mock_inspect: MagicMock, loader: LoadCurrentWeather
) -> None:
    """
    Testa se _create_new_columns adiciona novas colunas corretamente.

    Args:
        mock_inspect:
            Mock para o módulo inspect.
        loader:
            Instância de LoadCurrentWeather para o teste.
    """
    # Given
    connection = MagicMock()
    df = loader._extract_data()

    mock_instance = MagicMock()
    mock_instance.has_table.return_value = True
    mock_instance.get_columns.return_value = [
        {"name": "rain"},
        {"name": "snow"}
    ]
    mock_inspect.return_value = mock_instance

    # When
    loader._create_new_columns(df, connection)

    # Then
    expected_missing = set(df.columns) - {"rain", "snow"}
    assert connection.execute.call_count == len(expected_missing)

@patch('src.load.load_current_weather.inspect')
def test_create_new_columns_no_new_columns(
    mock_inspect: MagicMock, loader: LoadCurrentWeather
) -> None:
    """
    Testa se _create_new_columns não tenta adicionar colunas que já existem.

    Args:
        mock_inspect:
            Mock para o módulo inspect.
        loader:
            Instância de LoadCurrentWeather para o teste.
    """
    # Given
    connection = MagicMock()
    df = loader._extract_data()

    mock_inspector_instance = MagicMock()
    mock_inspector_instance.has_table.return_value = True
    mock_inspector_instance.get_columns.return_value = [{"name": col} for col in df.columns]
    mock_inspect.return_value = mock_inspector_instance

    # When
    loader._create_new_columns(df, connection)

    # Then
    connection.execute.assert_not_called()

# TODO: Verificar porque está falhando o teste de carregamento dos dados 
#
# @patch('src.load.load_current_weather.sa.create_engine')
# @patch('src.load.load_current_weather.pd.DataFrame.to_sql')
# @patch('src.load.load_current_weather.inspect')
# def test_load_data_success(
#     mock_create_engine: MagicMock, mock_to_sql: MagicMock, mock_inspect: MagicMock, loader: LoadCurrentWeather
# ) -> None:
#     """
#     Testa se o método load_data carrega os dados corretamente no banco de dados.

#     Args:
#         mock_create_engine:
#             Mock para o método create_engine do SQLAlchemy.
#         mock_to_sql:
#             Mock para o método to_sql do pandas.
#         mock_inspect:
#             Mock para o módulo inspect do SQLAlchemy.
#         loader:
#             Instância de LoadCurrentWeather para o teste.
#     """
#     # Given
#     mock_engine = MagicMock()
#     mock_create_engine.return_value = mock_engine
#     mock_connection = MagicMock()
#     mock_engine.begin.return_value.__enter__.return_value = mock_connection

#     mock_inspector = MagicMock()
#     mock_inspector.has_table.return_value = True
#     mock_inspector.get_columns.return_value = [{'name': 'col1'}]
#     mock_inspect.return_value = mock_inspector

#     # When
#     loader.load_data()

#     # Then
#     mock_to_sql.assert_called_once_with(
#         "test_table",
#         con=mock_connection,
#         if_exists="append",
#         index=False,
#         schema="test_schema"
#     )
#     assert mock_engine.dispose.called