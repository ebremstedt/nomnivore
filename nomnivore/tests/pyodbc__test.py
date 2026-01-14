import polars as pl
import pytest
from unittest.mock import Mock, patch, MagicMock
from nomnivore.pyodbc_ import read_pyodbc_mssql


@pytest.fixture
def mock_dsn() -> Mock:
    dsn = Mock()
    dsn.hostname = "testhost"
    dsn.port = "1433"
    dsn.database = "testdb"
    dsn.username = "testuser"
    dsn.password = "testpass"
    return dsn


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
@patch("nomnivore.pyodbc_.pl.read_database")
def test_read_pyodbc_mssql_returns_dataframe(
    mock_read_db: Mock,
    mock_connect: Mock,
    mock_env_var: Mock,
    mock_dsn: Mock,
    sample_df: pl.DataFrame,
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_read_db.return_value = sample_df

    result = list(read_pyodbc_mssql("SELECT * FROM test", "TEST_DSN"))

    assert len(result) == 1
    assert result[0].equals(sample_df)
    mock_connect.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
@patch("nomnivore.pyodbc_.pl.read_database")
def test_read_pyodbc_mssql_empty_dataframe(
    mock_read_db: Mock, mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_read_db.return_value = pl.DataFrame()

    result = list(read_pyodbc_mssql("SELECT * FROM empty", "TEST_DSN"))

    assert len(result) == 0
    mock_conn.close.assert_called_once()


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
def test_read_pyodbc_mssql_default_connection_string(
    mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    list(read_pyodbc_mssql("SELECT 1", "TEST_DSN"))

    expected_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=testhost,1433;"
        "DATABASE=testdb;"
        "UID=testuser;"
        "PWD=testpass;"
        "TrustServerCertificate=yes"
    )
    mock_connect.assert_called_once_with(expected_conn_str)


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
def test_read_pyodbc_mssql_custom_driver(
    mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    list(
        read_pyodbc_mssql(
            "SELECT 1", "TEST_DSN", driver="{ODBC Driver 17 for SQL Server}"
        )
    )

    expected_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=testhost,1433;"
        "DATABASE=testdb;"
        "UID=testuser;"
        "PWD=testpass;"
        "TrustServerCertificate=yes"
    )
    mock_connect.assert_called_once_with(expected_conn_str)


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
def test_read_pyodbc_mssql_custom_trust_certificate(
    mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    list(read_pyodbc_mssql("SELECT 1", "TEST_DSN", trust_server_certificate="no"))

    expected_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=testhost,1433;"
        "DATABASE=testdb;"
        "UID=testuser;"
        "PWD=testpass;"
        "TrustServerCertificate=no"
    )
    mock_connect.assert_called_once_with(expected_conn_str)


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
@patch("nomnivore.pyodbc_.pl.read_database")
def test_read_pyodbc_mssql_closes_on_exception(
    mock_read_db: Mock, mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_read_db.side_effect = Exception("DB Error")

    with pytest.raises(Exception, match="DB Error"):
        list(read_pyodbc_mssql("SELECT * FROM test", "TEST_DSN"))

    mock_conn.close.assert_called_once()


@patch("nomnivore.pyodbc_.env_var_dsn")
@patch("nomnivore.pyodbc_.pyodbc.connect")
def test_read_pyodbc_mssql_uses_correct_env_var(
    mock_connect: Mock, mock_env_var: Mock, mock_dsn: Mock
) -> None:
    mock_env_var.return_value = mock_dsn
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    list(read_pyodbc_mssql("SELECT 1", "CUSTOM_DSN"))

    mock_env_var.assert_called_once_with(name="CUSTOM_DSN")
