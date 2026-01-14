# nomnivore

Database readers that return Polars DataFrame generators.

## Usage
```python
from nomnivore import read_pyodbc_mssql

for df in read_pyodbc_mssql("SELECT * FROM table", "MY_DB_DSN"):
    print(df)

# Custom driver
for df in read_pyodbc_mssql(
    "SELECT * FROM table",
    "MY_DB_DSN",
    driver="{ODBC Driver 17 for SQL Server}",
    trust_server_certificate="no"
):
    print(df)
```
