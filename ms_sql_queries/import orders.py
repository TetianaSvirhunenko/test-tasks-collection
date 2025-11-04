import pyodbc
import pandas as pd

connection = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-2DQ736S\\SQLEXPRESS;"
    "Database=ShopDB;"
    "Trusted_Connection=yes;"
)

sql_query = "SELECT * FROM dbo.orders;"
df = pd.read_sql(sql_query, connection)

df.to_excel("orders_export.xlsx", index=False, engine="openpyxl")