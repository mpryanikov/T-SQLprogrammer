# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pandas as pd
import numpy as np

# ## Подключение к бд

import sqlalchemy
# sqlalchemy.__version__

# # !pip install pyodbc
import pyodbc
import warnings
warnings.filterwarnings('ignore')

conn = pyodbc.connect('DSN=TestDB;Trusted_Connection=yes;')


def select(sql):
  return pd.read_sql(sql,conn)


cur = conn.cursor()
sql = '''
truncate table TestTable;
INSERT INTO TestTable VALUES 
    (1,'Клавиатура', 100), 
    (1, 'Мышь', 50), 
    (2, 'Телефон', 300);
    
truncate table TestTable2;    
INSERT INTO TestTable2 VALUES 
    ('Комплектующие компьютера'), 
    ('Мобильные устройства')     
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

# + active=""
# sp_configure 'show advanced options', 1; 
# RECONFIGURE;

# + active=""
# sp_configure 'Ad Hoc Distributed Queries', 1; 
# RECONFIGURE;
# -

# ## 186 Получение данных из внешних источников

# https://www.stackfinder.ru/questions/36987636/cannot-create-an-instance-of-ole-db-provider-microsoft-jet-oledb-4-0-for-linked

# + active=""
# --С помощью OPENDATASOURCE 
# SELECT * FROM OPENDATASOURCE('Microsoft.Jet.OLEDB.4.0', 
#     'Data Source=D:\TestExcel.xls; Extended Properties=Excel 8.0')...[Лист1$];

# + active=""
# --С помощью OPENROWSET 
# SELECT * FROM OPENROWSET('Microsoft.Jet.OLEDB.4.0', 
#     'Excel 8.0; Database=D:\TestExcel.xls', [Лист1$]);

# + active=""
# --С помощью OPENROWSET (с запросом) 
# SELECT * FROM OPENROWSET('Microsoft.Jet.OLEDB.4.0', 
#     'Excel 8.0; Database=D:\TestExcel.xls', 'SELECT ProductName, Price FROM [Лист1$]');
# -



cur = conn.cursor()
sql = '''
SELECT * FROM OPENDATASOURCE('Microsoft.Jet.OLEDB.4.0', 
    'Data Source=D:\TestExcel.xls; Extended Properties=Excel 8.0')...[Лист1$];    
'''
cur.execute(sql)
conn.commit()
cur.close()

# https://learn.microsoft.com/ru-ru/sql/t-sql/functions/opendatasource-transact-sql?view=sql-server-ver16



# ---

# https://info-comp.ru/import-excel-in-ms-sql-server

sql = """
SELECT @@VERSION;
"""
select(sql)

# #### Шаг 1 – Проверяем наличие провайдера Microsoft.ACE.OLEDB.12.0 на SQL Server

# + active=""
# EXEC sp_enum_oledb_providers;
# -

# #### Шаг 2 – Установка провайдера Microsoft.ACE.OLEDB.12.0 (32-bit)

# https://www.microsoft.com/en-us/download/details.aspx?id=13255

# Выберите и скачайте файл, соответствующий архитектуре x86 (т.е. в названии без x64).



# + active=""
# icacls C:\Windows\ServiceProfiles\LocalService\AppData\Local\Temp /grant UserName:(R,W)
# -













# ---

conn.close()


