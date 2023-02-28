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


# ## 143 Пользовательские процедуры

# + active=""
# --Создаем процедуру 
# CREATE PROCEDURE TestProcedure (
#     @CategoryId INT,
#     @ProductName VARCHAR(100)
# )
# AS 
# BEGIN
#     --Объявляем переменную 
#     DECLARE @AVG_Price MONEY
#
#     --Определяем среднюю цену в категории 
#     SELECT @AVG_Price = ROUND(AVG(Price), 2) 
#     FROM TestTable
#     WHERE CategoryId = @CategoryId
#
#     --Добавляем новую запись
#     INSERT INTO TestTable(CategoryId, ProductName, Price) VALUES 
#     (@CategoryId, LTRIM(RTRIM(@ProductName)),
#     @AVG_Price)
#
#     --Возвращаем данные
#     SELECT * FROM TestTable WHERE CategoryId = @CategoryId
# END 
# -

# Вызываем процедуру:

# + active=""
# EXEC TestProcedure @CategoryId = 1, @ProductName = 'Тестовый товар'
# -

# ### Создание

# + active=""
# declare @ProcName sysname set @ProcName = 'TestProcedure'
# if not exists (select * from dbo.sysobjects where id = object_id(@ProcName))
#   exec ('create procedure ' + @ProcName + ' as return')
# GO
#
# ALTER PROCEDURE TestProcedure (
#     @CategoryId INT,
#     @ProductName VARCHAR(100)
# )
# AS 
# BEGIN
#     --Объявляем переменную 
#     DECLARE @AVG_Price MONEY
#
#     --Определяем среднюю цену в категории 
#     SELECT @AVG_Price = ROUND(AVG(Price), 2) 
#     FROM TestTable
#     WHERE CategoryId = @CategoryId
#
#     --Добавляем новую запись
#     INSERT INTO TestTable(CategoryId, ProductName, Price) VALUES 
#     (@CategoryId, LTRIM(RTRIM(@ProductName)),
#     @AVG_Price)
#
#     --Возвращаем данные
#     SELECT * FROM TestTable WHERE CategoryId = @CategoryId
# END 
#
# GO

# + active=""
# --Вызываем процедуру
# EXEC TestProcedure @CategoryId = 1, @ProductName = 'Тестовый товар'
# -

# ### Изменение

# + active=""
# declare @ProcName sysname set @ProcName = 'TestProcedure'
# if not exists (select * from dbo.sysobjects where id = object_id(@ProcName))
#   exec ('create procedure ' + @ProcName + ' as return')
# GO
#
# ALTER PROCEDURE TestProcedure (
#     @CategoryId INT,
#     @ProductName VARCHAR(100),
#     @Price MONEY = NULL -- Необязательный параметр
# )
# AS 
# BEGIN
#     --Если цену не передали, то определяем среднюю цену 
#     IF @Price IS NULL 
#         SELECT @Price = ROUND(AVG(Price), 2)
#         FROM TestTable
#         WHERE CategoryId = @CategoryId
#
#     --Добавляем новую запись
#     INSERT INTO TestTable(CategoryId, ProductName, Price) VALUES 
#     (@CategoryId, LTRIM(RTRIM(@ProductName)), @Price)
#
#     --Возвращаем данные
#     SELECT * FROM TestTable WHERE CategoryId = @CategoryId
# END 
#
# GO

# + active=""
# --Вызываем процедуру
# EXEC TestProcedure @CategoryId = 1, @ProductName = 'Тестовый товар', @Price = 100
# -

# ## 146 Системные хранимые процедуры

# + active=""
# EXEC sp_helpdb 'TestDB'
# EXEC sp_tables @table_type = "'TABLE'"

# + active=""
# EXEC sp_rename TestTable_OldName, TestTable_NewName
# -

# ---

conn.close()


