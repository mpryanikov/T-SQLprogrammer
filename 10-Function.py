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


# ## 129 Пользовательские функции

# + active=""
# CREATE FUNCTION TestFunction (
#     @ProductId INT --Объявление входящих параметров 
#     ) 
# RETURNS VARCHAR(100) --Тип возвращаемого результата
# AS
# BEGIN
#     --Объявление переменных внутри функции 
#     DECLARE @ProductName VARCHAR(100);
#     --Получение наименования товара по его идентификатору
#     SELECT @ProductName = ProductName 
#     FROM TestTable
#     WHERE ProductId = @ProductId 
#
#     --Возвращение результата
#     RETURN @ProductName 
# END
# -

# Вызов функции. Получение наименования конкретного товара:

sql = '''
SELECT dbo.TestFunction(1) AS [Наименование товара]
'''
select(sql)

# Вызов функции. Передача в функцию параметра в виде столбца:

sql = '''
SELECT ProductId,
    ProductName, 
    dbo.TestFunction(ProductId) AS [Наименование товара]
FROM TestTable
'''
select(sql)

# ### Пример обращения к табличной функции.

# + active=""
# CREATE FUNCTION FT_TestFunction (
#     @CategoryId INT --Объявление входящих параметров
# )
# RETURNS TABLE
# AS
# RETURN(
#     --Получение всех товаров в определѐнной категории
#     SELECT ProductId,
#         ProductName,
#         Price, 
#         CategoryId
#     FROM TestTable
#     WHERE CategoryId = @CategoryId
#     )
# -

sql = '''
SELECT * FROM FT_TestFunction(2)
'''
select(sql)

# + active=""
# CREATE FUNCTION FT_TestFunction2 (
#     --Объявление входящих параметров
#     @CategoryId INT,
#     @Price MONEY 
#     ) 
# --Определяем результирующую таблицу 
# RETURNS @TMPTable TABLE (
#                         ProductId INT, 
#                         ProductName VARCHAR(100),
#                         Price MONEY, 
#                         CategoryId INT
#                         )
# AS 
# BEGIN 
#     --Если указана отрицательная цена, то задаем цену равной 0
#     IF @Price < 0
#         SET @Price = 0
#     --Заполняем данными результирующую таблицу
#     INSERT INTO @TMPTable
#         SELECT ProductId, 
#             ProductName,
#             Price,
#             CategoryId
#         FROM TestTable
#         WHERE CategoryId = @CategoryId 
#             AND Price <= @Price
#     --Возвращаем результат и прекращаем выполнение функции 
#     RETURN
# END
# -

sql = '''
SELECT * FROM FT_TestFunction2(2, 300)
'''
select(sql)

# ### Пример использования функции.

# + active=""
# ALTER FUNCTION TestFunction (
#     @ProductId INT --Объявление входящих параметров 
#     ) 
# RETURNS VARCHAR(100) --Тип возвращаемого результата
# AS
# BEGIN
#     --Объявление переменных внутри функции 
#     DECLARE @CategoryName VARCHAR(100);
#     --Получение наименования категории товара по идентификатору товара
#     SELECT @CategoryName = T2.CategoryName
#     FROM TestTable T1 
#     INNER JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId
#     WHERE T1.ProductId = @ProductId
#
#     --Возвращение результата
#     RETURN @CategoryName 
# END
# -

sql = '''
SELECT ProductId,
    ProductName,
    dbo.TestFunction(ProductId) AS [CategoryName]
FROM TestTable
'''
select(sql)

# ## 135 Строковые функции

sql = '''
DECLARE @TestVar VARCHAR(100),
    @TestVar2 VARCHAR(100)

SELECT @TestVar = 'ТеКст', @TestVar2 = 'ТЕкст'
--Без использования функции
SELECT @TestVar AS TestVar,	@TestVar2 AS TestVar2
'''
select(sql)

sql = '''
DECLARE @TestVar VARCHAR(100),
    @TestVar2 VARCHAR(100)

SELECT @TestVar = 'ТеКст', @TestVar2 = 'ТЕкст'
--С использованием функций
SELECT UPPER(@TestVar) AS TestVar, LOWER(@TestVar2) AS TestVar2
'''
select(sql)

sql = '''
DECLARE @TestVar VARCHAR(100),
    @TestVar2 VARCHAR(100)

SELECT @TestVar = '1234567890', @TestVar2 = '1234567890'
SELECT LEFT(@TestVar, 5) AS TestVar, RIGHT(@TestVar2, 5) AS TestVar2
'''
select(sql)

sql = '''
DECLARE @TestVar VARCHAR(100),
    @TestVar2 VARCHAR(100)

SELECT @TestVar = '1234567890', @TestVar2 = '1234567890'
SELECT SUBSTRING(@TestVar, 3, 5) AS TestVar
'''
select(sql)

# ## 138 Функции для работы с датой и временем

sql = '''
DECLARE @TestDate DATETIME 

SET @TestDate = GETDATE() 

SELECT GETDATE() AS [Текущая дата],
    DATENAME(M, @TestDate) AS [[Название месяца],
    DATEPART(M, @TestDate) AS [[Номер месяца],
    DAY(@TestDate) AS [День],
    MONTH(@TestDate) AS [Месяц],
    YEAR(@TestDate) AS [Год], 
    DATEDIFF(D, '01.01.2018', @TestDate) AS [Количество дней],
    DATEADD(D, 5, GETDATE()) AS [+ 5 Дней]
'''
select(sql)

# ## 139 Математические функции

sql = '''
SELECT ABS(-100) AS [ABS],
    ROUND(1.567, 2) AS [ROUND], 
    CEILING(1.6) AS [CEILING], 
    FLOOR(1.6) AS [FLOOR],
    SQRT(16) AS [SQRT], 
    SQUARE(4) AS [SQUARE], 
    POWER(4, 2) AS [POWER], 
    LOG(10) AS [LOG]
'''
select(sql)

# ## 140 Функции метаданных

sql = '''
SELECT DB_ID() AS [Идентификатор текущей БД], 
    DB_NAME() AS [Имя текущей БД], 
    OBJECT_ID ('TestTable') AS [Идентификатор таблицы TestTable], 
    OBJECT_NAME(149575571) AS [Имя объекта с ИД 149575571]
'''
select(sql)

# ## 141 Прочие функции

sql = '''
SELECT 
    ISNULL(NULL, 5) AS [ISNULL],
    COALESCE (NULL, NULL, 5) AS [COALESCE],
    CAST(1.5 AS INT) AS [CAST],
    HOST_NAME() AS [HOST_NAME], 
    SUSER_SNAME() AS [SUSER_SNAME],
    USER_NAME() AS [USER_NAME]
'''
select(sql)

# ---

conn.close()


