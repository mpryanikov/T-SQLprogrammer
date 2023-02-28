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

# # Подключение к бд

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
delete from TestTable;
INSERT INTO TestTable
       VALUES (1,'Системный блок', 300),
              (1,'Монитор', 200),
              (1,'Клавиатура', 100),
              (1,'Мышь', 50),
              (3,'Принтер', 200),
              (3,'Сканер', 150),
              (2,'Телефон', 250),
              (2,'Планшет', 300)
'''
cur.execute(sql)
conn.commit()

sql = '''SELECT * FROM TestTable'''
select(sql)

# # 49 Оператор TOP

# ### Возвращает только 20 процентов итогового результата:

sql = '''
   SELECT TOP (20) PERCENT ProductId, ProductName, Price
   FROM TestTable
   ORDER BY Price DESC;
'''
select(sql)

# ### WITH TIES

# Допустим, Вам нужно определить 5 самых дорогих товаров.  
# Вы, соответственно, отсортируете данные по столбцу с ценой и укажете оператор TOP 5, но,  
# если товаров с одинаковой ценой (эта цена входит в число самых больших) несколько, например, 7,  
# Вам все равно вернется 5
# Tlgm: @it_boooks
# 50 Глава 4 - Выборка данных – оператор SELECT
# строк, что, как Вы понимаете, неправильно, так как самых дорогих товаров на самом деле 7.  
# Чтобы это узнать или, как говорят, определить, есть ли «хвосты», т.е. строки с таким же значением, которые не попали в выборку, за счет ограничения TOP, можно использовать параметр **WITH TIES**.

sql = '''
SELECT TOP 4 ProductID, ProductName, Price 
FROM TestTable ORDER BY Price DESC;
'''
select(sql)

sql = '''
SELECT TOP 4 WITH TIES ProductID, ProductName, Price 
FROM TestTable ORDER BY Price DESC;
'''
select(sql)

# ## Как в SQL получить первые (или последние) строки запроса? TOP или OFFSET?

sql = '''SELECT * FROM TestTable'''
select(sql)

# https://info-comp.ru/obucheniest/672-get-first-query-records-sql.html

sql = '''
SELECT TOP 5 ProductID, ProductName, Price FROM TestTable;
'''
select(sql)

sql = '''
SELECT TOP 4 ProductID, ProductName, Price 
FROM TestTable 
ORDER BY Price DESC;
'''
select(sql)

sql = '''
SELECT ProductId, ProductName, Price
FROM TestTable
ORDER BY Price DESC
OFFSET 0 ROWS FETCH NEXT 4 ROWS ONLY;
'''
select(sql)

# ### Получаем последние строки SQL запроса с помощью TOP

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
WITH 
SRC AS (
    --Получаем 5 последних строк в таблице
    SELECT TOP (5) ProductId, ProductName, Price
    FROM TestTable
    ORDER BY ProductId DESC
)
SELECT * FROM SRC
ORDER BY Price; --Применяем нужную нам сортировку
'''
select(sql)

# ### Получаем последние строки SQL запроса с помощью OFFSET-FETCH

sql = '''
--Объявляем переменную
DECLARE @CNT INT;

--Узнаем количество строк в таблице
SELECT @CNT = COUNT(*) 
FROM TestTable;

--Получаем 5 последних строк
SELECT ProductId, ProductName, Price
FROM TestTable
ORDER BY ProductId
OFFSET @CNT - 5 ROWS FETCH NEXT 5 ROWS ONLY;
'''
select(sql)


