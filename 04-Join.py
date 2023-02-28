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
truncate table TestTable;
INSERT INTO TestTable VALUES 
    (1,'Клавиатура', 100), 
    (1, 'Мышь', 50), 
    (2, 'Телефон', 300)
'''
cur.execute(sql)
conn.commit()

# ## 62 Объединение JOIN

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

sql = '''
SELECT T1.ProductName, T2.CategoryName, T1.Price 
FROM TestTable T1 
INNER JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId 
ORDER BY T1.CategoryId;
'''
select(sql)

sql = '''
SELECT T1.ProductName, T2.CategoryName, T1.Price 
FROM TestTable T1 
LEFT JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId 
ORDER BY T1.CategoryId;
'''
select(sql)

# ## 68 Объединение UNION

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
UNION 
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 3;
'''
select(sql)

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
UNION 
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1;
'''
select(sql)

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
UNION ALL
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1;
'''
select(sql)

# ## 70 Объединение INTERSECT и EXCEPT

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

# **INTERSECT** (пересечение) – данный оператор выводит одинаковые строки из первого, второго и последующих наборов данных.

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
INTERSECT
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1;
'''
select(sql)

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
UNION
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 2;
'''
select(sql)

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
INTERSECT
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 2;
'''
select(sql)

# **EXCEPT** (разность) – этот оператор выводит только те данные из первого набора строк, 
# которых нет во втором наборе. Он полезен, например, тогда, когда необходимо сравнить 
# две таблицы и вывести только те строки из первой таблицы, которых нет в другой таблице.

sql = '''
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 1 
EXCEPT
SELECT T1.ProductId, T1.ProductName, T1.Price 
FROM TestTable T1 
WHERE T1.ProductId = 2;
'''
select(sql)

# ## 74 Подзапросы (вложенные запросы)

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

sql = '''
SELECT T2.CategoryName AS [Название категории], 
(
    SELECT COUNT(*) 
    FROM TestTable 
    WHERE CategoryId = T2.CategoryId
) AS [Количество товаров] 
FROM TestTable2 T2;
'''
select(sql)

sql = '''
SELECT ProductId, Price 
FROM (
    SELECT ProductId, Price FROM TestTable
    ) AS Q1;
'''
select(sql)

sql = '''
SELECT Q1.ProductId, Q1.Price, Q2.CategoryName 
FROM (
    SELECT ProductId, Price, CategoryId 
    FROM TestTable
) AS Q1 
LEFT JOIN (
    SELECT CategoryId, CategoryName 
    FROM TestTable2
) AS Q2 ON Q1.CategoryId = Q2.CategoryId;
'''
select(sql)


