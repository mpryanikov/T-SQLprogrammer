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

# ## 170 Конструкция WITH – обобщенное табличное выражение

sql = '''
--Пишем CTE с названием TestCTE 
WITH 
TestCTE (ProductId, ProductName, Price) AS 
( 
    --Запрос, который возвращает определѐнные логичные данные 
    SELECT ProductId, ProductName, Price 
    FROM TestTable 
    WHERE CategoryId = 1 
) 
--Запрос, в котором мы можем использовать CTE 
SELECT * FROM TestCTE
'''
select(sql)

# Перечисление столбцов после названия CTE (в нашем случае после TestCTE) можно и опустить:

sql = '''
--Пишем CTE с названием TestCTE 
WITH 
TestCTE AS 
( 
    --Запрос, который возвращает определѐнные логичные данные 
    SELECT ProductId, ProductName, Price 
    FROM TestTable 
    WHERE CategoryId = 1 
) 
--Запрос, в котором мы можем использовать CTE 
SELECT * FROM TestCTE
'''
select(sql)

# Несколько именованных запросов:

sql = '''
WITH 
TestCTE1 AS (
    --Представьте, что здесь запрос со своей сложной логикой 
    SELECT ProductId, CategoryId, ProductName, Price 
    FROM TestTable 
), 
TestCTE2 AS ( 
    --Здесь также сложный запрос 
    SELECT CategoryId, CategoryName 
    FROM TestTable2 
) 
--Работаем с результирующими наборами данных двух запросов 
SELECT T1.ProductName, T2.CategoryName, T1.Price 
FROM TestCTE1 T1 
LEFT JOIN TestCTE2 T2 ON T1.CategoryId = T2.CategoryId 
WHERE T1.CategoryId = 1
'''
select(sql)

# Без WITH:

sql = '''
SELECT T1.ProductName, T2.CategoryName, T1.Price 
FROM (
    SELECT ProductId, CategoryId, ProductName, Price 
    FROM TestTable
    ) T1 
LEFT JOIN (
    SELECT CategoryId, CategoryName 
    FROM TestTable2
    ) T2 ON T1.CategoryId = T2.CategoryId 
WHERE T1.CategoryId = 1
'''
select(sql)

# ---

conn.close()


