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

# ## 185 Оператор APPLY

sql = '''
SELECT * FROM FT_TestFunction(1)
'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

# Существует два типа оператора APPLY:  
#  **CROSS APPLY** - возвращает только строки из внешней таблицы, которые создает табличная функция;  
#  **OUTER APPLY** - возвращает и строки, которые формирует табличная функция, и строки со значениями NULL в столбцах, созданные табличной функцией. Например, табличная функция может не возвращать никаких данных для определенных значений, CROSS APPLY в таких случаях подобные строки не выводит, а OUTER APPLY выводит (OUTER APPLY лично мне требуется редко).

# В данном случае функция FT_TestFunction была вызвана для каждой строки таблицы TestTable2.

sql = '''
SELECT T2.CategoryName, FT1.* 
FROM TestTable2 T2 
CROSS APPLY FT_TestFunction(T2.CategoryId) AS FT1
'''
select(sql)

cur = conn.cursor()
sql = '''
--Добавление новой строки в таблицу TestTable2 
INSERT INTO TestTable2 VALUES ('Новая категория');
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable2'''
select(sql)

sql = '''
SELECT T2.CategoryName, FT1.* 
FROM TestTable2 T2 
OUTER APPLY FT_TestFunction(T2.CategoryId) AS FT1
'''
select(sql)

# ---

conn.close()


