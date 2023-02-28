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

# ## 181 Аналитический оператор ROLLUP

# Ооператор, который формирует промежуточные итоги для каждого указанного элемента и общий итог.

sql = '''
--Без использования ROLLUP 
SELECT CategoryId,
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY CategoryId
'''
select(sql)

sql = '''
--С использованием ROLLUP 
SELECT CategoryId,
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY ROLLUP (CategoryId)
'''
select(sql)

# ## 182 Аналитический оператор CUBE

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
--С использованием ROLLUP 
SELECT ProductName, CategoryId, 
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY ROLLUP (CategoryId, ProductName)
'''
select(sql)

# Оператор, который формирует результаты для всех возможных перекрестных вычислений. Отличие от ROLLUP состоит в том, что, если мы укажем несколько столбцов для группировки, ROLLUP выведет строки подытогов высокого уровня, т.е. для каждого уникального сочетания перечисленных столбцов, CUBE выведет подытоги для всех возможных сочетаний этих столбцов.

sql = '''
--С использованием CUBE 
SELECT ProductName, CategoryId, 
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY CUBE (CategoryId, ProductName)
'''
select(sql)

# ## 183 Аналитический оператор GROUPING SETS

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
--С использованием UNION ALL 
SELECT ProductName, NULL AS CategoryId, 
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY ProductName 
UNION ALL 
SELECT NULL AS ProductName, CategoryId, 
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY CategoryId
'''
select(sql)

# Оператор, который формирует результаты нескольких группировок в один набор данных, другими словами, в результирующий набор попадают только строки по группировкам. Данный оператор эквивалентен конструкции UNION ALL, если в нем указать запросы просто с GROUP BY по каждому указанному столбцу.

sql = '''
SELECT ProductName, CategoryId, 
    SUM(Price) AS Summa 
FROM TestTable 
GROUP BY GROUPING SETS (CategoryId, ProductName)
'''
select(sql)

# ---

conn.close()


