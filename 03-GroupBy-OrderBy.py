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

# ## 57 Группировка – GROUP BY

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
SELECT COUNT(*) AS [Количество строк], 
SUM(Price) AS [Сумма по столбцу Price], 
MAX(Price) AS [Максимальное значение в столбце Price], 
MIN(Price) AS [Минимальное значение в столбце Price], 
AVG(Price) AS [Среднее значение в столбце Price] 
FROM TestTable;
'''
select(sql)

sql = '''
SELECT CategoryId AS [Id категории], 
COUNT(*) AS [Количество строк], 
MAX(Price) AS [Максимальное значение в столбце Price], 
MIN(Price) AS [Минимальное значение в столбце Price], 
AVG(Price) AS [Среднее значение в столбце Price] 
FROM TestTable 
GROUP BY CategoryId;
'''
select(sql)

sql = '''
SELECT CategoryId AS [Id категории], 
COUNT(*) AS [Количество строк], 
MAX(Price) AS [Максимальное значение в столбце Price], 
MIN(Price) AS [Минимальное значение в столбце Price], 
AVG(Price) AS [Среднее значение в столбце Price] 
FROM TestTable 
WHERE CategoryId <> 1 
GROUP BY CategoryId;
'''
select(sql)

sql = '''
SELECT CategoryId AS [Id категории], 
COUNT(*) AS [Количество строк] 
FROM TestTable 
GROUP BY CategoryId 
HAVING COUNT(*) > 2;
'''
select(sql)

# ## 60 Сортировка - ORDER BY

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
SELECT ProductID, ProductName, Price 
FROM TestTable 
ORDER BY Price DESC;
'''
select(sql)

# Следующий запрос возвращает все строки, начиная со второй, т.е. первая строка будет пропущена:

sql = '''
SELECT ProductID, ProductName, Price 
FROM TestTable 
ORDER BY Price DESC 
OFFSET 1 ROWS;
'''
select(sql)

sql = '''
SELECT ProductID, ProductName, Price 
FROM TestTable 
ORDER BY Price DESC 
OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY;
'''
select(sql)


