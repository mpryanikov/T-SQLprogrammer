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

# ## 174 Агрегатные оконные функции

sql = '''
SELECT ProductId, ProductName, CategoryId, Price, 
    SUM(Price) OVER (PARTITION BY CategoryId) AS [SUM], 
    AVG(Price) OVER (PARTITION BY CategoryId) AS [AVG], 
    COUNT(Price) OVER (PARTITION BY CategoryId) AS [COUNT], 
    MIN(Price) OVER (PARTITION BY CategoryId) AS [MIN], 
    MAX(Price) OVER (PARTITION BY CategoryId) AS [MAX] 
FROM TestTable
'''
select(sql)

# ## 175 Ранжирующие оконные функции

# **ROW_NUMBER** – функция нумерации строк в секции результирующего набора данных, которая возвращает просто номер строки.

sql = '''
SELECT 
    ROW_NUMBER () OVER (PARTITION BY CategoryId ORDER BY ProductID) AS [ROW_NUMBER]
    ,* 
FROM TestTable
'''
select(sql)

# **RANK** – ранжирующая функция, которая возвращает ранг каждой строки. В данном случае, в отличие от ROW_NUMBER (), здесь уже идет анализ значений. В случае если в столбце, по которому происходит сортировка, есть одинаковые значения, для них возвращается также одинаковый ранг (*следующее значение ранга в этом случае пропускается*).

sql = '''
SELECT 
    RANK () OVER (PARTITION BY CategoryId ORDER BY Price) AS [RANK]
    ,* 
FROM TestTable
'''
select(sql)

# **DENSE_RANK** - ранжирующая функция, которая возвращает ранг каждой строки, но в отличие от RANK в случае нахождения одинаковых значений возвращает ранг без пропуска следующего.

sql = '''
SELECT 
    DENSE_RANK () OVER (PARTITION BY CategoryId ORDER BY Price) AS [DENSE_RANK]
    ,* 
FROM TestTable
'''
select(sql)

# **NTILE** – ранжирующая оконная функция, которая делит результирующий набор на группы по определенному столбцу. Количество групп передается в качестве параметра. В случае если в группах получается не одинаковое количество строк, то в самой первой группе будет наибольшее количество, например, в случае если в источнике 10 строк, при этом мы поделим результирующий набор на три группы, то в первой будет 4 строки, а во второй и третьей по 3.

sql = '''
SELECT 
    NTILE (3) OVER (ORDER BY ProductId) AS [NTILE]
    ,* 
FROM TestTable
'''
select(sql)

# ## 176 Оконные функции смещения

#  **LEAD** – функция обращается к данным из следующей строки набора данных. Ее можно использовать, например, для того чтобы сравнить текущее значение строки со следующим. Имеет три параметра: столбец, значение которого необходимо вернуть (*обязательный параметр*), количество строк для смещения (*по умолчанию 1*), значение, которое необходимо вернуть, если после смещения возвращается значение NULL;  
#  **LAG** – функция обращается к данным из предыдущей строки набора данных. В данном случае функцию можно использовать для того, чтобы сравнить текущее значение строки с предыдущим. Имеет три параметра: столбец, значение, которого необходимо вернуть (*обязательный параметр*), количество строк для смещения (*по умолчанию 1*), значение, которое необходимо вернуть, если после смещения возвращается значение NULL;  
#  **FIRST_VALUE** - функция возвращает первое значение из набора данных, в качестве параметра принимает столбец, значение которого необходимо вернуть;  
#  **LAST_VALUE** - функция возвращает последнее значение из набора данных, в качестве параметра принимает столбец, значение которого необходимо вернуть.

sql = '''
SELECT *
FROM TestTable 
ORDER BY ProductId
'''
select(sql)

sql = '''
SELECT ProductId, ProductName, CategoryId, Price, 
    LEAD(ProductId) OVER (PARTITION BY CategoryId ORDER BY ProductId) AS [LEAD], 
    LEAD(ProductId, 2, 0) OVER (PARTITION BY CategoryId ORDER BY ProductId) AS [LEAD2], 
    LAG(ProductId) OVER (PARTITION BY CategoryId ORDER BY ProductId) AS [LAG], 
    LAG(ProductId, 2, 0) OVER (PARTITION BY CategoryId ORDER BY ProductId) AS [LAG2], 
    FIRST_VALUE(ProductId) OVER (
                                PARTITION BY CategoryId 
                                ORDER BY ProductId 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
                                ) AS [FIRST_VALUE], 
    LAST_VALUE (ProductId) OVER (
                                PARTITION BY CategoryId 
                                ORDER BY ProductId 
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING 
                                ) AS [LAST_VALUE], 
    LEAD(ProductId, 2) OVER (ORDER BY ProductId) AS [LEAD_2], 
    LAG(ProductId, 2, 0) OVER (ORDER BY ProductId) AS [LAG_2] 
FROM TestTable 
ORDER BY ProductId
'''
select(sql)

# ## 178 Аналитические оконные функции

#  **CUME_DIST** - вычисляет и возвращает интегральное распределение значений в наборе данных. Иными словами, она определяет относительное положение значения в наборе;  
#  **PERCENT_RANK** - вычисляет и возвращает относительный ранг строки в наборе данных;  
#  **PERCENTILE_CONT** - вычисляет процентиль на основе постоянного распределения значения столбца. В качестве параметра принимает процентиль, который необходимо вычислить;  
#  **PERCENTILE_DISC** - вычисляет определенный процентиль для отсортированных значений в наборе данных. В качестве параметра принимает процентиль, который необходимо вычислить.

sql = '''
SELECT ProductId, ProductName, CategoryId, Price, 
    CUME_DIST() OVER (PARTITION BY CategoryId ORDER BY Price) AS [CUME_DIST], 
    PERCENT_RANK() OVER (PARTITION BY CategoryId ORDER BY Price) AS [PERCENT_RANK], 
    PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY ProductId) 
        OVER(PARTITION BY CategoryId) AS [PERCENTILE_DISC], 
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY ProductId) 
        OVER(PARTITION BY CategoryId) AS [PERCENTILE_CONT] 
FROM TestTable
'''
select(sql)

sql = '''
SELECT *
FROM TestTable 
ORDER BY Price
'''
select(sql)

sql = '''
SELECT *
FROM TestTable 
ORDER BY ProductId
'''
select(sql)

# ---

conn.close()


