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

# ## 178 Операторы PIVOT

# Можно реализовать хранимую процедуру, с помощью которой можно формировать динамические запросы PIVOT (пример реализации можете найти на моем сайте <a href="http://info-comp.ru/obucheniest/631-dynamic-pivot-in-t-sql.html">info-comp.ru</a>).
#

# #### Обычная группировка:

sql = ''' 
SELECT T2.CategoryName, 
    AVG(T1.Price) AS AvgPrice 
FROM TestTable T1 
LEFT JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId 
GROUP BY T2.CategoryName
'''
select(sql)

# #### Группировка с использованием PIVOT :

sql = '''
SELECT T1.Price, T2.CategoryName 
FROM TestTable T1 
LEFT JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId
'''
select(sql)

sql = '''
SELECT 'Средняя цена' AS AvgPrice, [Комплектующие компьютера], [Мобильные устройства] 
FROM (
    SELECT T1.Price, T2.CategoryName 
    FROM TestTable T1 
    LEFT JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId
    ) AS SourceTable 
PIVOT (
    AVG(Price) FOR CategoryName IN ([Комплектующие компьютера],[Мобильные устройства])
) AS PivotTable;
'''
select(sql)

# Где,  
#  [Комплектующие компьютера], [Мобильные устройства] – это значения в столбце CategoryName, которые мы заранее должны знать;  
#  SourceTable – псевдоним выражения, в котором мы указываем исходный источник данных, например, вложенный запрос;  
#  PIVOT – вызов оператора PIVOT;  
#  AVG – агрегатная функция, в которую мы передаем столбец для анализа, в нашем случае Price;  
#  FOR – с помощью данного ключевого слова мы указываем столбец, содержащий значения, которые будут выступать именами столбцов, в нашем случае CategoryName;  
#  IN – оператор, с помощью которого мы перечисляем значения в столбце CategoryName;  
#  PivotTable - псевдоним сводной таблицы, его необходимо указывать обязательно.

# ## 179 Операторы UNPIVOT

cur = conn.cursor()
sql = '''
--Создаѐм временную таблицу с помощью SELECT INTO 
SELECT 
    'Город' AS NamePar, 
    'Москва' AS Column1, 
    'Калуга' AS Column2, 
    'Тамбов' AS Column3 
INTO #TestUnpivot    
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
SELECT * FROM #TestUnpivot
'''
select(sql)

sql = '''
--Применяем оператор UNPIVOT 
SELECT NamePar, ColumnName, CityNameValue 
FROM #TestUnpivot 
UNPIVOT(
    CityNameValue FOR ColumnName IN ([Column1], [Column2], [Column3]) 
)AS UnpivotTable
'''
select(sql)

# Где,  
#  #TestUnpivot – таблица источник, в нашем случае временная таблица;  
#  CityNameValue – псевдоним столбца, который будет содержать значения наших столбцов;  
#  FOR – ключевое слово, с помощью которого мы указываем псевдоним для столбца, который будет содержать имена наших столбцов;  
#  ColumnName - псевдоним столбца, который будет содержать имена наших столбцов;  
#  IN – ключевое слово для указания имен столбцов.

# ---

conn.close()


