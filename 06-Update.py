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
drop table if exists TestTable;
CREATE TABLE TestTable(
[ProductId] [INT] IDENTITY(1,1) NOT NULL, 
[CategoryId] [INT] NOT NULL, 
[ProductName] [VARCHAR](100) NOT NULL, 
[Price] [Money] NULL 
) 

drop table if exists TestTable2;
CREATE TABLE TestTable2(
[CategoryId] [INT] IDENTITY(1,1) NOT NULL, 
[CategoryName] [VARCHAR](100) NOT NULL 
) 
'''
cur.execute(sql)
conn.commit()

cur = conn.cursor()
sql = '''
truncate table TestTable;
INSERT INTO TestTable VALUES 
    (1,'Клавиатура', 100), 
    (1, 'Мышь', 50), 
    (2, 'Телефон', 300);
    
INSERT INTO TestTable VALUES 
    (1,'Клавиатура', 100), 
    (1, 'Мышь', 50), 
    (2, 'Телефон', 300);    
    
truncate table TestTable2;    
INSERT INTO TestTable2 VALUES 
    ('Комплектующие компьютера'), 
    ('Мобильные устройства');    
    
INSERT INTO TestTable2 VALUES 
    ('Комплектующие компьютера'), 
    ('Мобильные устройства');        
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

# ## 85 INSERT INTO TestTable

sql = '''SELECT * FROM TestTable'''
select(sql)

cur = conn.cursor()
sql = '''
INSERT INTO TestTable (CategoryId, ProductName, Price) 
    SELECT CategoryId, ProductName, Price 
    FROM TestTable WHERE ProductId > 3;    
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

# ## 172 Конструкция SELECT INTO

cur = conn.cursor()
sql = '''
drop table if exists TestTableDop;

SELECT T1.ProductName, T2.CategoryName, T1.Price 
INTO TestTableDop 
FROM TestTable T1 
LEFT JOIN TestTable2 T2 ON T1.CategoryId = T2.CategoryId 
WHERE T1.CategoryId = 1 
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
SELECT * FROM TestTableDop
'''
select(sql)

# ## 86 Обновление данных – UPDATE

cur = conn.cursor()
sql = '''
UPDATE TestTable SET 
    Price = 120 
WHERE ProductId = 1    
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
SELECT * FROM TestTable WHERE ProductId > 3 
'''
select(sql)

cur = conn.cursor()
sql = '''
UPDATE TestTable SET 
    ProductName = 'Тестовый товар', 
    Price = 150 
WHERE ProductId > 3   
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
SELECT * FROM TestTable WHERE ProductId > 3 
'''
select(sql)

# #### достаточно часто требуется перекинуть данные из одной таблицу в другую, 
# т.е. обновить записи одной таблицы на значения, которые расположены в другой.

cur = conn.cursor()
sql = '''
UPDATE TestTable SET 
    ProductName = T2.CategoryName, 
    Price = 200 
FROM TestTable2 T2 --источник 
INNER JOIN TestTable T1 ON T1.CategoryId = T2.CategoryId 
WHERE T1.ProductId > 3   
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
SELECT * FROM TestTable WHERE ProductId > 3 
'''
select(sql)

# ## 90 Удаление данных – DELETE, TRUNCATE

sql = '''SELECT * FROM TestTable'''
select(sql)

cur = conn.cursor()
sql = '''
DELETE TestTable WHERE ProductId > 3  
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

# ## 95 MERGE

cur = conn.cursor()
sql = '''
drop table if exists TestTable3;
CREATE TABLE TestTable3(
    [ProductId] [INT] NOT NULL, 
    [CategoryId] [INT] NOT NULL, 
    [ProductName] [VARCHAR](100) NOT NULL, 
    [Price] [Money] NULL
)
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
INSERT INTO TestTable3 VALUES 
    (1, 1, 'Клавиатура', 0), 
    (2, 1, 'Мышь', 0), 
    (4, 1, 'Тест', 0)     
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable3'''
select(sql)

sql = '''
MERGE TestTable3 AS T_Base 
    USING TestTable AS T_Source 
    ON (T_Base.ProductId = T_Source.ProductId) 
    WHEN MATCHED THEN 
        UPDATE SET 
        ProductName = T_Source.ProductName, 
        CategoryId = T_Source.CategoryId, 
        Price = T_Source.Price 
    WHEN NOT MATCHED THEN 
        INSERT (ProductId, CategoryId, ProductName, Price) VALUES 
        (T_Source.ProductId, 
        T_Source.CategoryId, 
        T_Source.ProductName, 
        T_Source.Price) 
    WHEN NOT MATCHED BY SOURCE THEN 
        DELETE 
    OUTPUT $action AS [Операция], 
        Inserted.ProductId, 
        Inserted.ProductName AS ProductNameNEW, 
        Inserted.Price AS PriceNEW, 
        Deleted.ProductName AS ProductNameOLD, 
        Deleted.Price AS PriceOLD;  
'''
select(sql)

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable3'''
select(sql)

# ## 96 Инструкция OUTPUT

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
INSERT INTO TestTable 
    OUTPUT Inserted.ProductId, 
        Inserted.CategoryId, 
        Inserted.ProductName, 
        Inserted.Price 
    VALUES (1, 'Тестовый товар 1', 300), 
        (1, 'Тестовый товар 2', 500), 
        (2, 'Тестовый товар 3', 400);
'''
select(sql)

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
UPDATE TestTable SET 
    Price = 0 
    OUTPUT Inserted.ProductId AS [ProductId], 
        Deleted.Price AS [Старое значение Price], 
        Inserted.Price AS [Новое значение Price] 
    WHERE ProductId > 3;
'''
select(sql)

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''
DELETE TestTable 
OUTPUT Deleted.* 
WHERE ProductId > 3;
'''
select(sql)

sql = '''SELECT * FROM TestTable'''
select(sql)

# ---

conn.close()


