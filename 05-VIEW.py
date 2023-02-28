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

# ## 78 Пользовательские представления

cur = conn.cursor()
sql = '''
drop view if exists ViewCntProducts;
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
CREATE VIEW ViewCntProducts 
AS 
SELECT T2.CategoryName AS CategoryName, 
(
    SELECT COUNT(*) 
    FROM TestTable 
    WHERE CategoryId = T2.CategoryId
) AS CntProducts 
FROM TestTable2 T2
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER VIEW ViewCntProducts 
AS 
SELECT T2.CategoryId AS CategoryId,
T2.CategoryName AS CategoryName, 
(
    SELECT COUNT(*) 
    FROM TestTable 
    WHERE CategoryId = T2.CategoryId
) AS CntProducts 
FROM TestTable2 T2
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

sql = '''SELECT * FROM TestTable2'''
select(sql)

sql = '''
SELECT * FROM ViewCntProducts;
'''
select(sql)

# ## 80 Системные представления

sql = '''
SELECT * FROM sys.tables;
'''
select(sql)

sql = '''
SELECT * 
FROM sys.columns 
WHERE object_id = object_id('TestTable');
'''
select(sql)

# ---

cur = conn.cursor()
sql = '''
drop view if exists ViewCntProducts;
'''
cur.execute(sql)
conn.commit()
cur.close()

conn.close()


