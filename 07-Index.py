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

# ## 101 Создание индексов

cur = conn.cursor()
sql = '''
CREATE UNIQUE CLUSTERED INDEX IX_Clustered ON TestTable 
(
    ProductId ASC 
)   
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
CREATE NONCLUSTERED INDEX IX_NonClustered ON TestTable 
( 
    CategoryId ASC 
)
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
DROP INDEX IX_NonClustered ON TestTable;
'''
cur.execute(sql)
conn.commit()
cur.close()

# Иногда требуется изменить эти индексы, например, добавить еще один ключевой столбец или добавить так называемые «Включенные столбцы» - это столбцы, которые не являются ключевыми, но включаются в индекс. За счет этого уменьшается количество дисковых операций ввода-вывода и скорость доступа к данным, соответственно, увеличивается

cur = conn.cursor()
sql = '''
CREATE NONCLUSTERED INDEX IX_NonClustered ON TestTable 
( 
    CategoryId ASC,
    ProductName ASC
)
INCLUDE (Price)
'''
cur.execute(sql)
conn.commit()
cur.close()

# Изменить индекс:

cur = conn.cursor()
sql = '''
CREATE NONCLUSTERED INDEX IX_NonClustered ON TestTable 
( 
    CategoryId ASC,
    ProductName ASC
)
INCLUDE (Price) 
WITH (DROP_EXISTING = ON);
'''
cur.execute(sql)
conn.commit()
cur.close()

# ## 105 Обслуживание индексов

#  Если степень фрагментации менее 5%, то реорганизацию или перестроение индекса вообще не стоит запускать;  
#  Если степень фрагментации от 5 до 30%, то имеет смысл запустить реорганизацию индекса, 
# так как данная операция использует минимальные системные ресурсы и не требует долговременных блокировок;  
#  Если степень фрагментации более 30%, то необходимо выполнять перестроение индекса, 
# так как данная операция, при значительной фрагментации, дает больший эффект чем операция реорганизации индекса.

sql = '''
SELECT OBJECT_NAME(T1.object_id) AS NameTable, 
    T1.index_id AS IndexId, 
    T2.name AS IndexName, 
    T1.avg_fragmentation_in_percent AS Fragmentation 
FROM sys.dm_db_index_physical_stats 
    (DB_ID(), NULL, NULL, NULL, NULL) AS T1 
LEFT JOIN sys.indexes AS T2 ON T1.object_id = T2.object_id AND T1.index_id = T2.index_id;
'''
select(sql)

cur = conn.cursor()
sql = '''
ALTER INDEX IX_NonClustered ON TestTable 
    REORGANIZE;
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER INDEX IX_NonClustered ON TestTable 
    REBUILD;
'''
cur.execute(sql)
conn.commit()
cur.close()

# ---

conn.close()


