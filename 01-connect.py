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


# # Создание базы данных

# + active=""
# --031 CreateDatabase
# CREATE DATABASE TestDB
# -

# 043 CreateTable
cur = conn.cursor()
sql = '''
USE TestDB;
drop table if exists TestTable;
drop table if exists TestTable2;
drop table if exists TestTable3;

CREATE TABLE TestTable(
[ProductId] [INT] IDENTITY(1,1) NOT NULL, 
[CategoryId] [INT] NOT NULL, 
[ProductName] [VARCHAR](100) NOT NULL, 
[Price] [Money] NULL 
) 

--GO 

CREATE TABLE TestTable2(
[CategoryId] [INT] IDENTITY(1,1) NOT NULL, 
[CategoryName] [VARCHAR](100) NOT NULL 
) 

--GO

CREATE TABLE TestTable3(
[ProductId] [INT] IDENTITY(1,1) NOT NULL, 
[ProductName] [VARCHAR](100) NOT NULL, 
[Weight] [DECIMAL](18, 2) NULL, 
[Price] [Money] NULL, 
[Summa] AS ([Weight] * [Price]) PERSISTED
)

--GO
'''
cur.execute(sql)
conn.commit()

# 044 AlterTable
cur = conn.cursor()
sql = '''
USE TestDB;
ALTER TABLE TestTable3 ADD [SummaDop] AS ([Weight] * [Price]) PERSISTED;
ALTER TABLE TestTable ALTER COLUMN [Price] [Money] NOT NULL;
ALTER TABLE TestTable DROP COLUMN [Price];
ALTER TABLE TestTable ADD [Price] [Money] NULL;
'''
cur.execute(sql)
conn.commit()

# 047 InsertTable
cur = conn.cursor()
sql = '''
USE TestDB;

INSERT INTO TestTable VALUES 
(1,'Клавиатура', 100), 
(1, 'Мышь', 50), 
(2, 'Телефон', 300) 

--GO 

INSERT INTO TestTable2 VALUES 
('Комплектующие компьютера'), 
('Мобильные устройства') 

--GO
'''
cur.execute(sql)
conn.commit()

# 048 SelectTable
sql = '''
SELECT * FROM TestTable;
'''

select(sql)

# 048 SelectTable
sql = '''
select * from TestTable2
'''

select(sql)


