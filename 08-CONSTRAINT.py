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
'''
cur.execute(sql)
conn.commit()
cur.close()

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

cur = conn.cursor()
sql = '''
drop table if exists TestTable8;
drop table if exists TestTable7;
drop table if exists TestTable6;
drop table if exists TestTable5;
drop table if exists TestTable4;
'''
cur.execute(sql)
conn.commit()
cur.close()

# ## 109 Создание ограничений

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable 
    ALTER COLUMN [Price] [Money] NOT NULL;    
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable 
    ADD CONSTRAINT PK_TestTable PRIMARY KEY (ProductId);   
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable4
CREATE TABLE TestTable4(
    [CategoryId] [INT] IDENTITY(1,1) NOT NULL 
        CONSTRAINT PK_CategoryId PRIMARY KEY, 
    [CategoryName] [VARCHAR](100) NOT NULL   
)
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable4
CREATE TABLE TestTable4(
    [CategoryId] [INT] IDENTITY(1,1) NOT NULL, 
    [CategoryName] [VARCHAR](100) NOT NULL, 
    CONSTRAINT PK_CategoryId PRIMARY KEY (CategoryId)   
)
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable5
CREATE TABLE TestTable5(
    [ProductId] [INT] IDENTITY(1,1) NOT NULL, 
    [CategoryId] [INT] NOT NULL, 
    [ProductName] [VARCHAR](100) NOT NULL, 
    [Price] [MONEY] NULL, 
    CONSTRAINT PK_TestTable5 PRIMARY KEY (ProductId), 
    CONSTRAINT FK_TestTable5 FOREIGN KEY (CategoryId) 
        REFERENCES TestTable4 (CategoryId) 
            ON DELETE CASCADE 
            ON UPDATE NO ACTION 
); 
'''
cur.execute(sql)
conn.commit()
cur.close()

# Для инструкций ON DELETE и ON UPDATE доступны следующие значения:   
# NO ACTION - ничего не делать, просто выводить ошибку,   
# CASCADE – каскадное изменение,  
# SET NULL - присвоить значение NULL,  
# SET DEFAULT - присвоить значение по умолчанию.  
# Эти инструкции необязательные, их можно и не указывать, тогда при изменении ключа, в случае наличия связанных записей,  
# будет выходить ошибка.

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable2 
    ADD CONSTRAINT PK_TestTable2 PRIMARY KEY (CategoryId); 
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable 
    ADD CONSTRAINT FK_TestTable FOREIGN KEY (CategoryId) 
        REFERENCES TestTable2 (CategoryId); 
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable6
CREATE TABLE TestTable6(
    [Column1] [INT] NOT NULL CONSTRAINT PK_TestTable6_C1 UNIQUE, 
    [Column2] [INT] NOT NULL, 
    [Column3] [INT] NOT NULL, 
    CONSTRAINT PK_TestTable6_C2 UNIQUE (Column3) 
); 
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable6 
    ADD CONSTRAINT PK_TestTable6_C3 UNIQUE (Column3);
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable7
CREATE TABLE TestTable7( 
    [Column1] [INT] NOT NULL,
    [Column2] [INT] NOT NULL,
    CONSTRAINT CK_TestTable7_C1 CHECK (Column1 <> 0) 
);
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable7 
    ADD CONSTRAINT CK_TestTable7_C2 CHECK (Column2 > Column1);
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
drop table if exists TestTable8
CREATE TABLE TestTable8(
    [Column1] [INT] NULL CONSTRAINT DF_C1 DEFAULT (1), 
    [Column2] [INT] NULL 
);
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable8 
    ADD CONSTRAINT DF_C2 DEFAULT (2) FOR Column2;
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ALTER TABLE TestTable7 DROP CONSTRAINT CK_TestTable7_C1;
ALTER TABLE TestTable7 DROP CONSTRAINT CK_TestTable7_C2;
ALTER TABLE TestTable8 DROP CONSTRAINT DF_C1;
ALTER TABLE TestTable8 DROP CONSTRAINT DF_C2;
'''
cur.execute(sql)
conn.commit()
cur.close()

# ---

conn.close()


