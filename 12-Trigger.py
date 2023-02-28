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
drop table if exists AutitTestTable;

CREATE TABLE AutitTestTable( 
    Id INT IDENTITY(1,1) NOT NULL,
    DtChange DATETIME NOT NULL,
    UserName VARCHAR(100) NOT NULL,
    SQL_Command VARCHAR(100) NOT NULL,
    ProductId_Old INT NULL,
    ProductId_New INT NULL,
    CategoryId_Old INT NULL,
    CategoryId_New INT NULL,
    ProductName_Old VARCHAR(100) NULL,
    ProductName_New VARCHAR(100) NULL, 
    Price_Old MONEY NULL,
    Price_New MONEY NULL, 
    CONSTRAINT PK_AutitTestTable PRIMARY KEY (Id)
)
'''
cur.execute(sql)
conn.commit()
cur.close()

# ## 148 Создание триггеров на T-SQL

cur = conn.cursor()
sql = '''
CREATE TRIGGER TRG_Audit_TestTable ON TestTable 
    AFTER INSERT, UPDATE, DELETE
AS
BEGIN 
    DECLARE @SQL_Command VARCHAR(100);
    /*
    Определяем, что это за операция на основе наличия записей в таблицах inserted и deleted. На практике, 
    конечно же, лучше делать отдельный триггер для каждой операции
    */
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        SET @SQL_Command = 'INSERT'
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        SET @SQL_Command = 'UPDATE'
    IF NOT EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        SET @SQL_Command = 'DELETE'

    -- Инструкция если происходит добавление или обновление записи
    IF @SQL_Command = 'UPDATE' OR @SQL_Command = 'INSERT'
    BEGIN 
        INSERT INTO AutitTestTable(DtChange, UserName, SQL_Command, ProductId_Old, ProductId_New,
                CategoryId_Old, CategoryId_New, ProductName_Old, ProductName_New, Price_Old, Price_New)
            SELECT GETDATE(), SUSER_SNAME(), @SQL_Command,
                D.ProductId, I.ProductId, D.CategoryId, I.CategoryId, D.ProductName, I.ProductName, D.Price, I.Price
            FROM inserted I
            LEFT JOIN deleted D ON I.ProductId = D.ProductId
    END

    -- Инструкция если происходит удаление записи 
    IF @SQL_Command = 'DELETE'
    BEGIN 
        INSERT INTO AutitTestTable(DtChange, UserName, SQL_Command, ProductId_Old, ProductId_New,
                CategoryId_Old, CategoryId_New, ProductName_Old, ProductName_New, Price_Old, Price_New)
            SELECT GETDATE(), SUSER_SNAME(), @SQL_Command, D.ProductId, NULL, D.CategoryId, NULL, D.ProductName, NULL,
            D.Price, NULL FROM deleted D
    END
END 
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
--Добавляем запись 
INSERT INTO TestTable VALUES (1, 'Новый товар', 0)
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
--Изменяем запись
UPDATE TestTable SET
    ProductName = 'Наименование товара', 
    Price = 200
WHERE ProductName = 'Новый товар' 
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
--Удаляем запись
DELETE TestTable 
WHERE ProductName = 'Наименование товара' 
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
SELECT * FROM TestTable
'''
select(sql)

sql = '''
SELECT * FROM AutitTestTable
'''
select(sql)

# ## 150 Включение и отключение триггеров

cur = conn.cursor()
sql = '''
DISABLE TRIGGER TRG_Audit_TestTable ON TestTable;
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
ENABLE TRIGGER TRG_Audit_TestTable ON TestTable;
'''
cur.execute(sql)
conn.commit()
cur.close()

# ---

conn.close()


