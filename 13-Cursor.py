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

# ## 153 Работа с курсорами

sql = '''SELECT * FROM TestTable'''
select(sql)

cur = conn.cursor()
sql = '''
--1. Объявление переменных 
DECLARE @ProductId INT,
    @ProductName VARCHAR(100),
    @Price MONEY

--2. Объявление курсора 
DECLARE TestCursor CURSOR FOR 
    SELECT ProductId, ProductName, Price
    FROM TestTable
    WHERE CategoryId = 1
--3. Открываем курсор
OPEN TestCursor
--4. Считываем данные из первой строки в курсоре --и записываем их в переменные 
FETCH NEXT FROM TestCursor INTO @ProductId, @ProductName, @Price
-- Запускаем цикл, выйдем из него, когда закончатся строки в курсоре
WHILE @@FETCH_STATUS = 0 
BEGIN
    --На каждую итерацию цикла выполняем необходимые нам SQL инструкции 
    --Для примера изменяем цену по условию
    IF @Price < 100 
        UPDATE TestTable SET
            Price = Price + 10
        WHERE ProductId = @ProductId
        --Считываем следующую строку курсора
        FETCH NEXT FROM TestCursor INTO @ProductId, @ProductName, @Price
END
--5. Закрываем курсор
CLOSE TestCursor 
-- Освобождаем ресурсы
DEALLOCATE TestCursor

'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTable'''
select(sql)

# ---

conn.close()


