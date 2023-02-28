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


# ## 115 Переменные

# + active=""
# DECLARE @TestTable TABLE (
#     [ProductId] [INT] IDENTITY(1,1) NOT NULL,
#     [CategoryId] [INT] NOT NULL,
#     [ProductName][VARCHAR](100) NOT NULL,
#     [Price] [Money] NULL
# ); 
#
# INSERT INTO @TestTable
#     SELECT CategoryId, ProductName, Price 
#         FROM TestTable 
#         WHERE ProductId <= 3;
#
# SELECT * FROM @TestTable;
# -

sql = '''
SELECT @@SERVERNAME [Имя локального сервера], @@VERSION AS [Версия SQL сервера];
'''
select(sql)

# ## 118 Команды условного выполнения

sql = '''
DECLARE @TestVar1 INT 
DECLARE @TestVar2 VARCHAR(20) 

SET @TestVar1 = 5 

IF @TestVar1 > 0 
    SET @TestVar2 = 'Больше 0' 
ELSE 
    SET @TestVar2 = 'Меньше 0' 
    
SELECT @TestVar2 AS [Значение TestVar1]
'''
select(sql)

sql = '''
DECLARE @TestVar1 INT 
DECLARE @TestVar2 VARCHAR(20) 

SET @TestVar1 = 0 

IF @TestVar1 > 0 
    SET @TestVar2 = 'Больше 0' 
    
SELECT @TestVar2 AS [Значение TestVar1]
'''
select(sql)

# ## 119 IF EXISTS

sql = '''
DECLARE @TestVar VARCHAR(20) 

IF EXISTS(SELECT * FROM TestTable) 
    SET @TestVar = 'Записи есть' 
ELSE 
    SET @TestVar = 'Записей нет' 
    
SELECT @TestVar AS [Наличие записей]
'''
select(sql)

# ## 120 CASE

sql = '''
DECLARE @TestVar1 INT 
DECLARE @TestVar2 VARCHAR(20) 

SET @TestVar1 = 1 

SELECT @TestVar2 = 
                CASE @TestVar1 WHEN 1 THEN 'Один' 
                    WHEN 2 THEN 'Два' 
                    ELSE 'Неизвестное' 
                END 
                
SELECT @TestVar2 AS [Число]
'''
select(sql)

# ## 121 BEGIN...END

sql = '''
DECLARE @TestVar1 INT 
DECLARE @TestVar2 VARCHAR(20), @TestVar3 VARCHAR(20) 

SET @TestVar1 = 5 

IF @TestVar1 NOT IN (0, 1, 2) 
BEGIN 
    SET @TestVar2 = 'Первая инструкция'; 
    SET @TestVar3 = 'Вторая инструкция'; 
END 

SELECT @TestVar2 AS [Значение TestVar1], @TestVar3 AS [Значение TestVar2]
'''
select(sql)

# ## 121 Циклы

sql = '''
DECLARE @CountAll INT = 0 --Запускаем цикл 

WHILE @CountAll < 10 
BEGIN 
    SET @CountAll = @CountAll + 1
END 

SELECT @CountAll AS [Результат]
'''
select(sql)

sql = '''
DECLARE @CountAll INT
SET @CountAll = 0
--Запускаем цикл 
WHILE @CountAll < 10 
BEGIN 
    SET @CountAll = @CountAll + 1
    IF @CountAll = 5 BREAK
END 
SELECT @CountAll AS [Результат];
'''
select(sql)

sql = '''
DECLARE @Cnt INT = 0 
DECLARE @CountAll INT = 0 
--Запускаем цикл
WHILE @CountAll < 10 
BEGIN 
    SET @CountAll += 1
    IF @CountAll = 5
        CONTINUE
    SET @Cnt += 1
END 

SELECT @CountAll AS [CountAll], @Cnt AS [Cnt];
'''
select(sql)

# ## 124 Команда RETURN

sql = '''
DECLARE @TestVar INT = 1 

IF @TestVar < 0 
    RETURN 
    
SELECT @TestVar AS [Результат]
'''
select(sql)

# ## 125 Команда GOTO

sql = '''
DECLARE @TestVar INT = 0

METKA: --Устанавливаем метку 
SET @TestVar += 1 --Увеличиваем значение переменной.
--Проверяем значение переменной
    IF @TestVar < 10 --Если оно меньше 10, то возвращаемся назад к метке
    GOTO METKA 

SELECT @TestVar AS [Результат];
'''
select(sql)

sql = '''
DECLARE @TestVar INT = 2
DECLARE @Rez INT = 0

IF @TestVar <= 0 
    GOTO METKA
SET @Rez = 10 / @TestVar

METKA: --Устанавливаем метку

SELECT @Rez AS [Результат];
'''
select(sql)

# ## 125 Команда WAITFOR

sql = '''
--Пауза на 5 секунд
WAITFOR DELAY '00:00:05'
SELECT 'Продолжение выполнение инструкции' AS [Test];
'''
select(sql)

sql = '''
--Пауза до 10 часов 
WAITFOR TIME '16:45:00' 
SELECT 'Продолжение выполнение инструкции' AS [Test];
'''
select(sql)

# ## 126 Обработка ошибок

sql = '''
--Начало блока обработки ошибок 
BEGIN TRY 
    --Инструкции, в которых могут возникнуть ошибки
    DECLARE @TestVar1 INT = 10,
            @TestVar2 INT = 0,
            @Rez INT
    SET @Rez = @TestVar1 / @TestVar2
END TRY 
--Начало блока CATCH
BEGIN CATCH
    -- Действия, которые будут выполняться в случае возникновения ошибки
    SELECT ERROR_NUMBER() AS [Номер ошибки], 
            ERROR_MESSAGE() AS [Описание ошибки]
    SET @Rez = 0 
END CATCH

SELECT @Rez AS [Результат];
'''
select(sql)

# ---

conn.close()


