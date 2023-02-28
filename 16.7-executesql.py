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

# ## 188 Выполнение динамических T-SQL инструкций

# ### Пример с использованием команды EXEC.

# + active=""
# --Объявляем переменные 
# DECLARE @SQL_QUERY VARCHAR(200), @Var1 INT; 
# --Присваиваем значение переменным 
# SET @Var1 = 1; 
# --Формируем SQL инструкцию 
# SET @SQL_QUERY = 'SELECT * FROM TestTable WHERE ProductID = ' + CAST(@Var1 AS VARCHAR(10));
# --Смотрим на итоговую строку 
# SELECT @SQL_QUERY AS [TEXT QUERY] 
# --Выполняем текстовую строку как SQL инструкцию 
# EXEC (@SQL_QUERY)
# -

# ### Пример с использованием хранимой процедуры sp_executesql.

# + active=""
# --Объявляем переменные 
# DECLARE @SQL_QUERY NVARCHAR(200); 
# --Формируем SQL инструкцию 
# SELECT @SQL_QUERY = N'SELECT * FROM TestTable WHERE ProductID = @Var1;'; 
# --Смотрим на итоговую строку 
# SELECT @SQL_QUERY AS [TEXT QUERY] 
# --Выполняем текстовую строку как SQL инструкцию 
# EXEC sp_executesql @SQL_QUERY,--Текст SQL инструкции 
# N'@Var1 AS INT', --Объявление переменных в процедуре 
# @Var1 = 1 --Передаем значение для переменных
# -

# ---

conn.close()


