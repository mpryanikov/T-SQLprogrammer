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

# ## 191 Администрирование сервера и базы данных

# ### Безопасность

# #### Создание имени входа

cur = conn.cursor()
sql = """
CREATE LOGIN [TestLogin] 
    WITH PASSWORD='Pa$$w0rd', 
    DEFAULT_DATABASE=[TestDB]   
"""
cur.execute(sql)
conn.commit()
cur.close()

# #### Назначение роли сервера

cur = conn.cursor()
sql = """
EXEC sp_addsrvrolemember 
    @loginame = 'TestLogin', 
    @rolename = 'sysadmin' 
"""
cur.execute(sql)
conn.commit()
cur.close()

# #### Создание пользователя базы данных и сопоставление с именем входа

cur = conn.cursor()
sql = """
CREATE USER [TestUser] 
    FOR LOGIN [TestLogin]
"""
cur.execute(sql)
conn.commit()
cur.close()

# #### Назначение пользователю роли базы данных (права доступа к объектам)

cur = conn.cursor()
sql = """
EXEC sp_addrolemember 'db_owner', 'TestUser'
"""
cur.execute(sql)
conn.commit()
cur.close()

# ### Параметры базы данных

cur = conn.cursor()
sql = """
ALTER DATABASE [TestDB] SET READ_ONLY
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
ALTER DATABASE [TestDB] SET READ_WRITE
"""
cur.execute(sql)
conn.commit()
cur.close()

# ### Создание архива базы данных

cur = conn.cursor()
sql = """
BACKUP DATABASE [TestDB] 
    TO DISK = 'A:\BACKUP_DB\TestDB.bak' 
    WITH 
        NAME = N'База данных TestDB', 
        STATS = 10
"""
cur.execute(sql)
conn.commit()
cur.close()

# + active=""
# 18 проц. обработано.
# 37 проц. обработано.
# 55 проц. обработано.
# 61 проц. обработано.
# 73 проц. обработано.
# 80 проц. обработано.
# 99 проц. обработано.
# 100 проц. обработано.
# Обработано 704 страниц для базы данных "TestDB", файл "TestDB" для файла 1.
# Обработано 2 страниц для базы данных "TestDB", файл "TestDB_log" для файла 1.
# BACKUP DATABASE успешно обработал 706 страниц за 0.049 секунд (112.484 MБ/сек).
#
# Completion time: 2022-12-25T16:58:06.7171007+03:00
# -

# ### Восстановление базы данных из архива

# Существуют следующие модели восстановления:  
#  **Простая** – используется для баз данных, данные в которых изменяются неинтенсивно, и потеря данных с момента создания последней копии базы не является критичной. Например, копии создаются каждую ночь, если произошел сбой в середине дня, то все данные, которые были сделаны в течение этого дня, будут потеряны. Копия журнала транзакций при такой модели восстановления не создается;  
#  **Полная** – используется для баз данных, в которых необходима поддержка длительных транзакций. Это самая надежная модель восстановления, она позволяет восстановить базу данных до точки сбоя, в случае наличия заключительного фрагмента журнала транзакций. В данном случае копию журнала транзакций необходимо делать по возможности как можно чаще. В журнал транзакций записываются все операции;  
#  С **неполным протоколированием** – данная модель похожа на «Полную» модель, однако в данном случае большинство массовых операций не протоколируется, и, в случае сбоя, их придѐтся повторить с момента создания последней копии журнал транзакций.

# + active=""
# USE master 
# GO 
# RESTORE DATABASE [TestDB] 
#     FROM DISK = N'D:\BACKUP_DB\TestDB.bak' 
#     WITH 
#         FILE = 1, 
#         STATS = 10
# -

# ### Перемещение базы данных

# + active=""
# USE master 
# GO 
# EXEC sp_detach_db @dbname = 'TestDB'

# + active=""
# USE master 
# GO 
# CREATE DATABASE [TestDB] ON (
#     FILENAME = 'D:\DataBase\TestDB.mdf'), 
#     (FILENAME = 'D:\DataBase\TestDB_log.ldf') 
# FOR ATTACH 
# GO
# -

# ### Сжатие базы данных

cur = conn.cursor()
sql = """
DBCC SHRINKDATABASE('TestDB')
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
DBCC SHRINKFILE ('TestDB_log', 5)
"""
cur.execute(sql)
conn.commit()
cur.close()

# ---

conn.close()


