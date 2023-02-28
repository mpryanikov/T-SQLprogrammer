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


# https://www.crummy.com/software/BeautifulSoup/bs4/doc.ru/bs4ru.html

from bs4 import BeautifulSoup


def xml(sql, FieldName, xml = True):
    df = select(sql)
    for i, row in df.iterrows():
        if xml:
            print(BeautifulSoup(row[FieldName], "xml").prettify())
        else:
            print(BeautifulSoup(row[FieldName]).prettify())
        print()


cur = conn.cursor()
sql = '''
Drop table if exists TestTableXML;
CREATE TABLE TestTableXML(
    Id INT IDENTITY(1,1) NOT NULL,
    NameColumn VARCHAR(100) NOT NULL, 
    XMLData XML NULL 
    CONSTRAINT PK_TestTableXML PRIMARY KEY (Id)
);  
'''
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = '''
truncate table TestTableXML;
INSERT INTO TestTableXML (NameColumn, XMLData) VALUES(
    'Текст',
    '<Catalog> <Name>Иван</Name> <LastName>Иванов</LastName> </Catalog>'
)   
INSERT INTO TestTableXML (NameColumn, XMLData) VALUES(
    'Текст',
    '<Catalog> <Name>Иван</Name> <LastName>Петров</LastName> </Catalog>'
)  
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTableXML'''
xml(sql, 'XMLData')

# ## 160 Методы типа данных XML

# ### Метод query

sql = '''
SELECT XMLData.query('/Catalog/Name') AS [Тег Name] 
FROM TestTableXML
'''
select(sql)

xml(sql, 'Тег Name')

# ### Метод value

sql = '''
SELECT XMLData,
    XMLData.value('/Catalog[1]/LastName[1]','VARCHAR(100)') AS [LastName]
FROM TestTableXML
'''
select(sql)

# ### Метод exist

# данный метод используется для того, чтобы проверять наличие тех или иных значений, атрибутов или элементов в XML документе. Метод возвращает значения типа bit, такие как: 1 – если выражение на языке XQuery при запросе возвращает непустой результат, 0 – если возвращается пустой результат, NULL – если данные типа xml, к которым идет обращение, не содержат никаких данных, т.е. NULL.

sql = '''
SELECT * FROM TestTableXML WHERE XMLData.exist('/Catalog[1]/LastName') = 1
'''
select(sql)

# ### Метод modify

sql = '''SELECT * FROM TestTableXML'''
xml(sql, 'XMLData')

cur = conn.cursor()
sql = '''
UPDATE TestTableXML SET 
    XMLData.modify('delete /Catalog/LastName')
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTableXML'''
xml(sql, 'XMLData')

cur = conn.cursor()
sql = '''
UPDATE TestTableXML SET 
    XMLData.modify('insert <LastName>Иванов</LastName> as last into (/Catalog)[1] ')
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTableXML'''
xml(sql, 'XMLData')

cur = conn.cursor()
sql = '''
UPDATE TestTableXML SET
    XMLData.modify('replace value of(/Catalog/Name[1]/text())[1] with "Сергей" ') 
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''SELECT * FROM TestTableXML'''
xml(sql, 'XMLData')

# ### Метод nodes

XML_Doc = '''
<Root> <row id="1" Name="Иван"></row> <row id="2" Name="Сергей"></row> </Root>
'''

print(BeautifulSoup(XML_Doc, "xml").prettify())

sql = f'''
DECLARE @XML_Doc XML;

SET @XML_Doc = '{XML_Doc}'
--'<Root> <row id="1" Name="Иван"></row> <row id="2" Name="Сергей"></row> </Root>';

SELECT TMP.col.value('@id','INT') AS Id, 
    TMP.col.value('@Name','VARCHAR(10)') AS Name 
FROM @XML_Doc.nodes('/Root/row') TMP(Col); 
'''
select(sql)

# В данном случае метод nodes сформировал таблицу TMP и столбец Col для пути '/Root/row' в XML данных. Каждый элемент row здесь отдельная строка, методом value мы извлекаем значения атрибутов.

# ### Конструкция FOR XML

# #### Существуют следующие режимы:

#  **RAW** – в данном случае в XML документе создается одиночный элемент <row> для каждой строки результирующего набора данных инструкции SELECT;  
#  **AUTO** – в данном режиме структура XML документа создается автоматически, в зависимости от инструкции SELECT (объединений, вложенных запросов и так далее);  
#  **EXPLICIT** – режим, при котором Вы сами формируете структуру итогового XML документа, это самый расширенный режим работы конструкции FOR XML и, в то же время, самый трудоемкий;  
#  **PATH** – это своего рода упрощенный режим EXPLICIT, который хорошо справляется с множеством задач по формированию XML документов, включая формирование атрибутов для элементов. Если Вам нужно самим сформировать структуру XML данных, то рекомендовано использовать именно этот режим.

# #### Несколько полезных параметров конструкции FOR XML:

#  **TYPE** – возвращает сформированные XML данные с типом XML, если параметр TYPE не указан, данные возвращаются с типом nvarchar(max). Параметр необходим в тех случаях, когда над итоговыми XML данными будут проводиться операции, характерные для XML данных, например, выполнение инструкций на языке XQuery;  
#  **ELEMENTS** – если указать данный параметр, столбцы возвращаются в виде вложенных элементов;  
#  **ROOT** – параметр добавляет к результирующему XML-документу один элемент верхнего уровня (корневой элемент), по умолчанию «root», однако название можно указать произвольное.

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

# #### Пример 1
# В этом примере используется режим **RAW**, а также параметр **TYPE**, как видите, мы просто после основного запроса SELECT написали данную конструкцию. Запрос нам вернет XML данные, где каждая строка таблицы TestTable будет элементом row, а все столбцы отображены в виде атрибутов этого элемента.

sql = '''
with 
xmlr as(
    select(
        SELECT ProductId, ProductName, Price 
        FROM TestTable
        ORDER BY ProductId
        FOR XML RAW, TYPE
    )as XMLData
)
select * from xmlr

'''
select(sql)

xml(sql, 'XMLData', False)

# #### Пример 2
# В данном случае мы также используем режим **RAW**, только мы изменили название каждого элемента на '*Product*', для этого указали соответствующий параметр, добавили параметр **ELEMENTS**, для того чтобы столбцы были отображены в виде вложенных элементов, а также добавили корневой элемент '*Products*' с помощью параметра **ROOT**.

sql = '''
with 
xmlr as(
    select(
        SELECT ProductId, ProductName, Price 
        FROM TestTable 
        ORDER BY ProductId
        FOR XML RAW ('Product'), TYPE, ELEMENTS, ROOT ('Products')
    )as XMLData
)
select * from xmlr

'''
xml(sql, 'XMLData')

# #### Пример 3

sql = '''
SELECT TestTable.ProductId, TestTable.ProductName, TestTable2.CategoryName, TestTable.Price 
FROM TestTable 
LEFT JOIN TestTable2 ON TestTable.CategoryId = TestTable2.CategoryId 
ORDER BY TestTable.ProductId 
'''
select(sql)

# Сейчас мы использовали режим **AUTO**, при этом мы модифицировали запрос, добавили в него объединение для наглядности, в данном режиме нам вернулись XML данные, где записи таблицы TestTable представлены в виде элементов, ее столбцы в виде атрибутов, а соответствующие записи (на основе объединения) таблицы TestTable2 в виде вложенных элементов с атрибутами.

sql = f'''
with 
xmlr as(
    select({sql}
        FOR XML AUTO, TYPE
    )as XMLData
)
select * from xmlr

'''
xml(sql, 'XMLData', False)

# #### Пример 4

sql = '''
SELECT TestTable.ProductId, TestTable.ProductName, TestTable2.CategoryName, TestTable.Price 
FROM TestTable 
LEFT JOIN TestTable2 ON TestTable.CategoryId = TestTable2.CategoryId 
ORDER BY TestTable.ProductId  
'''
select(sql)

# Как видите, если мы добавим параметр **ELEMENTS**, то данные сформируются уже в виде элементов без атрибутов.

sql = f'''
with 
xmlr as(
    select({sql}
    FOR XML AUTO, TYPE, ELEMENTS
    )as XMLData
)
select * from xmlr
'''
xml(sql, 'XMLData', False)

# ### Пример 5

sql = '''SELECT * FROM TestTable'''
select(sql)

# В этом примере мы уже используем расширенный режим **PATH**, при котором мы можем сами указывать, что у нас будет элементами, а что атрибутами.  
# В запросе SELECT, при определении списка выборки, для столбца ProductId мы задали псевдоним Id с помощью инструкции "*@Id*", SQL сервер расценивает такую запись как инструкцию создания атрибута, таким образом, мы указали в результирующем XML документе, что у нас значение ProductId будет атрибутом элемента, а его название Id. Элементом у нас также выступает каждая строка таблицы TestTable, название элементов мы переопределили с помощью параметра, указав значение ('*Product*'), а с помощью параметра **ROOT** мы указали корневой элемент.

sql = f'''
with 
xmlr as(
    select(
        SELECT ProductId AS "@Id", ProductName, Price 
        FROM TestTable 
        ORDER BY ProductId 
        FOR XML PATH ('Product'), TYPE, ROOT ('Products')
    )as XMLData
)
select * from xmlr
'''
xml(sql, 'XMLData', False)

# ### 167 Конструкция OPENXML

# #### В следующем примере мы сформируем XML документ, а затем извлечем из него данные в табличном виде.

sql = '''
--Объявляем переменные 
DECLARE @XML_Doc XML; 
DECLARE @XML_Doc_Handle INT; 

--Формируем XML документ 
/*
<Products>
  <Product Id="1">
    <ProductName>Клавиатура</ProductName>
    <Price>100.0000</Price>
  </Product>
  <Product Id="2">
    <ProductName>Мышь</ProductName>
    <Price>60.0000</Price>
  </Product>
  ...
  */
SET @XML_Doc = ( 
    SELECT ProductId AS "@Id", ProductName, Price 
    FROM TestTable 
    ORDER BY ProductId 
    FOR XML PATH ('Product'), TYPE, ROOT ('Products') 
    );

--Подготавливаем XML документ 
EXEC sp_xml_preparedocument @XML_Doc_Handle OUTPUT, @XML_Doc; 

--Извлекаем данные из XML документа 
SELECT * 
FROM OPENXML (@XML_Doc_Handle, '/Products/Product', 2)
WITH (
    ProductId INT '@Id',
    ProductName VARCHAR(100),
    Price MONEY 
    );

--Удаляем дескриптор XML документа 
EXEC sp_xml_removedocument @XML_Doc_Handle;

'''
select(sql)

# #### XML документ строкой (1):

sql = '''
--Объявляем переменные 
DECLARE @XML_Str VARCHAR(max);
DECLARE @XML_Doc XML; 
DECLARE @XML_Doc_Handle INT; 

--Формируем XML документ 
SET @XML_Str = '<Products>
  <Product Id="1">
    <ProductName>Клавиатура</ProductName>
    <Price>100.0000</Price>
  </Product>
  <Product Id="2">
    <ProductName>Мышь</ProductName>
    <Price>50.0000</Price>
  </Product>
  <Product Id="3">
    <ProductName>Телефон</ProductName>
    <Price>300.0000</Price>
  </Product>
</Products>'

SET @XML_Doc = @XML_Str;

--Подготавливаем XML документ 
EXEC sp_xml_preparedocument @XML_Doc_Handle OUTPUT, @XML_Doc; 

--Извлекаем данные из XML документа 
SELECT * 
FROM OPENXML (@XML_Doc_Handle, '/Products/Product', 2)
WITH (
    ProductId INT '@Id',
    ProductName VARCHAR(100),
    Price MONEY 
    );

--Удаляем дескриптор XML документа 
EXEC sp_xml_removedocument @XML_Doc_Handle;
'''
select(sql)

# #### XML документ строкой (2):

xml = """<Products>
  <Product Id="1">
    <ProductName>Клавиатура</ProductName>
    <Price>100.0000</Price>
  </Product>
  <Product Id="2">
    <ProductName>Мышь</ProductName>
    <Price>50.0000</Price>
  </Product>
  <Product Id="3">
    <ProductName>Телефон</ProductName>
    <Price>300.0000</Price>
  </Product>
</Products>"""

sql = f'''
--Объявляем переменные 
DECLARE @XML_Str VARCHAR(max);
DECLARE @XML_Doc XML; 
DECLARE @XML_Doc_Handle INT; 

--Формируем XML документ 
SET @XML_Str = '{xml}';
SET @XML_Doc = @XML_Str;

--Подготавливаем XML документ 
EXEC sp_xml_preparedocument @XML_Doc_Handle OUTPUT, @XML_Doc; 

--Извлекаем данные из XML документа 
SELECT * 
FROM OPENXML (@XML_Doc_Handle, '/Products/Product', 2)
WITH (
    ProductId INT '@Id',
    ProductName VARCHAR(100),
    Price MONEY 
    );

--Удаляем дескриптор XML документа 
EXEC sp_xml_removedocument @XML_Doc_Handle;

'''
select(sql)

# #### XML документ строкой (2):

xml = """<data>
    <student>
        <name>Alice</name>
        <major>Computer Science</major>
        <age>20</age>
    </student>
    <student>
        <name>Bob</name>
        <major>Philosophy</major>
        <age>22</age>
    </student>
    <student>
        <name>Mary</name>
        <major>Biology</major>
        <age>21</age>
    </student>
</data>"""

sql = f'''
--Объявляем переменные 
DECLARE @XML_Str VARCHAR(max);
DECLARE @XML_Doc XML; 
DECLARE @XML_Doc_Handle INT; 

--Формируем XML документ 
SET @XML_Str = '{xml}';
SET @XML_Doc = @XML_Str;

--Подготавливаем XML документ 
EXEC sp_xml_preparedocument @XML_Doc_Handle OUTPUT, @XML_Doc; 

--Извлекаем данные из XML документа 
SELECT * 
FROM OPENXML (@XML_Doc_Handle, '/data/student', 2)
WITH (
    name VARCHAR(100),
    major VARCHAR(100),
    age INT 
    );

--Удаляем дескриптор XML документа 
EXEC sp_xml_removedocument @XML_Doc_Handle;

'''
select(sql)

# ---

conn.close()

# ---

# https://blog.finxter.com/reading-and-writing-xml-with-pandas/

xml = """<?xml version='1.0' encoding='utf-8'?>
<data>
    <student>
        <name>Alice</name>
        <major>Computer Science</major>
        <age>20</age>
    </student>
    <student>
        <name>Bob</name>
        <major>Philosophy</major>
        <age>22</age>
    </student>
    <student>
        <name>Mary</name>
        <major>Biology</major>
        <age>21</age>
    </student>
</data>"""

df = pd.read_xml(xml)
print(df)

print(df.to_xml())

# ---

# https://rukovodstvo.net/posts/id_659/

# https://translated.turbopages.org/proxy_u/en-ru.ru.724ddfd8-63a56540-442180e5-74722d776562/https/stackoverflow.com/questions/28813876/how-do-i-get-pythons-elementtree-to-pretty-print-to-an-xml-file.

# https://dev-gang.ru/article/kak-perebirat-stroki-v-freime-dannyh-pandas-6kv1i4ayi8/


