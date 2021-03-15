#importación de librerias necesarias para el desarrollo de la automatización
import json
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
import requests
import datetime
from bs4 import BeautifulSoup
from urllib.request import Request
from urllib.request import urlopen
from six.moves import urllib
import csv

#Obtención de la fecha diaria total y dividida por año, mes(número y texto) y dia
fecha = datetime.datetime.now()
anio = fecha.year
print(anio)
mestexto = fecha.strftime("%B")
mes = fecha.month
dia = fecha.day

athena = boto3.client('athena',region_name = 'us-east-1') #Conectividad del athena por medio de boto3 (nombre servicio AWS, región donde se encuentra este)

#Creación de Querys por medio del athena.start_query_execution 
response = athena.start_query_execution(
    QueryString="alter table stocks add partition(company='avianca');", #Query a realizar, agregar partición por compañia
    QueryExecutionContext={
        'Database': 'yahooscraping' #Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/', #Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3', #Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString="alter table stocks add partition(company='ecopetrol');", #Query a realizar, agregar partición por compañia
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)
response = athena.start_query_execution(
    QueryString="alter table stocks add partition(company='cementosargos');",#Query a realizar, agregar partición por compañia
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString="alter table stocks add partition(company='grupoaval');",#Query a realizar, agregar partición por compañia
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table stocks add partition(year={anio});",#Query a realizar, agregar partición por año
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table stocks add partition(month='{mestexto}');",#Query a realizar, agregar partición por mes
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)
response = athena.start_query_execution(
    QueryString = f"alter table stocks add partition(day={dia});",#Query a realizar, agregar partición por dia
    QueryExecutionContext={
        'Database': 'yahooscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table noticias add partition(periodico='eltiempo');", #Query a realizar, agregar partición por periodico
    QueryExecutionContext={
        'Database': 'webscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table noticias add partition(periodico='publimetro');",#Query a realizar, agregar partición por periodico
    QueryExecutionContext={
        'Database': 'webscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table noticias add partition(year={anio});",#Query a realizar, agregar partición por año
    QueryExecutionContext={
        'Database': 'webscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)
response = athena.start_query_execution(
    QueryString = f"alter table noticias add partition(month='{mestexto}');",#Query a realizar, agregar partición por mes
    QueryExecutionContext={
        'Database': 'webscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table noticias add partition(day={dia});",#Query a realizar, agregar partición por dia
    QueryExecutionContext={
        'Database': 'webscraping'#Base de datos donde se realizara el Query
    },
    ResultConfiguration={
        'OutputLocation': 's3://buckettorresp/stocks/queryoutput/',#Dirección del bucket donde se almacena la respuesta del Query
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',#Metodo de encriptación del query SSE_S3 dado que se almacenara en un bucket S3
        }
    },
)