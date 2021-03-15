#Librerias para el funcionamiento 
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

#Listas donde se van a guardar la informacion
titulos = []
categorias = []
noticias = []

#funcion f1, en la cual se va a programar cada evento
def f1(event, context):

    fecha = datetime.datetime.now() #establecer tiempo actual
    anio = fecha.year #establecer año actual
    print(anio) #imprimir año
    mestexto = fecha.strftime("%B") 
    mes = fecha.month #mes establecido
    dia = fecha.day #dia establecido


    s3 = boto3.client('s3') #enlace de s3 con la funcion lambda
   
    

    nameBucket=event['Records'][0]['s3']['bucket']['name'] #obtencion del nombre del bucket por medio del evento
 
    nameObject=(unquote_plus(event ['Records'][0]['s3']['object']['key']).split('/')[-1]).split('.')[0] #obtencion del nombre objeto dentro del bucket 
    key=unquote_plus(event ['Records'][0]['s3']['object']['key']) #obtencion ruta del objeto

    print(nameObject)
    print(key)
    download_path = '/tmp/{}.csv'.format(nameObject) #ruta en la que se va a descargar el archivo en el entorno lambda

    s3.download_file(nameBucket,key,download_path) #descarga del archivo por medio de boto3
    s3.upload_file(download_path,'buckettorresp',f'news/raw/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/Eltiempo.csv') #subir el archivo por medio del boto3

    nameObject2='publimetro' #obtencion del nombre objeto dentro del bucket 
    key2=f'headlines/final/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv' #obtencion ruta del objeto
    download_path2 = '/tmp/{}.csv'.format(nameObject2) #ruta en la que se va a descargar el archivo en el entorno lambda
    s3.download_file(nameBucket,key2,download_path2) #descarga del archivo por medio de boto3
    s3.upload_file(download_path2,'buckettorresp',f'news/raw/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv') #subir el archivo por medio del boto3
