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

titulos = []
categorias = []
noticias = []

def f1(event, context):

    fecha = datetime.datetime.now()
    anio = fecha.year
    print(anio)
    mestexto = fecha.strftime("%B")
    mes = fecha.month
    dia = fecha.day


    s3 = boto3.client('s3')
   
    

    nameBucket=event['Records'][0]['s3']['bucket']['name']
 
    nameObject=(unquote_plus(event ['Records'][0]['s3']['object']['key']).split('/')[-1]).split('.')[0]
    key=unquote_plus(event ['Records'][0]['s3']['object']['key'])

    print(nameObject)
    print(key)
    download_path = '/tmp/{}.csv'.format(nameObject)

    s3.download_file(nameBucket,key,download_path)
    s3.upload_file(download_path,'buckettorresp',f'news/raw/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/Eltiempo.csv')

    nameObject2='publimetro'
    key2=f'headlines/final/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv'
    download_path2 = '/tmp/{}.csv'.format(nameObject2)
    s3.download_file(nameBucket,key2,download_path2)
    s3.upload_file(download_path2,'buckettorresp',f'news/raw/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv')
