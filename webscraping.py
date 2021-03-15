#importar librerias necesarias para la ejecucion del proyecto
import csv #es la libreria que permitira crear csv en python 
import json
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.request import Request
from urllib.request import urlopen
from six.moves import urllib

#lista donde se van a almacenar los requerimientos 
titulos = []
categorias = []
noticias = []

titulos2 = []
categorias2 = []
noticias2 = []
link=[]

s3 = boto3.client('s3') #enlace de s3 con la funcion lambda
fecha = datetime.datetime.now() #establecer tiempo actual
anio = fecha.year #establecer año actual
print(anio) #imprimir año
mestexto = fecha.strftime("%B") 
mes = fecha.month #mes establecido
dia = fecha.day #dia establecido

urllib.request.urlretrieve('https://www.eltiempo.com/', '/home/ubuntu/eltiempo.html') #obtencion del archivo .html


ruta_Archivo = "/home/ubuntu/eltiempo.html" #ubicacion del archivo
s3.upload_file(ruta_Archivo,'buckettorresp',f'headlines/raw/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/eltiempo.html') #subir el archivo al bucket mediante boto3
#lectura del archivo descargado
f = open(ruta_Archivo,'r') 
Fe =  f.read() 

soup = BeautifulSoup(Fe, 'lxml') #lectura del archivo con discriminacion de clases y etiquetas por medio de Beautifulsoup 


categories = soup.findall(class=re.compile("category page-link")) #obtencion de las categorias 

titles = soup.findall(class="title page-link") #obtencion de los titulares 

#for para construir la lista noticias (titular, categoria, link)
for i in titles:
    titulos.append(i.string)
    noticias.append([i.string,'', 'https://www.eltiempo.com/' + i['href']])

for i in range(len(categories)):
    categorias.append(categories[i].string)
    noticias[i][1] = categories[i].string
#for x in noticias:
#    print(x)

#creacion del archivo .csv con la lista noticias 
with open('/home/ubuntu/Eltiempo.csv','w',newline='') as fp:
    Q = csv.writer(fp,delimiter=',')
    Q.writerows(noticias)

s3.upload_file('/home/ubuntu/Eltiempo.csv','buckettorresp',f'headlines/final/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/Eltiempo.csv') #subir el archivo al bucket mediante boto3

urllib.request.urlretrieve('https://www.publimetro.co/co/', '/home/ubuntu/publimetro.html')
ruta_Archivo = "/home/ubuntu/publimetro.html"
s3.upload_file(ruta_Archivo,'buckettorresp',f'headlines/raw/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.html')

ruta_Archivo2 = "/home/ubuntu/publimetro.html"
p = open(ruta_Archivo2,'r')
Pe = p.read()
soup2 = BeautifulSoup(Pe, 'lxml')

categories2 = soup2.findall(class=re.compile("cardKicker"))
for i in soup2.find_all('a',href=True):
    link.append(i['href'])

titles2 = soup2.findall(class=re.compile("cardTitle"))

for i in titles2:
    titulos2.append(i.string)
    noticias2.append([i.string,'',''])

for i in range(len(categories2)):
    categorias2.append(categories2[i].string)
    noticias2[i][1] = categories2[i].string

for i in range(len(titles2)):
    noticias2[i][2] = link[i]

for i in noticias2:
    print(i)

with open('/home/ubuntu/publimetro.csv','w',newline='') as fp2:
    Q2 = csv.writer(fp2,delimiter=',')
    Q2.writerows(noticias2)

s3.upload_file('/home/ubuntu/publimetro.csv','buckettorresp',f'headlines/final/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv')