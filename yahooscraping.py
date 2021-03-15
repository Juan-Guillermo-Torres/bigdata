#importar librerias necesarias para la ejecucion del proyecto
import yfinance as yf #obtencion de la api de yahoo finance para extraccion de informacion 
import json
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
import requests
import datetime
import csv


fecha = datetime.datetime.now()

anio = fecha.year

print(anio)

mestexto = fecha.strftime("%B")

mes = fecha.month

dia = fecha.day

print(dia-1)


#AVIANCA

data_df = yf.download("AVHOQ", start=f"{anio}-{mes}-{dia-3}", end=f"{anio}-{mes}-{dia}") #obtencion de informacion de bolsa de avianca 
data_df.to_csv('AVHOQ.csv') #traduccion a archivo .csv
print(csv) 
s3 = boto3.client('s3') #enlace a bucket por boto3
ruta_Archivo = f"/home/ubuntu/AVHOQ.csv" #ruta ubicacion del archivo 
#modificacion del csv descargado para eliminar los headers 
lines = open(ruta_Archivo,'r').readlines() 
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'buckettorresp',f'stocks/company=avianca/year={anio}/month={mestexto}/day={dia}/AVHOQ.csv') #subida del archivo al bucket y automatizacion de fecha

#AVIANCA
#ARGOS

data_df = yf.download("CMTOY", start=f"{anio}-{mes}-{dia-3}", end=f"{anio}-{mes}-{dia}") #obtencion de informacion de bolsa de cementos argos
data_df.to_csv('CMTOY.csv') #traduccion a archivo .csv
print(csv) 
s3 = boto3.client('s3') #enlace a bucket por boto3
ruta_Archivo = f"/home/ubuntu/CMTOY.csv" #ruta ubicacion del archivo
#modificacion del csv descargado para eliminar los headers 
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'buckettorresp',f'stocks/company=cementosargos/year={anio}/month={mestexto}/day={dia}/CMTOY.csv') #subida del archivo al bucket y automatizacion de fecha

#ARGOS

#ECOPETROL

data_df = yf.download("EC", start=f"{anio}-{mes}-{dia-3}", end=f"{anio}-{mes}-{dia}") #obtencion de informacion de bolsa de ecopetrol
data_df.to_csv('EC.csv') #traduccion a archivo .csv
print(csv)
s3 = boto3.client('s3') #enlace a bucket por boto3
ruta_Archivo = f"/home/ubuntu/EC.csv" #ruta ubicacion del archivo
#modificacion del csv descargado para eliminar los headers 
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'buckettorresp',f'stocks/company=ecopetrol/year={anio}/month={mestexto}/day={dia}/EC.csv') #subida del archivo al bucket y automatizacion de fecha


#ECOPETROL

#AVAL

data_df = yf.download("AVAL", start=f"{anio}-{mes}-{dia-3}", end=f"{anio}-{mes}-{dia}") #obtencion de informacion de bolsa de grupo aval
data_df.to_csv('AVAL.csv') #traduccion a archivo .csv
print(csv)
s3 = boto3.client('s3') #enlace a bucket por boto3
ruta_Archivo = f"/home/ubuntu/AVAL.csv" #ruta ubicacion del archivo
#modificacion del csv descargado para eliminar los headers 
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'buckettorresp',f'stocks/company=grupoaval/year={anio}/month={mestexto}/day={dia}/AVAL.csv') #subida del archivo al bucket y automatizacion de fecha

#AVAL