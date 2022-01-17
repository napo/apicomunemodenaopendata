# -*- coding: utf-8 -*-
import pandas as pd
import requests
import geopandas as gpd
from datetime import date
from zipfile import ZipFile
import numpy as np
import json
import os
import re
def save_json(data, out):
    """save json to a file"""
    with open(out, 'w') as outfile:
        json.dump(data, outfile)

headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

today = date.today()
today_str = today.strftime("%Y-%m-%d")
today_str += " 00:00"

url="https://www.comune.modena.it/api/@querystring-search"

subdata = {"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]}
subdata_2 = {"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":today_str}
d = [subdata,subdata_2]
data={'fullobjects': '1',"query":d}

r = requests.post(url, headers=headers,json={"b_size":10000,"fullobjects":1,"query":[{"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]},{"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":"2019-01-01 00:00"}]})

#r = requests.post(url, headers=headers,json=data)})

#r.encoding = 'ISO 8859-1'
r.encoding = 'utf-8'
data_scraped = r.json()
save_json(data_scraped,"docs/eventi/eventi_modena.json")
events = pd.DataFrame(data_scraped['items'])

# categoria_evento => è una lista
# city => alcune volte è vuoto
# descrizione_destinatari => va letto meglio
# descrizione_estesa => va pulito 
# image => pulire
# geolocaton => pulire
# orari => pulire
# organizzato_da_esterno => pulire
# prezzo => pulire
# ulteriori_informazioni => pulire
#filter=['UID', '@id','categoria_evento','city','street','created',
#'description','descrizione_estesa',
#'effective','email','start','end','geolocation','image','image_caption','modified','nome_sede',
#'orari','organizzato_da_esterno','patrocinato_da','prezzo','reperibilita','telefono','title',
#'ulteriori_informazioni','web','whole_day','zip_code']

filter=['@id','categoria_evento','city','street','created',
'description','effective','email','start','end','geolocation','image','image_caption','modified','nome_sede',
'orari','patrocinato_da','prezzo','reperibilita','telefono','title',
'ulteriori_informazioni','web','whole_day','zip_code']

events = events[filter]
events['pagina_web'] = events['@id'].apply(lambda x: x.replace('/api',""))
del events['@id']
events['cap'] = events.zip_code.apply(lambda x: 41123 if(x == None) else x)
del events['cap']

events= events.rename(columns={'title':'nome',"street":"via","zip_code":"cap","modified":"data_ultima_modifica"})
events= events.rename(columns={'city':'città',"street":"via","description":"descrizione","created":"data_creazione","end":"fine","start":"inizio"})
events= events.rename(columns={'image':'immagine','whole_day':'giornata_intera'})
events= events.rename(columns={'ulteriori_informazioni':'extrainfo'})


events['latitudine']= events.geolocation.apply(lambda x:  x['latitude'] if(x != None) else x)
events['longitudine']= events.geolocation.apply(lambda x:  x['longitude'] if(x != None) else x)
events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == None) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == None) else x)
events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == 0) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == 0) else x)
events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(pd.isna(x)) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(pd.isna(x)) else x)
del events['geolocation']

def categoriaEvento(e):
    categoria=""
    for c in e:
        categoria = categoria + "," + c 
    categoria = categoria.lstrip(",")
    return categoria
events['categoria_evento'] = events['categoria_evento'].apply(lambda x: categoriaEvento(x))

def desc(value):
    desc = ""
    try:
        for i in range(len(value['blocks'])):
            for k in list(value['blocks'].keys()):
                bk= value['blocks'][k]
                for b in bk['text']['blocks']:
                    ndesc = b['text']
                    desc = ndesc + " " + desc #+ " "+ ndesc
    except KeyError:
        pass
    desc = desc.replace(";"," ")
    desc = re.sub(r'((\b\w+\b.{1,2}\w+\b)+).+\1', r'\1', desc, flags = re.I)
    return desc

def removetxtduplicate(text):
    if text is not None:
        text = re.sub(r'((\b\w+\b.{1,2}\w+\b)+).+\1', r'\1', text, flags = re.I)
    return(text)

events['extrainfo'] = events['extrainfo'].apply(lambda x: desc(x))
events['immagine'] = events['immagine'].apply(lambda x: x['download'])
events.rename(columns={'image_caption':'desc_img'},inplace=True)  
events.rename(columns={'effective':'data_pubblicazione'},inplace=True)     
events['prezzo'] = events['prezzo'].apply(lambda x: desc(x)) 
events['orari'] = events['orari'].apply(lambda x: desc(x))


events.web = events.web.apply(lambda x: "" if (str(x) == "[]") else x)
events.web = events.web.apply(lambda x: "" if (x == None) else x)

events['descrizione'] = events.descrizione.str.lstrip("\n")
events['descrizione'] = events.descrizione.str.lstrip(" ")
events['descrizione'] = events.descrizione.str.replace(" \xa0 ","").replace("\n\n","\n").replace(";"," ")
events['descrizione'] = events.descrizione.str.replace("\n"," ").replace("\t"," ").replace("\r","")
events['descrizione'] = events.descrizione.str.lstrip(" ")
events['descrizione'] = events.descrizione.str.lstrip(" ")
events['descrizione'] = events.descrizione.str.replace("“","").replace("”","")

events["orari"] = events['orari'].str.lstrip(" ")
events["orari"] = events['orari'].str.lstrip("\n").replace(";"," ").replace("\n"," ").replace("\t"," ").replace("\r","")
events['orari'] = events.orari.str.replace("“","").replace("”","")


events["prezzo"] = events['prezzo'].str.lstrip(" ").replace(";"," ")
events["prezzo"] = events['prezzo'].str.lstrip("\n").replace("\n"," ").replace("\t"," ").replace("\r"," ")
events['prezzo'] = events.prezzo.str.replace("“","").replace("”","")

events.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

events["extrainfo"] = events['extrainfo'].apply(lambda x: removetxtduplicate(x))
events["prezzo"] = events['prezzo'].apply(lambda x: removetxtduplicate(x))
events["descrizione"] = events['descrizione'].apply(lambda x: removetxtduplicate(x))
events["categoria_evento"] = events['categoria_evento'].apply(lambda x: removetxtduplicate(x))
events["città"] = events['città'].apply(lambda x: removetxtduplicate(x))
events["data_creazione"] = events['data_creazione'].apply(lambda x: removetxtduplicate(x))
events["data_pubblicazione"] = events['data_pubblicazione'].apply(lambda x: removetxtduplicate(x))
events["email"] = events['email'].apply(lambda x: removetxtduplicate(x))
events["desc_img"] = events['desc_img'].apply(lambda x: removetxtduplicate(x))
events["data_ultima_modifica"] = events['data_ultima_modifica'].apply(lambda x: removetxtduplicate(x))
events["nome_sede"] = events['nome_sede'].apply(lambda x: removetxtduplicate(x))
events["orari"] = events['orari'].apply(lambda x: removetxtduplicate(x))
events["prezzo"] = events['prezzo'].apply(lambda x: removetxtduplicate(x))
events["reperibilita"] = events['reperibilita'].apply(lambda x: removetxtduplicate(x))
events["telefono"] = events['telefono'].apply(lambda x: removetxtduplicate(x))
events["nome"] = events['nome'].apply(lambda x: removetxtduplicate(x))
events["web"] = events['web'].apply(lambda x: removetxtduplicate(x))
events["cap"] = events['cap'].apply(lambda x: removetxtduplicate(x))

events.to_csv("docs/eventi/eventi_modena.csv",sep=";",index=False)
events.to_csv("docs/eventi/eventi_modena.tsv",sep="\t",index=False,line_terminator="\r\n")
events.to_csv("docs/eventi/eventi_modena_pipe.csv",sep="|",index=False,line_terminator="\r\n")

geo_events = gpd.GeoDataFrame(
    events, geometry=gpd.points_from_xy(events['longitudine'], events['latitudine']))


geo_events.set_crs(4326,inplace=True)
os.chdir("docs/eventi")
geo_events.to_file("eventi_modena.shp",encoding='utf-8')
zipObj = ZipFile('eventi_modena.zip', 'w')
zipObj.write('eventi_modena.shp')
zipObj.write('eventi_modena.shx')
zipObj.write('eventi_modena.prj')
zipObj.write('eventi_modena.dbf')
zipObj.close()

events.longitudine = events.longitudine.apply(lambda x: str(x).replace(".",","))
events.latitudine = events.latitudine.apply(lambda x: str(x).replace(".",","))

events.to_csv("eventi_modena_coordinate_con_virgola.csv",sep=";",index=False)

events.to_excel("eventi_modena.xlsx",index=False, sheet_name="eventi")

os.chdir("../..")


servizi = "https://www.comune.modena.it/api/@search?portal_type=UnitaOrganizzativa&path.query=/amministrazione/aree-amministrative&path.depth=2&fullobjects=1&b_size=10000"
r = requests.get(servizi, headers=headers)
r.encoding = 'UTF-8'
data = r.json()
data_items = pd.DataFrame(data['items'])

filter=['@id','title','street','zip_code','email','telefono','fax','geolocation',
'legami_con_altre_strutture',
'orario_pubblico','pec','web',
'competenze','modified']

table = data_items[filter]
table= table.rename(columns={'title':'nome',"street":"via","zip_code":"cap","modified":"data_ultima_modifica"})
table= table.rename(columns={'effective':'data_pubblicazione'})
table['latitudine']= table.geolocation.apply(lambda x:  x['latitude'] if(x != None) else x)
table['longitudine']= table.geolocation.apply(lambda x:  x['longitude'] if(x != None) else x)
del table['geolocation']

def extractOrarioPubblico(value):
    orario = ""
    try:
        for i in range(len(value['blocks'])):
            for k in list(value['blocks'].keys()):
                bk= value['blocks'][k]
                for b in bk['text']['blocks']:
                    orario = orario + " "+ b['text']
    except KeyError:
        pass
    return orario

table['orario_al_pubblico'] = table.orario_pubblico.apply(lambda x:  extractOrarioPubblico(x))
del table['orario_pubblico']
table['cap'] = table.cap.apply(lambda x: 41123 if(x == None) else x)
table['fax'] = table.fax.apply(lambda x: x.lstrip("\t") if(x != None) else x)

def extractCompetenze(value):
    competenze = ""
    try:
        for i in range(len(value['blocks'])):
            for k in list(value['blocks'].keys()):
                bk= value['blocks'][k]
                for b in bk['text']['blocks']:
                    competenze = competenze + "||"+ b['text']
    except KeyError:
        pass
    competenze = competenze.lstrip("||")
    return competenze

table['competenze'] = table.competenze.apply(lambda x:  extractCompetenze(x))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("\n"," "))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("\xa0"," "))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("|| ","||"))

#def getPosizioneOrganizzativa(value):
#    v = ""
#    if value.find("Posizione Organizzativa: ") > 0:
##        v = value.split('Posizione Organizzativa: ')[1].split("||")[0]
#    v = v.lstrip("\n")
#    v = v.rstrip("\n")
#    return v

nomi = list(table['nome'].unique())

df_competenze = pd.DataFrame()
for nome in nomi:
    data_competenze = {}
    competenze = table[table['nome'] == nome]['competenze'].values[0].split("||")
    nome_data = []
    for i in range(len(competenze)):
        nome_data.append(nome)
    df = pd.DataFrame({'nome':nome_data,'competenze':competenze}).drop_duplicates()
    df['competenze'] = df['competenze'].apply(lambda x: x.lstrip())
    df['competenze'] = df['competenze'].apply(lambda x: x.rstrip())
    df_competenze = pd.concat([df_competenze,df])

df_competenze = df_competenze[df_competenze['competenze'] != 'Competenze']
del table['competenze']
table['pagina_web'] = table['@id'].apply(lambda x: x.replace('/api',""))
del table['@id']

def getLegamiAltreStrutture(value):
    v = ""
    if (value != ""):
        for t in value:
            v = v +"||" + t['title']
    v = v.lstrip("||")
    return v

table['legami'] = table.legami_con_altre_strutture.apply(lambda x: getLegamiAltreStrutture(x))

df_legami = pd.DataFrame()
for nome in nomi:
    data_legami = {}
    legami = table[table['nome'] == nome]['legami'].values[0].split("||")
    nome_data = []
    for i in range(len(legami)):
        nome_data.append(nome)
    df = pd.DataFrame({'struttura':nome_data,'struttura_collegata':legami}).drop_duplicates()
    df_legami = pd.concat([df_legami,df])

df_legami = df_legami[df_legami['struttura_collegata'] != '']

del table['legami_con_altre_strutture']
del table['legami']

def completeLat(id,df):
    row = df[df['nome'] == id]
    lat = row['latitudine'].values[0]
    if (lat == 0):
        v = row['via'].values[0]
        if v == "Via Santi, 60":
            lat = '44.655952' #	10.915423
        if v == 'Via Galaverna, 8':
            lat = '44.655039'
    return(lat)
def completeLon(id,df):
    row = df[df['nome'] == id]
    lon = row['longitudine'].values[0]
    if (lon == 0):
        v = row['via'].values[0]
        if v == "Via Santi, 60":
            lon = '10.915423'
        if v == 'Via Galaverna, 8':
            lon = '10.914606'
    return(lon)

table['latitudine'] = table.nome.apply(lambda x: completeLat(x,table))
table['longitudine'] = table.nome.apply(lambda x: completeLon(x,table))

table.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
df_legami.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
df_competenze.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

table.to_csv("docs/strutture/elenco_strutture.csv",sep=";",index=False,encoding="utf-8")
df_legami.to_csv("docs/strutture/relazioni_fra_strutture.csv",sep=";",index=False,encoding="utf-8")
df_competenze.to_csv("docs/strutture/competenze_strutture.csv",sep=";",index=False,encoding="utf-8")