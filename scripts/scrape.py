#!/usr/bin/env python
# coding: utf-8

# In[41]:


import warnings
import pandas as pd
import requests
import geopandas as gpd
from datetime import date
from zipfile import ZipFile
import os
warnings.filterwarnings("ignore")


# In[42]:


intestazione = {'Accept': 'application/json'}
url_api = "https://www.comune.modena.it/api/"
url_search = url_api + "@querystring-search"


# In[43]:


def getParams(from_day, to_day):
	parametri = {
		"b_size": 100000,
		"fullobjects": 1,
		"query": [{
				"i": "portal_type",
					"o": "plone.app.querystring.operation.selection.any",
					"v": ["Event"]
		}, {
				"i": "start",
					"o": "plone.app.querystring.operation.date.largerThan",
					"v": from_day
		}, {
				"i": "end",
				"o": "plone.app.querystring.operation.date.lessThan",
				"v": to_day
		}
			]
	}
	return(parametri)


# In[44]:


da_anno = "2019"
ultimo_giorno_anno = "-12-31 00:00"
primo_giorno_anno = "-01-31 00:00"


# In[45]:


oggi = date.today()


# In[46]:


anno_attuale = str(oggi.year)
mese_attuale = str(oggi.month)
giorno_attuale = str(oggi.day)


# In[47]:


def recuperaDati(url_search,intestazione,parametri):
    richiesta = requests.post(url_search, headers=intestazione,json=parametri)
    richiesta.encoding = 'utf-8'
    dati = richiesta.json()
    return(pd.DataFrame(dati['items']))


# In[48]:


eventi_df = []
for anno in range(2019,int(anno_attuale)+1):
    da = str(anno)+primo_giorno_anno
    a = str(anno)+ultimo_giorno_anno
    data = recuperaDati(url_search,intestazione,getParams(da,a))
    eventi_df.append(data)


# In[49]:


eventi = None
for i in range(len(eventi_df)):
    if i == 0:
        eventi = eventi_df[0]
    else:
        eventi = eventi.append(eventi_df[i])


# In[50]:


filter=['@id','categoria_evento','city','street','created',
'description','effective','email','start','end','geolocation','image','image_caption','modified','nome_sede',
'orari','patrocinato_da','prezzo','reperibilita','telefono','title',
'ulteriori_informazioni','web','whole_day','zip_code']


# In[51]:


eventi = eventi[filter]
eventi['pagina_web'] = eventi['@id'].apply(lambda x: x.replace('/api', ""))
del eventi['@id']
eventi['cap'] = eventi.zip_code.apply(lambda x: 41123 if(x == None) else x)
del eventi['cap']


# In[52]:


eventi = eventi.rename(columns={'title': 'nome', "street": "via",
                       "zip_code": "cap", "modified": "data_ultima_modifica"})
eventi = eventi.rename(columns={'city': 'città', "street": "via", "description": "descrizione",
                       "created": "data_creazione", "end": "fine", "start": "inizio"})
eventi = eventi.rename(
    columns={'image': 'immagine', 'whole_day': 'giornata_intera'})
eventi = eventi.rename(columns={'ulteriori_informazioni': 'extrainfo'})
#events=events.rename(columns={'organizzato_da_esterno':'org_esterna'})
#events=events.rename(columns={"descrizione_estesa":"desc_estesa"})


# In[53]:


eventi['latitudine'] = eventi.geolocation.apply(
    lambda x:  x['latitude'] if(x != None) else x)
eventi['longitudine'] = eventi.geolocation.apply(
    lambda x:  x['longitude'] if(x != None) else x)
eventi['longitudine'] = eventi.longitudine.apply(
    lambda x: 10.92572 if(x == None) else x)
eventi['latitudine'] = eventi.latitudine.apply(
    lambda x: 44.64582 if(x == None) else x)
eventi['longitudine'] = eventi.longitudine.apply(
    lambda x: 10.92572 if(x == 0) else x)
eventi['latitudine'] = eventi.latitudine.apply(
    lambda x: 44.64582 if(x == 0) else x)
eventi['longitudine'] = eventi.longitudine.apply(
    lambda x: 10.92572 if(pd.isna(x)) else x)
eventi['latitudine'] = eventi.latitudine.apply(
    lambda x: 44.64582 if(pd.isna(x)) else x)
del eventi['geolocation']


# In[54]:


def categoriaEvento(e):
    categoria = ""
    for c in e:
        categoria = categoria + "," + c
    categoria = categoria.lstrip(",")
    return categoria


eventi['categoria_evento'] = eventi['categoria_evento'].apply(
    lambda x: categoriaEvento(x))


# In[55]:


def desc(value):
    desc = ""
    try:
        for i in range(len(value['blocks'])):
            for k in list(value['blocks'].keys()):
                bk = value['blocks'][k]
                for b in bk['text']['blocks']:
                    desc = desc + " " + b['text']
    except KeyError:
        pass
    desc = desc.replace(";", " ")
    return desc


# In[56]:

def immagineDownload(row):
    download = ""
    try:
        download = row['download']
    except Exception as e:
        print (e)
        pass
    return(download)
        

eventi['extrainfo'] = eventi['extrainfo'].apply(lambda x: desc(x))
#eventi['immagine'] = eventi['immagine'].apply(lambda x: x['download'])
eventi['immagine'] = eventi['immagine'].apply(lambda x: immagineDownload(x))
eventi.rename(columns={'image_caption': 'desc_img'}, inplace=True)
eventi.rename(columns={'effective': 'data_pubblicazione'}, inplace=True)
eventi['prezzo'] = eventi['prezzo'].apply(lambda x: desc(x))
eventi['orari'] = eventi['orari'].apply(lambda x: desc(x))


# In[57]:


eventi['cap'] = eventi.cap.apply(lambda x: 41123 if(x == None) else x)


# In[58]:


eventi.web = eventi.web.apply(lambda x: "" if (str(x) == "[]") else x)
eventi.web = eventi.web.apply(lambda x: "" if (x == None) else x)


# In[59]:


eventi['città'] = eventi['città'].apply(lambda x: "Modena" if(x == None) else x)


# In[60]:


eventi.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)


# In[61]:


eventi.drop_duplicates(inplace=True)


# In[73]:


eventi.to_csv("docs/eventi/eventi_modena.csv",sep=";",index=False)
#eventi.to_csv("eventi_modena.csv",sep=";",index=False)


# In[74]:


eventi.to_csv("docs/eventi/eventi_modena.tsv",sep="\t",index=False,line_terminator="\r\n")


# In[75]:


geo_events = gpd.GeoDataFrame(
    eventi, geometry=gpd.points_from_xy(eventi['longitudine'], eventi['latitudine']))


# In[76]:


geo_events.set_crs(4326, inplace=True)
os.chdir("docs/eventi")
geo_events.to_file("eventi_modena.shp", encoding='utf-8')
zipObj = ZipFile('eventi_modena.zip', 'w')
zipObj.write('eventi_modena.shp')
zipObj.write('eventi_modena.shx')
zipObj.write('eventi_modena.prj')
zipObj.write('eventi_modena.dbf')
zipObj.close()


# In[77]:


eventi.longitudine = eventi.longitudine.apply(lambda x: str(x).replace(".",","))
eventi.latitudine = eventi.latitudine.apply(lambda x: str(x).replace(".",","))


# In[78]:


eventi.to_csv("eventi_modena_coordinate_con_virgola.csv",sep=";",index=False)


# In[79]:


eventi.to_excel("eventi_modena.xlsx",index=False, sheet_name="eventi")

