#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


# In[2]:


url="https://www.comune.modena.it/api/@querystring-search"


# In[3]:


subdata = {"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]}
subdata_2 = {"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":"2021-12-20 00:00"}
d = [subdata,subdata_2]


# In[4]:


data={'fullobjects': '1',"query":d}


# In[5]:


r = requests.post(url, headers=headers,json={"b_size":10000,"fullobjects":1,"query":[{"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]},{"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":"2019-01-01 00:00"}]})


# In[6]:


r.encoding


# In[7]:


#r.encoding = 'ISO 8859-1'
r.encoding = 'utf-8'
data_scraped = r.json()


# In[8]:


events = pd.DataFrame(data_scraped['items'])


# In[9]:


#events['zip_code'].unique()


# In[10]:


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
filter=['UID', '@id','categoria_evento','city','street','created',
'description','descrizione_estesa',
'effective','email','start','end','geolocation','image','image_caption','modified','nome_sede',
'orari','organizzato_da_esterno','patrocinato_da','prezzo','reperibilita','telefono','title',
'ulteriori_informazioni','web','whole_day','zip_code']


# In[11]:


events = events[filter]


# In[12]:


events['pagina_web'] = events['@id'].apply(lambda x: x.replace('/api',""))


# In[13]:


del events['@id']


# In[14]:


events['cap'] = events.zip_code.apply(lambda x: 41123 if(x == None) else x)


# In[15]:


del events['cap']


# In[16]:


events= events.rename(columns={'title':'nome',"street":"via","zip_code":"cap","modified":"data_ultima_modifica"})


# In[17]:


events= events.rename(columns={'city':'città',"street":"via","description":"descrizione","created":"data_creazione","end":"fine","start":"inizio"})


# In[18]:


events= events.rename(columns={'image':'immagine','whole_day':'giornata_intera'})


# In[19]:


events['latitudine']= events.geolocation.apply(lambda x:  x['latitude'] if(x != None) else x)


# In[20]:


events['longitudine']= events.geolocation.apply(lambda x:  x['longitude'] if(x != None) else x)


# In[21]:


events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == None) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == None) else x)


# In[22]:


events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == 0) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == 0) else x)


# In[23]:


del events['geolocation']


# In[24]:


def categoriaEvento(e):
    categoria=""
    for c in e:
        categoria = categoria + "," + c 
    categoria = categoria.lstrip(",")
    return categoria


# In[25]:


events['categoria_evento'] = events['categoria_evento'].apply(lambda x: categoriaEvento(x))


# In[26]:


def desc(value):
    desc = ""
    try:
        for i in range(len(value['blocks'])):
            for k in list(value['blocks'].keys()):
                bk= value['blocks'][k]
                for b in bk['text']['blocks']:
                    desc = desc + " "+ b['text']
    except KeyError:
        pass
    desc = desc.replace(";"," ")
    return desc  


# In[27]:


events['descrizione_estesa'] = events['descrizione_estesa'].apply(lambda x: desc(x))


# In[28]:


events['ulteriori_informazioni'] = events['ulteriori_informazioni'].apply(lambda x: desc(x))


# In[29]:


events['immagine'] = events['immagine'].apply(lambda x: x['download'])


# In[30]:


events.rename(columns={'image_caption':'descrizione_immagine'},inplace=True)    


# In[31]:


events.rename(columns={'effective':'data_pubblicazione'},inplace=True)    


# In[32]:


events['prezzo'] = events['prezzo'].apply(lambda x: desc(x))


# In[33]:


events['orari'] = events['orari'].apply(lambda x: desc(x))


# In[34]:


events['organizzato_da_esterno'] = events['organizzato_da_esterno'].apply(lambda x: desc(x))


# In[35]:


events.descrizione_estesa = events.descrizione_estesa.apply(lambda x: x.lstrip("\n"))


# In[36]:


events.web = events.web.apply(lambda x: "" if (str(x) == "[]") else x)


# In[37]:


events.web = events.web.apply(lambda x: "" if (x == None) else x)


# In[38]:


events['descrizione'] = events.descrizione.str.lstrip("\n")


# In[39]:


events['descrizione'] = events.descrizione.str.lstrip(" ")


# In[40]:


events['descrizione'] = events.descrizione.str.replace(" \xa0 ","").replace("\n\n","\n").replace(";"," ")


# In[41]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.replace("\n\n","\n").replace(";"," ")


# In[42]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip("\n")  


# In[43]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip(" \n ")  


# In[44]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip(" ")


# In[45]:


events["orari"] = events['orari'].str.lstrip(" ")
events["orari"] = events['orari'].str.lstrip("\n").replace(";"," ")


# In[46]:


events["organizzato_da_esterno"] = events['organizzato_da_esterno'].str.lstrip(" ").replace(";"," ")
events["organizzato_da_esterno"] = events['organizzato_da_esterno'].str.lstrip("\n")


# In[47]:


events["prezzo"] = events['prezzo'].str.lstrip(" ").replace(";"," ")
events["prezzo"] = events['prezzo'].str.lstrip("\n")


# In[48]:


events.to_csv("docs/eventi/eventi_modena.csv",sep=";",index=False)


# In[49]:


events.longitudine = events.longitudine.apply(lambda x: str(x).replace(".",","))
events.latitudine = events.latitudine.apply(lambda x: str(x).replace(".",","))


# In[50]:


events.to_csv("docs/eventi/eventi_modena_coordinate_con_virgola.csv",sep=";",index=False)

