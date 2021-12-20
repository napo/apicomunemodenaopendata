#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from datetime import date
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


# In[2]:


today = date.today()
today_str = today.strftime("%Y-%m-%d")
today_str += " 00:00"


# In[3]:


url="https://www.comune.modena.it/api/@querystring-search"


# In[4]:


subdata = {"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]}
subdata_2 = {"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":today_str}
d = [subdata,subdata_2]


# In[5]:


data={'fullobjects': '1',"query":d}


# In[6]:


r = requests.post(url, headers=headers,json={"b_size":10000,"fullobjects":1,"query":[{"i":"portal_type","o":"plone.app.querystring.operation.selection.any","v":["Event"]},{"i":"start","o":"plone.app.querystring.operation.date.largerThan","v":"2019-01-01 00:00"}]})


# In[7]:


r.encoding


# In[8]:


#r.encoding = 'ISO 8859-1'
r.encoding = 'utf-8'
data_scraped = r.json()


# In[9]:


events = pd.DataFrame(data_scraped['items'])


# In[10]:


#events['zip_code'].unique()


# In[11]:


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


# In[12]:


events = events[filter]


# In[13]:


events['pagina_web'] = events['@id'].apply(lambda x: x.replace('/api',""))


# In[14]:


del events['@id']


# In[15]:


events['cap'] = events.zip_code.apply(lambda x: 41123 if(x == None) else x)


# In[16]:


del events['cap']


# In[17]:


events= events.rename(columns={'title':'nome',"street":"via","zip_code":"cap","modified":"data_ultima_modifica"})


# In[18]:


events= events.rename(columns={'city':'città',"street":"via","description":"descrizione","created":"data_creazione","end":"fine","start":"inizio"})


# In[19]:


events= events.rename(columns={'image':'immagine','whole_day':'giornata_intera'})


# In[20]:


events['latitudine']= events.geolocation.apply(lambda x:  x['latitude'] if(x != None) else x)


# In[21]:


events['longitudine']= events.geolocation.apply(lambda x:  x['longitude'] if(x != None) else x)


# In[22]:


events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == None) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == None) else x)


# In[23]:


events['longitudine']= events.longitudine.apply(lambda x: 10.92572 if(x == 0) else x)
events['latitudine']= events.latitudine.apply(lambda x: 44.64582 if(x == 0) else x)


# In[24]:


del events['geolocation']


# In[25]:


def categoriaEvento(e):
    categoria=""
    for c in e:
        categoria = categoria + "," + c 
    categoria = categoria.lstrip(",")
    return categoria


# In[26]:


events['categoria_evento'] = events['categoria_evento'].apply(lambda x: categoriaEvento(x))


# In[27]:


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


# In[28]:


events['descrizione_estesa'] = events['descrizione_estesa'].apply(lambda x: desc(x))


# In[29]:


events['ulteriori_informazioni'] = events['ulteriori_informazioni'].apply(lambda x: desc(x))


# In[30]:


events['immagine'] = events['immagine'].apply(lambda x: x['download'])


# In[31]:


events.rename(columns={'image_caption':'descrizione_immagine'},inplace=True)    


# In[32]:


events.rename(columns={'effective':'data_pubblicazione'},inplace=True)    


# In[33]:


events['prezzo'] = events['prezzo'].apply(lambda x: desc(x))


# In[34]:


events['orari'] = events['orari'].apply(lambda x: desc(x))


# In[35]:


events['organizzato_da_esterno'] = events['organizzato_da_esterno'].apply(lambda x: desc(x))


# In[36]:


events.descrizione_estesa = events.descrizione_estesa.apply(lambda x: x.lstrip("\n"))


# In[37]:


events.web = events.web.apply(lambda x: "" if (str(x) == "[]") else x)


# In[38]:


events.web = events.web.apply(lambda x: "" if (x == None) else x)


# In[39]:


events['descrizione'] = events.descrizione.str.lstrip("\n")


# In[40]:


events['descrizione'] = events.descrizione.str.lstrip(" ")


# In[41]:


events['descrizione'] = events.descrizione.str.replace(" \xa0 ","").replace("\n\n","\n").replace(";"," ")


# In[42]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.replace("\n\n","\n").replace(";"," ")


# In[43]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip("\n")  


# In[44]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip(" \n ")  


# In[45]:


events["descrizione_estesa"] = events['descrizione_estesa'].str.lstrip(" ")


# In[46]:


events["orari"] = events['orari'].str.lstrip(" ")
events["orari"] = events['orari'].str.lstrip("\n").replace(";"," ")


# In[47]:


events["organizzato_da_esterno"] = events['organizzato_da_esterno'].str.lstrip(" ").replace(";"," ")
events["organizzato_da_esterno"] = events['organizzato_da_esterno'].str.lstrip("\n")


# In[48]:


events["prezzo"] = events['prezzo'].str.lstrip(" ").replace(";"," ")
events["prezzo"] = events['prezzo'].str.lstrip("\n")


# In[49]:


events.to_csv("eventi_modena.csv",sep=";",index=False)


# In[50]:


events.longitudine = events.longitudine.apply(lambda x: str(x).replace(".",","))
events.latitudine = events.latitudine.apply(lambda x: str(x).replace(".",","))


# In[51]:


events.to_csv("eventi_modena_coordinate_con_virgola.csv",sep=";",index=False)


# In[52]:


servizi = "https://www.comune.modena.it/api/@search?portal_type=UnitaOrganizzativa&path.query=/amministrazione/aree-amministrative&path.depth=2&fullobjects=1&b_size=10000"
r = requests.get(servizi, headers=headers)
r.encoding = 'UTF-8'
data = r.json()
data_items = pd.DataFrame(data['items'])
filter = ['@id', 'competenze',
        'created', 'effective', 'email', 'fax',
        'geolocation', 'id', 'legami_con_altre_strutture',
        'modified', 'orario_pubblico', 
        'pec', 'street', 'telefono', 'title',
        'web', 'zip_code']


# In[53]:


table = data_items[filter]
table= table.rename(columns={'title':'nome',"street":"via","zip_code":"cap","modified":"data_ultima_modifica"})
table= table.rename(columns={'effective':'data_pubblicazione'})
table['latitudine']= table.geolocation.apply(lambda x:  x['latitude'] if(x != None) else x)
table['longitudine']= table.geolocation.apply(lambda x:  x['longitude'] if(x != None) else x)
del table['geolocation']


# In[54]:


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


# In[55]:


table['orario_al_pubblico'] = table.orario_pubblico.apply(lambda x:  extractOrarioPubblico(x))
del table['orario_pubblico']
table['cap'] = table.cap.apply(lambda x: 41123 if(x == None) else x)
table['fax'] = table.fax.apply(lambda x: x.lstrip("\t") if(x != None) else x)


# In[56]:


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


# In[57]:


table['competenze'] = table.competenze.apply(lambda x:  extractCompetenze(x))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("\n"," "))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("\xa0"," "))
table['competenze'] = table.competenze.apply(lambda x:  x.replace("|| ","||"))


# In[58]:


def getPosizioneOrganizzativa(value):
    v = ""
    if value.find("Posizione Organizzativa: ") > 0:
        v = value.split('Posizione Organizzativa: ')[1].split("||")[0]
    v = v.lstrip("\n")
    v = v.rstrip("\n")
    return v


# In[59]:


nomi = list(table['nome'].unique())


# In[60]:


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


# In[61]:


df_competenze = df_competenze[df_competenze['competenze'] != 'Competenze']
del table['competenze']
table['pagina_web'] = table['@id'].apply(lambda x: x.replace('/api',""))
del table['@id']


# In[62]:


def getLegamiAltreStrutture(value):
    v = ""
    if (value != ""):
        for t in value:
            v = v +"||" + t['title']
    v = v.lstrip("||")
    return v


# In[63]:


table['legami'] = table.legami_con_altre_strutture.apply(lambda x: getLegamiAltreStrutture(x))


# In[64]:


df_legami = pd.DataFrame()
for nome in nomi:
    data_legami = {}
    legami = table[table['nome'] == nome]['legami'].values[0].split("||")
    nome_data = []
    for i in range(len(legami)):
        nome_data.append(nome)
    df = pd.DataFrame({'struttura':nome_data,'struttura_collegata':legami}).drop_duplicates()
    df_legami = pd.concat([df_legami,df])


# In[65]:


df_legami = df_legami[df_legami['struttura_collegata'] != '']


# In[66]:


del table['legami_con_altre_strutture']
del table['legami']


# In[67]:


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


# In[68]:


table['latitudine'] = table.nome.apply(lambda x: completeLat(x,table))
table['longitudine'] = table.nome.apply(lambda x: completeLon(x,table))


# In[69]:


table.to_csv("docs/strutture/elenco_strutture.csv",sep=";",index=False)
df_legami.to_csv("docs/strutture/relazioni_fra_strutture.csv",sep=";",index=False)
df_competenze.to_csv("docs/strutture/competenze_strutture.csv",sep=";",index=False)

