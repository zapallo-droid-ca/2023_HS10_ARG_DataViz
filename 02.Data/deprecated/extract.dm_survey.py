### --- UN COMTRADE DB PIPELINE: Extract - Dimensions

##-- Environment Setting
import os
import datetime as dt #the updateTime and Zone could be usefull in those cases where a data stage db would be needed
import time
import numpy as np

import comtradeapicall as cmtp

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/02.Data/02.Pipeline/'
wd_out = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/02.Data/02.Pipeline/in'


os.chdir(wd)

#Credentials
wd_sec = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/00.Secure/'

subscription_key = open(wd_sec + '/unComtradeKey.txt').read()

#General Settings
csvAttr = {'index': False, 'sep': ';' , 'encoding': 'UTF-8'} #csv settings


#------#------#------#------#------#
##-- DATA EXTRACTION
aux_references = cmtp.listReference()

#DIM COUNTRY
df_partner = cmtp.getReference('partner')
df_partner['updateTime'] = dt.datetime.now()
df_partner['updateZone'] = time.tzname[0]

df_reporter = cmtp.getReference('reporter')
df_reporter['updateTime'] = dt.datetime.now()
df_reporter['updateZone'] = time.tzname[0]

#DIM FREQUENCY
df_frequency_ori = cmtp.getReference('freq')
df_frequency_ori['updateTime'] = dt.datetime.now()
df_frequency_ori['updateZone'] = time.tzname[0]

#DIM FLOW
df_flow = cmtp.getReference('flow')
df_flow['updateTime'] = dt.datetime.now()
df_flow['updateZone'] = time.tzname[0]

#DIM COMMERCIAL UNITS
df_units = cmtp.getReference('qtyunit')
df_units['updateTime'] = dt.datetime.now()
df_units['updateZone'] = time.tzname[0]

#DIM MODE OF TRANSPORTS
df_mot = cmtp.getReference('mot')
df_mot['updateTime'] = dt.datetime.now()
df_mot['updateZone'] = time.tzname[0]

#DIM MODE OF TRANSPORTS
df_customs = cmtp.getReference('customs')
df_customs['updateTime'] = dt.datetime.now()
df_customs['updateZone'] = time.tzname[0]

#DIM CLASSIFICATION FOR GOODS
df_classificationHS = cmtp.getReference('cmd:HS')
df_classificationHS['updateTime'] = dt.datetime.now()
df_classificationHS['updateZone'] = time.tzname[0]

df_classificationSS = cmtp.getReference('cmd:SS')
df_classificationSS['updateTime'] = dt.datetime.now()
df_classificationSS['updateZone'] = time.tzname[0]


#DIM CLASSIFICATION FOR SERVICES // Multiple Classifications
df_classificationEB = cmtp.getReference('cmd:EB10')
df_classificationEB['updateTime'] = dt.datetime.now()
df_classificationEB['updateZone'] = time.tzname[0]


df_partner.to_csv(wd_out + 'df_partner_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_reporter.to_csv(wd_out + 'df_reporter_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_frequency_ori.to_csv(wd_out + 'df_frequency_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_flow.to_csv(wd_out + 'df_flow_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_units.to_csv(wd_out + 'df_units_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_mot.to_csv(wd_out + 'df_mot_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_customs.to_csv(wd_out + 'df_customs_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_classificationHS.to_csv(wd_out + 'df_classificationHS_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
df_classificationEB.to_csv(wd_out + 'df_classificationEB_ori.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])
aux_references.to_csv(wd_out + 'aux_references_unComtrade.csv', index = csvAttr['index'], sep = csvAttr['sep'] , encoding = csvAttr['encoding'])



