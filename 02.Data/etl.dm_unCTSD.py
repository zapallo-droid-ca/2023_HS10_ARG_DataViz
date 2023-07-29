### --- UN COMTRADE DB PIPELINE: ETL - Dimensions

##-- Environment Setting
import pandas as pd
import numpy as np

import os
import datetime as dt #the updateTime and Zone could be usefull in those cases where a data stage db would be needed
import time

import comtradeapicall as cmtp

import pyodbc

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/'
wd_in = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/01.UN_CTSD/00.aux/'
wd_out = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/out/'

os.chdir(wd)

#Credentials
wd_sec = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/00.Secure/'

subscription_key = open(wd_sec + '/unComtradeKey.txt').read()

serverConnectString = open(wd_sec + '/serverConnectionSTG.txt').read()

#General Settings
csvAttr_imp = {'sep': ';' , 'encoding': 'UTF-8'} #csv settings - import
csvAttr_exp = {'index': False, 'sep': ';' , 'encoding': 'UTF-8'} #csv settings - export

code_lpadSide = 'left'
fillchar = '0'

updateMark = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
updateMark_TZ = time.tzname[0]

#------#------#------#------#------#
##-- DATA EXTRACTION
aux_references = cmtp.listReference()

#DIM COUNTRY
df_partner = cmtp.getReference('partner')
df_partner['updateMark'] = updateMark
df_partner['updateMark_TZ'] = updateMark_TZ

df_reporter = cmtp.getReference('reporter')
df_reporter['updateMark'] = updateMark
df_reporter['updateMark_TZ'] = updateMark_TZ

#DIM FREQUENCY
df_frequency_ori = cmtp.getReference('freq')
df_frequency_ori['updateMark'] = updateMark
df_frequency_ori['updateMark_TZ'] = updateMark_TZ

#DIM FLOW
df_flow = cmtp.getReference('flow')
df_flow['updateMark'] = updateMark
df_flow['updateMark_TZ'] = updateMark_TZ

#DIM COMMERCIAL UNITS
df_units = cmtp.getReference('qtyunit')
df_units['updateMark'] = updateMark
df_units['updateMark_TZ'] = updateMark_TZ

#DIM MODE OF TRANSPORTS
df_mot = cmtp.getReference('mot')
df_mot['updateMark'] = updateMark
df_mot['updateMark_TZ'] = updateMark_TZ

#DIM MODE OF TRANSPORTS
df_customs = cmtp.getReference('customs')
df_customs['updateMark'] = updateMark
df_customs['updateMark_TZ'] = updateMark_TZ

#DIM CLASSIFICATION FOR GOODS
df_classificationHS = cmtp.getReference('cmd:HS')
df_classificationHS['updateMark'] = updateMark
df_classificationHS['updateMark_TZ'] = updateMark_TZ

df_classificationSS = cmtp.getReference('cmd:SS')
df_classificationSS['updateMark'] = updateMark
df_classificationSS['updateMark_TZ'] = updateMark_TZ


#DIM CLASSIFICATION FOR SERVICES // Multiple Classifications
df_classificationEB = cmtp.getReference('cmd:EB10')
df_classificationEB['updateMark'] = updateMark
df_classificationEB['updateMark_TZ'] = updateMark_TZ

df_partner.to_csv(wd_in + 'df_partner_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_reporter.to_csv(wd_in + 'df_reporter_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_frequency_ori.to_csv(wd_in + 'df_frequency_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_flow.to_csv(wd_in + 'df_flow_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_units.to_csv(wd_in + 'df_units_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_mot.to_csv(wd_in + 'df_mot_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_customs.to_csv(wd_in + 'df_customs_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_classificationHS.to_csv(wd_in + 'df_classificationHS_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
df_classificationEB.to_csv(wd_in + 'df_classificationEB_ori.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
aux_references.to_csv(wd_in + 'aux_references_unComtrade.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])


#------#------#------#------#------#
##-- DATA TRANSFORMATION

#-DIM COUNTRY - concat of reporters and partners
dim_country_tempA = pd.read_csv(wd_in + '/df_reporter_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
dim_country_tempB = pd.read_csv(wd_in + '/df_partner_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])

#Dropping unnecessary columns
dim_country_tempA.drop(columns = ['updateMark','updateMark_TZ'], inplace = True)
dim_country_tempB.drop(columns = ['updateMark','updateMark_TZ'], inplace = True)

#Setting column names to lowercase
dim_country_tempA.columns = [x.lower() for x in dim_country_tempA.columns]
dim_country_tempB.columns = [x.lower() for x in dim_country_tempB.columns]

#Dropping redundand columns
dim_country_tempA = dim_country_tempA.drop(columns = ['reportercode','reporterdesc'])
dim_country_tempB = dim_country_tempB.drop(columns = ['partnercode','partnerdesc'])

#Standarizing column names
colVect = ['unComtrade_id','unComtrade_text','unComtrade_textNote','alpha2ISO','alpha3ISO',
           'unComtrade_entryDate','unComtrade_flagGroup','unComtrade_expiredDate']

dim_country_tempA.columns = colVect
dim_country_tempB.columns = colVect

#Creating an unique countries base
dim_country = pd.concat([dim_country_tempA,dim_country_tempB]).drop_duplicates().reset_index(drop = True)
dim_country.drop_duplicates(inplace = True)

#Keeping only non groups observations
dim_country = dim_country[dim_country['unComtrade_flagGroup'] == False].reset_index(drop = True)

#New Variable -- We are going to keep the duplicates on ISO codes just in case, it's importat to considere that unComtrade codes and ISO are not 1:1 but *:1
dim_country['extractCountryStatus'] = np.where(dim_country['unComtrade_expiredDate'].isna(), 'active', 'inactive' )


#The country names are dynamic, for these reasons it is possible to find several countries id with multiple names, so, the dim_country will be splitted in two tables,
#one with the stage (aux_country_stg) and other with only the active countries (dim_country)
aux_country_stg = dim_country.copy()

dim_country = dim_country[dim_country['extractCountryStatus'] == 'active'].reset_index(drop = True)
dim_country.drop(columns = ['unComtrade_entryDate','unComtrade_expiredDate','unComtrade_flagGroup','extractCountryStatus'], inplace = True)

codesToDrop = ['756','250','578','840']

dim_country = dim_country[dim_country['unComtrade_id'].astype(str).isin(codesToDrop) == False].reset_index(drop = True)

#Deletting redundand objects
del(dim_country_tempA,dim_country_tempB,colVect)


#-DIM CLASSIFICATION FOR GOODS
dim_classificationHS = pd.read_csv(wd_in + '/df_classificationHS_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
dim_classificationHS.aggrLevel.unique() #Check later in the formal pipeline it's ok (QA)

colVect = ['code','desc','code_ls','leave_flag','level_code','updateMark','updateMark_TZ']

dim_classificationHS.columns = colVect

dim_classificationHS['classificationDesc'] = 'HS'
dim_classificationHS['classificationType'] = 'Goods'
dim_classificationHS['typeCode'] = 'C'

dim_classificationHS['desc'] = dim_classificationHS['desc'].str.split(' - ').str[1:].str[0]

colVect = ['code','desc','code_ls','level_code','updateMark','updateMark_TZ','classificationDesc','classificationType','typeCode']

dim_classificationHS = dim_classificationHS[colVect].copy()

dim_classificationHS['level_code'].replace({6:3,
                                            4:2,
                                            2:1,
                                            0:0}, inplace = True)

dim_classHS_l1 = dim_classificationHS[dim_classificationHS['level_code'] == 1].reset_index(drop = True)
dim_classHS_l2 = dim_classificationHS[dim_classificationHS['level_code'] == 2].reset_index(drop = True)
dim_classHS_l3 = dim_classificationHS[dim_classificationHS['level_code'] == 3].reset_index(drop = True)


dim_classHS_l1['pk_l1'] = 'G-LVL' + dim_classHS_l1.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classHS_l1.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classHS_l1['un_pk_l0'] = dim_classHS_l1['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l1['code_ls']
dim_classHS_l1['un_pk_l1'] = dim_classHS_l1['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l1['code']

dim_classHS_l2['pk_l2'] = 'G-LVL' + dim_classHS_l2.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classHS_l2.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classHS_l2['un_pk_l1'] = dim_classHS_l2['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l2['code_ls']
dim_classHS_l2['un_pk_l2'] = dim_classHS_l2['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l2['code']

dim_classHS_l3['pk_l3'] = 'G-LVL' + dim_classHS_l3.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classHS_l3.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classHS_l3['un_pk_l2'] = dim_classHS_l3['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l3['code_ls']
dim_classHS_l3['un_pk_l3'] = dim_classHS_l3['classificationDesc'] + '-' + dim_classificationHS['typeCode'] + '-' + dim_classHS_l3['code']

#aux_str_classHS = dim_classHS_l3[['pk_l3','un_pk_l2','un_pk_l3']].copy().merge(dim_classHS_l2[['pk_l2','un_pk_l1','un_pk_l2']], on = 'un_pk_l2', how = 'left').merge(dim_classHS_l1[['pk_l1','un_pk_l0','un_pk_l1']], on = 'un_pk_l1', how = 'left')
aux_str_classHS = dim_classHS_l1[['pk_l1','un_pk_l0','un_pk_l1']].copy().merge(dim_classHS_l2[['pk_l2','un_pk_l1','un_pk_l2']], on = 'un_pk_l1', how = 'left').merge(dim_classHS_l3[['pk_l3','un_pk_l2','un_pk_l3']], on = 'un_pk_l2', how = 'left')

dim_classificationHS = aux_str_classHS.merge(dim_classHS_l3[['code','desc','pk_l3']].rename(columns = {'code':'un_code_l3','desc':'desc_l3'}), on = 'pk_l3', how = 'left')
dim_classificationHS = dim_classificationHS.merge(dim_classHS_l2[['code','desc','pk_l2']].rename(columns = {'code':'un_code_l2','desc':'desc_l2'}), on = 'pk_l2', how = 'left')
dim_classificationHS = dim_classificationHS.merge(dim_classHS_l1[['code','desc','pk_l1','classificationDesc','classificationType','typeCode']].rename(columns = {'code':'un_code_l1','desc':'desc_l1'}), on = 'pk_l1', how = 'left')

#dim_classificationHS['pk'] = dim_classificationHS['pk_l1'] + '-' + dim_classificationHS['pk_l2'] + '-' + dim_classificationHS['pk_l3']
dim_classificationHS['pk'] = np.where((dim_classificationHS['pk_l3'].isna() & dim_classificationHS['pk_l2'].isna()),
                                      dim_classificationHS['pk_l1'],
                                      np.where(dim_classificationHS['pk_l3'].isna(),
                                               dim_classificationHS['pk_l1'] + '-' + dim_classificationHS['pk_l2'],
                                               dim_classificationHS['pk_l1'] + '-' + dim_classificationHS['pk_l2'] + '-' + dim_classificationHS['pk_l3'])
                                      )

colVect = ['pk','pk_l1','pk_l2','pk_l3','un_pk_l0','un_pk_l1','un_pk_l2','un_pk_l3','un_code_l1','un_code_l2','un_code_l3','classificationDesc','classificationType','typeCode','desc_l1','desc_l2','desc_l3']
dim_classificationHS = dim_classificationHS[colVect].copy()


#-DIM CLASSIFICATION FOR SERVICES
dim_classificationEB = pd.read_csv(wd_in + '/df_classificationEB_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])

dim_classificationEB['level_code'] = np.where(dim_classificationEB['parent'].astype(str) == '200', 
                                              0, 
                                              dim_classificationEB['parent'].str.split('.').str.len())

colVect = ['code','desc','code_ls','updateMark','updateMark_TZ','level_code']

dim_classificationEB.columns = colVect

dim_classificationEB['classificationDesc'] = 'EBOPS 2010'
dim_classificationEB['classificationType'] = 'Services'
dim_classificationEB['typeCode'] = 'S'

dim_classificationEB['desc'] = dim_classificationEB['desc'].str.split(' ').str[1:].str.join(' ')

colVect = ['code','desc','code_ls','level_code','updateMark','updateMark_TZ','classificationDesc','classificationType','typeCode']

dim_classificationEB = dim_classificationEB[colVect].copy()


dim_classEB_l1 = dim_classificationEB[dim_classificationEB['level_code'] == 1].reset_index(drop = True)
dim_classEB_l2 = dim_classificationEB[dim_classificationEB['level_code'] == 2].reset_index(drop = True)
dim_classEB_l3 = dim_classificationEB[dim_classificationEB['level_code'] == 3].reset_index(drop = True)


dim_classEB_l1['pk_l1'] = 'S-LVL' + dim_classEB_l1.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classEB_l1.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classEB_l1['un_pk_l0'] = dim_classEB_l1['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l1['code_ls']
dim_classEB_l1['un_pk_l1'] = dim_classEB_l1['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l1['code']

dim_classEB_l2['pk_l2'] = 'S-LVL' + dim_classEB_l2.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classEB_l2.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classEB_l2['un_pk_l1'] = dim_classEB_l2['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l2['code_ls']
dim_classEB_l2['un_pk_l2'] = dim_classEB_l2['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l2['code']

dim_classEB_l3['pk_l3'] = 'S-LVL' + dim_classEB_l3.level_code.astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar) + '-IDX-' + dim_classEB_l3.index.astype(str).str.pad(width = 4, side = code_lpadSide, fillchar = fillchar)
dim_classEB_l3['un_pk_l2'] = dim_classEB_l3['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l3['code_ls']
dim_classEB_l3['un_pk_l3'] = dim_classEB_l3['classificationDesc'] + '-' + dim_classificationEB['typeCode'] + '-' + dim_classEB_l3['code']

#aux_str_classEB = dim_classEB_l3[['pk_l3','un_pk_l2','un_pk_l3']].copy().merge(dim_classEB_l2[['pk_l2','un_pk_l1','un_pk_l2']], on = 'un_pk_l2', how = 'left').merge(dim_classEB_l1[['pk_l1','un_pk_l0','un_pk_l1']], on = 'un_pk_l1', how = 'left')
aux_str_classEB = dim_classEB_l1[['pk_l1','un_pk_l0','un_pk_l1']].copy().merge(dim_classEB_l2[['pk_l2','un_pk_l1','un_pk_l2']], on = 'un_pk_l1', how = 'left').merge(dim_classEB_l3[['pk_l3','un_pk_l2','un_pk_l3']], on = 'un_pk_l2', how = 'left')


dim_classificationEB = aux_str_classEB.merge(dim_classEB_l3[['code','desc','pk_l3']].rename(columns = {'code':'un_code_l3','desc':'desc_l3'}), on = 'pk_l3', how = 'left')
dim_classificationEB = dim_classificationEB.merge(dim_classEB_l2[['code','desc','pk_l2']].rename(columns = {'code':'un_code_l2','desc':'desc_l2'}), on = 'pk_l2', how = 'left')
dim_classificationEB = dim_classificationEB.merge(dim_classEB_l1[['code','desc','pk_l1','classificationDesc','classificationType','typeCode']].rename(columns = {'code':'un_code_l1','desc':'desc_l1'}), on = 'pk_l1', how = 'left')


#dim_classificationEB['pk'] = dim_classificationEB['pk_l1'] + '-' + dim_classificationEB['pk_l2'] + '-' + dim_classificationEB['pk_l3']
dim_classificationEB['pk'] = np.where((dim_classificationEB['pk_l3'].isna() & dim_classificationEB['pk_l2'].isna()),
                                      dim_classificationEB['pk_l1'],
                                      np.where(dim_classificationEB['pk_l3'].isna(),
                                               dim_classificationEB['pk_l1'] + '-' + dim_classificationEB['pk_l2'],
                                               dim_classificationEB['pk_l1'] + '-' + dim_classificationEB['pk_l2'] + '-' + dim_classificationEB['pk_l3'])
                                      )

colVect = ['pk','pk_l1','pk_l2','pk_l3','un_pk_l0','un_pk_l1','un_pk_l2','un_pk_l3','un_code_l1','un_code_l2','un_code_l3','classificationDesc','classificationType','typeCode','desc_l1','desc_l2','desc_l3']
dim_classificationEB = dim_classificationEB[colVect].copy()

dim_classification = pd.concat([dim_classificationEB,dim_classificationHS]).reset_index(drop = True)

colVect = ['pk_l1','un_pk_l1','un_code_l1','classificationDesc','classificationType','typeCode','desc_l1']
dim_class_l1 = dim_classification[colVect].drop_duplicates().reset_index(drop = True)

colVect = ['pk_l1','pk_l2','un_pk_l2','un_code_l2','classificationDesc','classificationType','typeCode','desc_l2']
dim_class_l2 = dim_classification[colVect].drop_duplicates().reset_index(drop = True)

colVect = ['pk_l2','pk_l3','un_pk_l3','un_code_l3','classificationDesc','classificationType','typeCode','desc_l3']
dim_class_l3 = dim_classification[colVect].drop_duplicates().reset_index(drop = True)

#Erasing nulls in pk by level
dim_class_l1 = dim_class_l1[dim_class_l1['pk_l1'].isna() == False].reset_index(drop = True)
dim_class_l2 = dim_class_l2[dim_class_l2['pk_l2'].isna() == False].reset_index(drop = True)
dim_class_l3 = dim_class_l3[dim_class_l3['pk_l3'].isna() == False].reset_index(drop = True)


del(dim_classEB_l1,dim_classEB_l2,dim_classEB_l3,dim_classHS_l1,dim_classHS_l2,dim_classHS_l3,colVect,aux_str_classEB,aux_str_classHS,dim_classificationEB,dim_classificationHS)


#DIM FREQUENCY
dim_frequency = pd.read_csv(wd_in + '/df_frequency_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
#Standarizing column names
colVect = ['un_freq_code','freq_desc','updateMark','updateMark_TZ']
dim_frequency.columns = colVect
#Creating an internal code
width = len(str(dim_frequency.index.max())) + 1
dim_frequency['freq_code'] = 'FREQ-' + dim_frequency.index.astype(str).str.pad(width = width, side = code_lpadSide, fillchar = fillchar)


#DIM FLOW
dim_flow = pd.read_csv(wd_in + '/df_flow_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
#Standarizing column names
colVect = ['un_flow_code','flow_desc','updateMark','updateMark_TZ']
dim_flow.columns = colVect
#Creating an internal code
width = len(str(dim_flow.index.max())) + 1
dim_flow['flow_code'] = 'FLOW-' + dim_flow.index.astype(str).str.pad(width = width, side = code_lpadSide, fillchar = fillchar)


#DIM COMMERCIAL UNITS
dim_units = pd.read_csv(wd_in + '/df_units_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
#Standarizing column names
colVect = ['un_units_code','units_desc_short','units_desc_large','updateMark','updateMark_TZ']
dim_units.columns = colVect
#Creating an internal code
width = len(str(dim_units.index.max())) + 1
dim_units['units_code'] = 'UNIT-' + dim_units.index.astype(str).str.pad(width = width, side = code_lpadSide, fillchar = fillchar)


#DIM MODE OF TRANSPORTS
dim_transport = pd.read_csv(wd_in + '/df_mot_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
#Standarizing column names
colVect = ['un_mot_code','mot_desc','updateMark','updateMark_TZ']
dim_transport.columns = colVect
#Creating an internal code
width = len(str(dim_transport.index.max())) + 1
dim_transport['mot_code'] = 'MOT-' + dim_transport.index.astype(str).str.pad(width = width, side = code_lpadSide, fillchar = fillchar)


#DIM CUSTOMS
dim_customs = pd.read_csv(wd_in + '/df_customs_ori.csv', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
#Standarizing column names
colVect = ['un_customs_code','customs_desc','updateMark','updateMark_TZ']
dim_customs.columns = colVect
#Creating an internal code
width = len(str(dim_customs.index.max())) + 1
dim_customs['customs_code'] = 'CUS-' + dim_customs.index.astype(str).str.pad(width = width, side = code_lpadSide, fillchar = fillchar)

dim_class_l1['updateMark'] = updateMark
dim_class_l1['updateMark_TZ'] = updateMark_TZ
dim_class_l2['updateMark'] = updateMark
dim_class_l2['updateMark_TZ'] = updateMark_TZ
dim_class_l3['updateMark'] = updateMark
dim_class_l3['updateMark_TZ'] = updateMark_TZ

dim_classification['updateMark'] = updateMark
dim_classification['updateMark_TZ'] = updateMark_TZ

dim_country['updateMark'] = updateMark
dim_country['updateMark_TZ'] = updateMark_TZ


#Local Export
dim_class_l1.to_csv(wd_out + '/dim_HSclass_l1.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_class_l2.to_csv(wd_out + '/dim_HSclass_l2.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_class_l3.to_csv(wd_out + '/dim_HSclass_l3.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])

#dim_classification.to_csv(wd_out + '/dim_classification.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])

dim_country.to_csv(wd_out + '/dim_country.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
aux_country_stg.to_csv(wd_out + '/aux_country_stg.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])

#dim_customs.to_csv(wd_out + '/dim_customs.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_flow.to_csv(wd_out + '/dim_comTradeFlow.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_frequency.to_csv(wd_out + '/dim_frequency.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_transport.to_csv(wd_out + '/dim_comTradeMot.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
dim_units.to_csv(wd_out + '/dim_units.csv', index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])



#------#------#------#------#------#
##-- DATA LOAD
wd_in = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/out/'

#-DIMENSION TABLES
dim_class_l1 = pd.read_csv(wd_in + 'dim_HSclass_l1.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_class_l2 = pd.read_csv(wd_in + 'dim_HSclass_l2.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_class_l3 = pd.read_csv(wd_in + 'dim_HSclass_l3.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)

#dim_classification = pd.read_csv(wd_in + 'dim_classification.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_country = pd.read_csv(wd_in + 'dim_country.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding'], dtype = 'str').replace(np.nan, None)
dim_country = dim_country[dim_country['alpha2ISO'].isna() == False].reset_index(drop = True)

#dim_customs = pd.read_csv(wd_in + 'dim_customs.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_flow = pd.read_csv(wd_in + 'dim_comTradeFlow.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_frequency = pd.read_csv(wd_in + 'dim_frequency.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_transport = pd.read_csv(wd_in + 'dim_comTradeMot.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_units = pd.read_csv(wd_in + 'dim_units.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)


#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#- DBO.DIM_COUNTRY
query = '''INSERT INTO dbo.dim_country(unComtrade_id, unComtrade_text, unComtrade_textNote, alpha2ISO, alpha3ISO, updateMark, updateMark_TZ)
           VALUES (?,?,?,?,?,?,?)'''
           
data = [(x.unComtrade_id, x.unComtrade_text, x.unComtrade_textNote, x.alpha2ISO, x.alpha3ISO, x.updateMark, x.updateMark_TZ) for x in dim_country.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_country in iter uploaded')


#- DBO.DIM_CLASS_L1
query = '''INSERT INTO dbo.dim_HSclass_l1(pk_l1, un_pk_l1, un_code_l1, classificationDesc, classificationType, typeCode, desc_l1, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?,?,?,?,?)'''
           
data = [(x.pk_l1, x.un_pk_l1, x.un_code_l1, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l1, x.updateMark, x.updateMark_TZ) for x in dim_class_l1.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_HSclass_l1 in iter uploaded')


#- DBO.DIM_CLASS_L2
query = '''INSERT INTO dbo.dim_HSclass_l2(pk_l1, pk_l2, un_pk_l2, un_code_l2, classificationDesc, classificationType, typeCode, desc_l2, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?,?,?,?,?,?)'''
           
data = [(x.pk_l1, x.pk_l2, x.un_pk_l2, x.un_code_l2, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l2, x.updateMark, x.updateMark_TZ) for x in dim_class_l2.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_HSclass_l2 in iter uploaded')


#- DBO.DIM_CLASS_L3
query = '''INSERT INTO dbo.dim_HSclass_l3(pk_l2, pk_l3, un_pk_l3, un_code_l3, classificationDesc, classificationType, typeCode, desc_l3, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?,?,?,?,?,?)'''
           
data = [(x.pk_l2, x.pk_l3, x.un_pk_l3, x.un_code_l3, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l3, x.updateMark, x.updateMark_TZ) for x in dim_class_l3.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_HSclass_l3 in iter uploaded')

#- DBO.DIM_FREQUENCY
query = '''INSERT INTO dbo.dim_frequency(un_freq_code, freq_desc, freq_code, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?)'''
           
data = [(x.un_freq_code, x.freq_desc, x.freq_code, x.updateMark, x.updateMark_TZ) for x in dim_frequency.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_frequency in iter uploaded')


#- DBO.DIM_FLOW
query = '''INSERT INTO dbo.dim_comTradeFlow(un_flow_code, flow_desc, flow_code, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?)'''
           
data = [(x.un_flow_code, x.flow_desc, x.flow_code, x.updateMark, x.updateMark_TZ) for x in dim_flow.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_comTradeFlow in iter uploaded')


#- DBO.DIM_TRANSPORT
query = '''INSERT INTO dbo.dim_comTradeMot(un_mot_code, mot_desc, mot_code, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?)'''
           
data = [(x.un_mot_code, x.mot_desc, x.mot_code, x.updateMark, x.updateMark_TZ) for x in dim_transport.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_comTradeMot in iter uploaded')


#- DBO.DIM_UNITS
query = '''INSERT INTO dbo.dim_units(un_units_code, units_desc_short, units_desc_large, units_code, updateMark, updateMark_TZ) 
           VALUES (?,?,?,?,?,?)'''
           
data = [(x.un_units_code, x.units_desc_short, x.units_desc_large, x.units_code, x.updateMark, x.updateMark_TZ) for x in dim_units.itertuples()]

cursor.executemany(query, data)
conn.commit()
print('dim_units in iter uploaded')

conn.close()
print('process finished and connection closed')

