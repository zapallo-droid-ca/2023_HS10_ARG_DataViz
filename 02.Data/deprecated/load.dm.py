### --- UN COMTRADE DB PIPELINE: Load - General

##-- Environment Setting
import pandas as pd
import numpy as np
import datetime as dt

import os
import io
import time

import pyodbc

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/02.Data/02.Pipeline/'
wd_in = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/02.Data/02.Pipeline/out/'

os.chdir(wd)

#Credentials
wd_sec = 'C:/Users/jrab9/OneDrive/07.Proyectos/2023.PP_ID01/00.Secure/'

#serverName = pd.read_csv(wd_sec + '/serverConfig.txt', sep = ',')['serverName'][0]
#userName = pd.read_csv(wd_sec + '/serverConfig.txt', sep = ',')['userName'][0]
#passwordCode = pd.read_csv(wd_sec + '/serverConfig.txt', sep = ',')['passwordCode'][0]
#stgDB = pd.read_csv(wd_sec + '/serverConfig.txt', sep = ',')['stgDB'][0]
#prdDB = pd.read_csv(wd_sec + '/serverConfig.txt', sep = ',')['prdDB'][0]

serverConnectString = open(wd_sec + '/serverConnectionSTG.txt').read()

#General Settings
csvAttr_imp = {'sep': ';' , 'encoding': 'UTF-8'} #csv settings - import
csvAttr_exp = {'index': False, 'sep': ';' , 'encoding': 'UTF-8'} #csv settings - export

code_lpadSide = 'left'
fillchar = '0'

#------#------#------#------#------#
##-- DATA
#-DIMENSION TABLES
dim_class_l1 = pd.read_csv(wd_in + 'dim_class_l1.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_class_l2 = pd.read_csv(wd_in + 'dim_class_l2.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_class_l3 = pd.read_csv(wd_in + 'dim_class_l3.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)

#dim_classification = pd.read_csv(wd_in + 'dim_classification.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_country = pd.read_csv(wd_in + 'dim_country.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding'], dtype = 'str').replace(np.nan, None)
#dim_customs = pd.read_csv(wd_in + 'dim_customs.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_flow = pd.read_csv(wd_in + 'dim_flow.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_frequency = pd.read_csv(wd_in + 'dim_frequency.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_transport = pd.read_csv(wd_in + 'dim_transport.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)
dim_units = pd.read_csv(wd_in + 'dim_units.csv', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)

#-FACT TABLES
ft_goods = pd.read_csv(wd_in + 'ft_goods.csv.gz', sep = csvAttr_imp['sep'] , encoding = csvAttr_imp['encoding']).replace(np.nan, None)


#------#------#------#------#------#
##-- LOAD
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#- DBO.DIM_COUNTRY
query = '''INSERT INTO dbo.dim_country(unComtrade_id, unComtrade_text, unComtrade_textNote, alpha2ISO, alpha3ISO)
           VALUES (?,?,?,?,?)'''
           
data = [(x.unComtrade_id, x.unComtrade_text, x.unComtrade_textNote, x.alpha2ISO, x.alpha3ISO) for x in dim_country.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_CLASS_L1
query = '''INSERT INTO dbo.dim_class_l1(pk_l1, un_pk_l1, un_code_l1, classificationDesc, classificationType, typeCode, desc_l1) 
           VALUES (?,?,?,?,?,?,?)'''
           
data = [(x.pk_l1, x.un_pk_l1, x.un_code_l1, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l1) for x in dim_class_l1.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_CLASS_L2
query = '''INSERT INTO dbo.dim_class_l2(pk_l1, pk_l2, un_pk_l2, un_code_l2, classificationDesc, classificationType, typeCode, desc_l2) 
           VALUES (?,?,?,?,?,?,?,?)'''
           
data = [(x.pk_l1, x.pk_l2, x.un_pk_l2, x.un_code_l2, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l2) for x in dim_class_l2.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_CLASS_L3
query = '''INSERT INTO dbo.dim_class_l3(pk_l2, pk_l3, un_pk_l3, un_code_l3, classificationDesc, classificationType, typeCode, desc_l3) 
           VALUES (?,?,?,?,?,?,?,?)'''
           
data = [(x.pk_l2, x.pk_l3, x.un_pk_l3, x.un_code_l3, x.classificationDesc, x.classificationType, x.typeCode, x.desc_l3) for x in dim_class_l3.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_FREQUENCY
query = '''INSERT INTO dbo.dim_frequency(un_freq_code, freq_desc, freq_code) 
           VALUES (?,?,?)'''
           
data = [(x.un_freq_code, x.freq_desc, x.freq_code) for x in dim_frequency.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_FLOW
query = '''INSERT INTO dbo.dim_flow(un_flow_code, flow_desc, flow_code) 
           VALUES (?,?,?)'''
           
data = [(x.un_flow_code, x.flow_desc, x.flow_code) for x in dim_flow.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_TRANSPORT
query = '''INSERT INTO dbo.dim_transport(un_mot_code, mot_desc, mot_code) 
           VALUES (?,?,?)'''
           
data = [(x.un_mot_code, x.mot_desc, x.mot_code) for x in dim_transport.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.DIM_UNITS
query = '''INSERT INTO dbo.dim_units(un_units_code, units_desc_short, units_desc_large, units_code) 
           VALUES (?,?,?,?)'''
           
data = [(x.un_units_code, x.units_desc_short, x.units_desc_large, x.units_code) for x in dim_units.itertuples()]

cursor.executemany(query, data)
conn.commit()


#- DBO.FT_GOODS

ft_goods['pk_trx'] = ft_goods['classificationDesc'] + '-' + ft_goods['reporterCode'].astype(str) + '-' + ft_goods['cal_monthYearCode'].astype(str) + '-' + ft_goods['partnerCode'].astype(str) + '-' + ft_goods['secondPartnerCode'].astype(str) + '-' + ft_goods['typeCode'] + '-' + ft_goods['un_freqCode'] + '-' + ft_goods['flowCode'] + '-' + ft_goods['un_customs_code']  + '-' + ft_goods['un_mot_code'].astype(str)  + '-' + ft_goods['un_code_l2'].astype(str)    

dfrev = ft_goods[ft_goods['pk_trx'].isna()]


query = '''INSERT INTO dbo.ft_goods(typeCode, un_freqCode, cal_monthYearCode, reporterCode,
								 	flowCode, partnerCode, secondPartnerCode, classificationDesc,
								 	un_code_l2, un_customs_code, un_mot_code, un_units_code, Q,
								 	estimatedQ, un_units_code_alt, AtlQty, estimatedQ_alt,
									netWeight, estimatedNetWeight, grossWeight,
									estimatedGrossWeight, valueCIF, valueFOB, PrimaryValue,
									legacyEstimationFlag, reportedFlag, aggregateFlag,pk_trx) 
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
           
data = [(x.typeCode, x.un_freqCode, x.cal_monthYearCode, x.reporterCode, x.flowCode, x.partnerCode, x.secondPartnerCode, x.classificationDesc, x.un_code_l2, x.un_customs_code, x.un_mot_code, x.un_units_code, x.Q, x.estimatedQ, x.un_units_code_alt, x.AtlQty, x.estimatedQ_alt, x.netWeight, x.estimatedNetWeight, x.grossWeight, x.estimatedGrossWeight, x.valueCIF, x.valueFOB, x.PrimaryValue, x.legacyEstimationFlag, x.reportedFlag, x.aggregateFlag, x.pk_trx) for x in ft_goods.itertuples()]

cursor.executemany(query, data)
conn.commit()


conn.close()
