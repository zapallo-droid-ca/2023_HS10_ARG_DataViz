### --- UN COMMODITY STATISTICS DB PIPELINE: ETL

##-- Environment Setting
import pandas as pd
import numpy as np
import datetime
import time

import os

import chardet #To detect the proper encoding of sources

import shutil #For moving files between folders

import pyodbc #To create the dabase connection and operate queries

import urllib.request
import io

import zipfile

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/'
wd_out = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/out/'
wd_stg = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/stg/'
wd_aux = "C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/00.Aux/"

os.chdir(wd)

#Credentials
wd_sec = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/00.Secure/'

serverConnectString = open(wd_sec + '/serverConnectionSTG.txt').read()

#General Settings
csvAttr_imp = {'sep': ';' , 'encoding': 'UTF-8'} #csv settings - import
csvAttr_exp = {'index': False, 'sep': ';' , 'encoding': 'UTF-8'} #csv settings - export

code_lpadSide = 'left'
fillchar = '0'

updateMark = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
updateMark_TZ = time.tzname[0]


#------#------#------#------#------#
##-- DATA EXTRACTION: DIM DATA
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#- DATA PROCESSING
q_reportersID = "SELECT DISTINCT reporterCode FROM dbo.ft_intComTrade"
q_partnersID = "SELECT DISTINCT partnerCode FROM dbo.ft_intComTrade"
q_dim_country = "SELECT * FROM dbo.dim_country"

dim_reporters = pd.read_sql(q_reportersID, conn)
dim_partners = pd.read_sql(q_partnersID, conn)
dim_country = pd.read_sql(q_dim_country, conn).drop(columns = ['unComtrade_textNote','alpha2ISO','updateMark', 'updateMark_TZ'])

dim_reporters = dim_country[dim_country['unComtrade_id'].isin(dim_reporters.reporterCode)].reset_index(drop = True)
dim_partners = dim_country[dim_country['unComtrade_id'].isin(dim_partners.partnerCode)].reset_index(drop = True)

dim_reporters.to_csv(wd_out + 'dim_reporters.csv.gz', index = False)
dim_partners.to_csv(wd_out + 'dim_partners.csv.gz', index = False)

#- DATA LOADING
query = '''INSERT INTO dbo.dim_partners(unComtrade_id ,unComtrade_text ,alpha3ISO, updateMark, updateMark_TZ)
           VALUES (?,?,?,?,?)'''

dim_partners['updateMark'] = updateMark
dim_partners['updateMark_TZ'] = updateMark_TZ

           
data = [(x.unComtrade_id ,x.unComtrade_text ,x.alpha3ISO , x.updateMark, x.updateMark_TZ) for x in dim_partners.itertuples()]

cursor.executemany(query, data)
conn.commit() 


query = '''INSERT INTO dbo.dim_reporters(unComtrade_id ,unComtrade_text ,alpha3ISO, updateMark, updateMark_TZ)
           VALUES (?,?,?,?,?)'''

dim_reporters['updateMark'] = updateMark
dim_reporters['updateMark_TZ'] = updateMark_TZ

           
data = [(x.unComtrade_id ,x.unComtrade_text ,x.alpha3ISO , x.updateMark, x.updateMark_TZ) for x in dim_reporters.itertuples()]

cursor.executemany(query, data)
conn.commit() 

conn.close()



#------#------#------#------#------#
##-- DATA EXTRACTION: FACT DATA
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

queryA = """ SELECT 
                calendarCode + '-' +flowCode + '-' + reporterCode + '-' + partnerCode trx_code,
                calendarCode calendarCode_year, reporterCode, flowCode, partnerCode, SUM(PrimaryValue) totalAnnualValue
            FROM dbo.ft_intComTrade 
            WHERE un_freqCode = 'A' AND typeCode = 'C' AND un_code_l2 = 'TOTAL' AND partnerCode IN (SELECT DISTINCT unComtrade_id FROM dbo.dim_country)
            GROUP BY calendarCode, reporterCode, flowCode, partnerCode, un_code_l2"""
            
queryB = """SELECT 
                LEFT(calendarCode,4) + '-' +flowCode + '-' + reporterCode + '-' + partnerCode trx_code, un_code_l2,
                SUM(PrimaryValue) totalValue
            FROM dbo.ft_intComTrade 
            WHERE un_freqCode = 'M' AND typeCode = 'C' AND un_code_l2 IN (SELECT DISTINCT un_code_l2 
                                                                          FROM dbo.dim_HSclass_l2 
                                                                          WHERE pk_l1 IN (SELECT DISTINCT pk_l1 FROM dbo.dim_HSclass_l1 WHERE un_code_l1 = '10'))
    			 AND partnerCode IN (SELECT DISTINCT unComtrade_id FROM dbo.dim_country) 
            GROUP BY LEFT(calendarCode,4) + '-' +flowCode + '-' + reporterCode + '-' + partnerCode, un_code_l2"""

#Extract
ft_inComTradeHS10_Annual_base = pd.read_sql(queryA, conn)

ft_inComTradeHS10_Annual_aux = pd.read_sql(queryB, conn)

#Transform
ft_inComTradeHS10_Annual_aux = ft_inComTradeHS10_Annual_aux.pivot(index = 'trx_code', columns = 'un_code_l2', values = 'totalValue').reset_index()
ft_inComTradeHS10_Annual_aux.columns = ['trx_code','totalValue1001','totalValue1002','totalValue1003','totalValue1004',
                                        'totalValue1005','totalValue1006','totalValue1007','totalValue1008']

ft_inComTradeHS10_Annual_aux.fillna(0, inplace = True)

ft_inComTradeHS10_Annual_aux['totalValueHS10'] = (ft_inComTradeHS10_Annual_aux['totalValue1001'] + ft_inComTradeHS10_Annual_aux['totalValue1002'] + 
                                                  ft_inComTradeHS10_Annual_aux['totalValue1003'] + ft_inComTradeHS10_Annual_aux['totalValue1004'] + 
                                                  ft_inComTradeHS10_Annual_aux['totalValue1005'] + ft_inComTradeHS10_Annual_aux['totalValue1006'] + 
                                                  ft_inComTradeHS10_Annual_aux['totalValue1007'] + ft_inComTradeHS10_Annual_aux['totalValue1008'])

ft_inComTradeHS10_Annual_aux = ft_inComTradeHS10_Annual_aux[['trx_code','totalValueHS10','totalValue1001','totalValue1002','totalValue1003','totalValue1004',
                                                             'totalValue1005','totalValue1006','totalValue1007','totalValue1008']].copy()

ft_inComTradeHS10_Annual = ft_inComTradeHS10_Annual_base.merge(ft_inComTradeHS10_Annual_aux, how = 'left', on = 'trx_code')
ft_inComTradeHS10_Annual.fillna(0, inplace = True)


#- DATA LOADING
query = '''INSERT INTO dbo.ft_inComTradeHS10_Annual(calendarCode_year ,reporterCode ,flowCode, partnerCode, totalValueHS10,
                                                    totalValue1001,totalValue1002,totalValue1003,totalValue1004,totalValue1005,
                                                    totalValue1006,totalValue1007,totalValue1008,totalAnnualValue,updateMark,updateMark_TZ)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

ft_inComTradeHS10_Annual['updateMark'] = updateMark
ft_inComTradeHS10_Annual['updateMark_TZ'] = updateMark_TZ

           
data = [(x.calendarCode_year ,x.reporterCode ,x.flowCode, x.partnerCode,x.totalValueHS10,
         x.totalValue1001, x.totalValue1002, x.totalValue1003, x.totalValue1004, x.totalValue1005,
         x.totalValue1006, x.totalValue1007, x.totalValue1008, x.totalAnnualValue, x.updateMark, x.updateMark_TZ) for x in ft_inComTradeHS10_Annual.itertuples()]

cursor.executemany(query, data)
conn.commit() 

conn.close()


#------#------#------#------#------#
##-- DATA EXTRACTION: CONTINENTS
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

q_country = """SELECT * FROM dbo.dim_country"""

df_country = pd.read_sql(q_country, conn)
df_country = df_country[['alpha3ISO']].copy()

df_auxContinents = pd.read_csv(wd_aux + '/curatedContinents.csv').rename(columns = {'Three_Letter_Country_Code':'alpha3ISO', 'Continent_Name':'continent'})

aux_countryContinent = df_country.merge(df_auxContinents[['alpha3ISO','continent']], on = 'alpha3ISO', how = 'left').replace(np.nan,None).drop_duplicates()
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'CYP') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'GEO') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'AZE') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'ARM') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'RUS') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'TUR') & (aux_countryContinent['continent'] == 'Asia')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'KAZ') & (aux_countryContinent['continent'] == 'Europe')) == False].reset_index(drop = True)
aux_countryContinent = aux_countryContinent[((aux_countryContinent['alpha3ISO'] == 'UMI') & (aux_countryContinent['continent'] == 'Oceania')) == False].reset_index(drop = True)

#- DATA LOADING
query = '''INSERT INTO dbo.aux_countryContinent(alpha3ISO ,continent ,updateMark,updateMark_TZ)
           VALUES (?,?,?,?)'''

aux_countryContinent['updateMark'] = updateMark
aux_countryContinent['updateMark_TZ'] = updateMark_TZ

           
data = [(x.alpha3ISO ,x.continent ,x.updateMark, x.updateMark_TZ) for x in aux_countryContinent.itertuples()]

cursor.executemany(query, data)
conn.commit() 

conn.close()








