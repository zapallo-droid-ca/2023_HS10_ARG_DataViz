### --- WB GLOBAL INDICATORS DB PIPELINE: ETL

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
wd_in = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/03.WB_WDI/01.extracted/'
wd_destination = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/03.WB_WDI/02.loaded/'
wd_out = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/out/'
wd_stg = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/stg/'
wd_aux = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/03.WB_WDI/00.aux/'

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

#Functions
def encodingTypeOfFile(path): #To detect type of encoding    
    if 'http' in path:        
        response = urllib.request.urlopen(path)
        raw_data = response.read()
        
    else:
        with open(path, 'rb') as file:
            raw_data = file.read()
        
    encodingType = chardet.detect(raw_data)['encoding']
    
    return encodingType


def filesInWD(path, extension):    
    filesinpath = [x for x in os.listdir(path) if not x.startswith('~')] #to avoid open files / temporal copies
    filesinpath = pd.DataFrame(filesinpath, columns = ['file'])
    filesinpath['type'] = filesinpath.file.str.split('.').str[-1]
    filesinpath = filesinpath[filesinpath['type'] == extension].reset_index(drop = True)

    filesinpathSize = pd.DataFrame()    
    
    for file in filesinpath.file.unique():
        dataTemp = {'file': file, 
                    'size (MB)': os.path.getsize(path + '/' + file) / 1000000}
        
        filesinpathSize = filesinpathSize.append(dataTemp, ignore_index = True)
        
    filesinpath = filesinpath.merge(filesinpathSize, how = 'left', on = 'file')

    filesinpath['fileWeight'] = (filesinpath['size (MB)'] / filesinpath['size (MB)'].sum()) * 100
    
    return filesinpath


#------#------#------#------#------#
##-- DATA EXTRACTION
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#- DATA PROCESSING
filesinpath = filesInWD(wd_in, 'csv')

filesQ = filesinpath.shape[0] - 1 #Base file
counter = 0
sizeStatus = 0

filesToCheck_v = pd.DataFrame()
filesToCheck_q = pd.DataFrame()

#As the main format is horizontal, taking the 2022 indicators as base file
base_file = '6cafc987-187c-434b-be2a-b09e19922f88_Data.csv' #2022

with open(wd_in + base_file, 'rb') as file:
    file_content = file.read()
    
encodingType = chardet.detect(file_content)['encoding']; 
    
df = pd.read_csv(io.BytesIO(file_content), sep = ',', encoding = encodingType)

#Cleaning Footnotes
df = df[df['Series Code'].isna() == False].reset_index(drop = True)

#Primary Key to join other years
df['pk_temp'] = df['Country Code'] + '-' + df['Series Code']


for fileSource in filesinpath[filesinpath['file'] != base_file]['file']:    
    counter += 1
    sizeStatus += filesinpath[filesinpath['file'] == fileSource].fileWeight.sum()  
 
    with open(wd_in + fileSource, 'rb') as file:
        file_content = file.read()
        
    encodingType = chardet.detect(file_content)['encoding']; 
    print(f'file {counter} de {filesQ} decoded')
        
    df_temp = pd.read_csv(io.BytesIO(file_content), sep = ',', encoding = encodingType)
    print(f'file {counter} de {filesQ} readed')
    
    #Cleaning Footnotes
    df_temp = df_temp[df_temp['Series Code'].isna() == False].reset_index(drop = True)
    
    #Primary Key to join other years
    df_temp['pk_temp'] = df_temp['Country Code'] + '-' + df_temp['Series Code']
    
    df = df.merge(df_temp.iloc[:,4:], on = 'pk_temp', how = 'left')
    print(f'file {counter} de {filesQ} merged')
    
    print(f'file {counter} of {filesQ}, {np.round((counter / filesQ) * 100,1)}% completed, {np.round(sizeStatus,1)}% of size readed')
    
    shutil.move(wd_in + fileSource,wd_destination) #Moving the file to the stagging folder
    
    del(df_temp)    
    
del(encodingType, fileSource, counter, sizeStatus)  

shutil.move(wd_in + base_file,wd_destination) #Moving the file to the stagging folder

df.drop(columns = ['pk_temp', 'Country Name'], inplace = True)

#Filtering data by indicators of interest
df_aux = pd.read_excel(wd_aux + 'wbIndicators.xlsx') ; df_aux = df_aux[df_aux['DW'] == 1].reset_index(drop = True).drop(columns = 'Index')

df = df[df['Series Code'].isin(df_aux['Series Code'])].reset_index(drop = True)

#Melting Dataset
df = df.melt(id_vars = ['Country Code', 'Series Name', 'Series Code'], var_name = 'year_code', value_name = 'measure').reset_index(drop = True)
df['year_code'] = df['year_code'].str[:4]

df['measure'] = np.where(df['measure'] == '..' , np.nan, df['measure']).astype(float)

df['measure_transform'] = np.where((df['measure'] >= 1000000000000) |  (df['measure'] <= -1000000000000), 'trillions', np.nan)
df['measure'] = np.where((df['measure'] >= 1000000000000) |  (df['measure'] <= -1000000000000) , df['measure'] / 1000000, df['measure']).astype(float)

df['measure_transform'] = np.where((df['measure'] >= 1000000000) |  (df['measure'] <= -1000000000), 'billions', np.nan)
df['measure'] = np.where((df['measure'] >= 1000000000) |  (df['measure'] <= -1000000000) , df['measure'] / 1000000, df['measure']).astype(float)

df['measure_transform'] = np.where((df['measure'] >= 1000000) |  (df['measure'] <= -1000000), 'millions', np.nan)
df['measure'] = np.where((df['measure'] >= 1000000) |  (df['measure'] <= -1000000) , df['measure'] / 1000000, df['measure']).astype(float)

df['measure'] = np.round(df['measure'],4)

df.columns = ['country_code','series_name','series_code','year_code','measure','measure_transform']

#Auxiliary Data
df_aux = pd.read_excel(wd_aux + 'wbIndicatorsAux.xlsx')
df_aux.columns = ['series_name','series_code','unit_of_measure','meaning_Desc']

#Handling np.nan
df = df.replace(np.nan, None)
df_aux = df_aux.replace(np.nan, None)

#- DBO.FT_NATINDICATORS
query = '''INSERT INTO dbo.ft_natIndicators(country_code,series_name,series_code,year_code,measure,measure_transform,updateMark,updateMark_TZ)
           VALUES (?,?,?,?,?,?,?,?)'''

df['updateMark'] = updateMark
df['updateMark_TZ'] = updateMark_TZ
           
data = [(x.country_code, x.series_name, x.series_code, x.year_code, x.measure, x.measure_transform, x.updateMark, x.updateMark_TZ) for x in df.itertuples()]

cursor.executemany(query, data)
conn.commit() 


#- DBO.AUX_NATINDICATORS
query = '''INSERT INTO dbo.aux_natIndicators(series_name,series_code,unit_of_measure,meaning_Desc,updateMark,updateMark_TZ)
           VALUES (?,?,?,?,?,?)'''

df_aux['updateMark'] = updateMark
df_aux['updateMark_TZ'] = updateMark_TZ
           
data = [(x.series_name, x.series_code, x.unit_of_measure, x.meaning_Desc, x.updateMark, x.updateMark_TZ) for x in df_aux.itertuples()]

cursor.executemany(query, data)
conn.commit() 

conn.close()
print('process finished and connection closed')

