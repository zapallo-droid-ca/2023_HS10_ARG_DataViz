### --- UN COMTRADE DB PIPELINE: ETL - TRX

##-- Environment Setting
import pandas as pd
import numpy as np
import datetime
import time

import os
import chardet #To detect the proper encoding of sources

import shutil #For moving files between folders

import pyodbc #To create the dabase connection and operate queries

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/'
wd_in = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/01.UN_CTSD/01.extracted/'
wd_destination = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/in/01.UN_CTSD/02.loaded/'
wd_out = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/out/'
wd_stg = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/stg/'

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
##-- DATA EXTRACTION: Manual

##-- DATA TRANSFORMATION
#-FILES IN SOURCE PATH
filesinpath = [x for x in os.listdir(wd_in) if not x.startswith('~')] #to avoid open files / temporal copies
filesinpath = pd.DataFrame(filesinpath, columns = ['file'])
filesinpath['type'] = filesinpath.file.str.split('.').str[-1]
filesinpath = filesinpath[filesinpath['type'] == 'csv'].reset_index(drop = True)


filesinpathSize = pd.DataFrame()

for file in filesinpath.file.unique():
    dataTemp = {'file': file, 
                'size (MB)': os.path.getsize(wd_in + '/' + file) / 1000000}
    
    filesinpathSize = filesinpathSize.append(dataTemp, ignore_index = True)
    
filesinpath = filesinpath.merge(filesinpathSize, how = 'left', on = 'file')

filesinpath['fileWeight'] = (filesinpath['size (MB)'] / filesinpath['size (MB)'].sum()) * 100


#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#-READING FILES
filesQ = filesinpath.shape[0]

counter = 0
sizeStatus = 0

for i in filesinpath.file:
    
    df = pd.DataFrame()
    
    counter += 1
    sizeStatus += filesinpath[filesinpath['file'] == i].fileWeight.sum()  
    
    with open(wd_in + i , 'rb') as f:
        raw_data = f.read()
        
    encodingType = chardet.detect(raw_data)['encoding']
    
    df_temp = pd.read_csv(wd_in + i, encoding = encodingType, dtype= 'str')
    
    #-PROCESSING FILES
    colsToDrop = ['RefPeriodId','RefYear','RefMonth','ReporterISO','ReporterDesc','FlowDesc','PartnerISO','PartnerDesc','Partner2ISO','Partner2Desc',
                  'ClassificationSearchCode','IsOriginalClassification','CmdDesc','AggrLevel','IsLeaf','CustomsDesc','MosCode','MotDesc','QtyUnitAbbr',
                  'AltQtyUnitAbbr','Unnamed: 47']
    
    df_temp.drop(columns = colsToDrop, inplace = True)
    
    colNamesVect = {'TypeCode':'typeCode',
                    'FreqCode':'un_freqCode',
                    'Period':'calendarCode',
                    'ReporterCode':'reporterCode',
                    'FlowCode':'flowCode',
                    'PartnerCode':'partnerCode',
                    'Partner2Code':'secondPartnerCode',
                    'ClassificationCode':'classificationDesc',
                    'CmdCode':'un_code_l2',
                    'CustomsCode':'un_customs_code',
                    'MotCode':'un_mot_code',
                    'QtyUnitCode':'un_units_code',
                    'Qty':'Q',
                    'IsQtyEstimated':'estimatedQ',
                    'AltQtyUnitCode':'un_units_code_alt',
                    'AltQty':'Q_alt',
                    'IsAltQtyEstimated':'estimatedQ_alt',
                    'NetWgt':'netWeight',
                    'IsNetWgtEstimated':'estimatedNetWeight',
                    'GrossWgt':'grossWeight',
                    'IsGrossWgtEstimated':'estimatedGrossWeight',
                    'Cifvalue':'valueCIF',
                    'Fobvalue':'valueFOB',
                    'primaryValue':'valuePrimary',
                    'LegacyEstimationFlag':'legacyEstimationFlag',
                    'IsReported':'reportedFlag',
                    'IsAggregate':'aggregateFlag'}
    
    df_temp.rename(columns = colNamesVect, inplace = True)    
     
    df_temp = df_temp[df_temp['partnerCode'].isna() == False].reset_index(drop = True)
    
    df_temp['pk_trx'] = df_temp['classificationDesc'] + '-' + df_temp['reporterCode'].astype(str) + '-' + df_temp['calendarCode'].astype(str) + '-' + df_temp['partnerCode'].astype(str) + '-' + df_temp['secondPartnerCode'].astype(str) + '-' + df_temp['typeCode'] + '-' + df_temp['un_freqCode'] + '-' + df_temp['flowCode'] + '-' + df_temp['un_customs_code']  + '-' + df_temp['un_mot_code'].astype(str)  + '-' + df_temp['un_code_l2'].astype(str)    
    
    df_temp = df_temp.replace(np.nan, None)
    
    if 'TOTAL' in df_temp['un_code_l2'].unique():
        df_temp['un_code_l2'] = np.where(df_temp['un_code_l2'].isna(), '9999', df_temp['un_code_l2'].astype(str))
    else:    
        df_temp['un_code_l2'] = np.where(df_temp['un_code_l2'].astype(int).isna(), '9999', df_temp['un_code_l2'].astype(str))
    
    df_temp['Q'] = df_temp['Q'].astype(float).replace(np.nan, None)
    df_temp['estimatedQ'] = df_temp['estimatedQ'].astype(bool).replace(np.nan, None)
    
    df_temp['AtlQty'] = df_temp['AtlQty'].astype(float).replace(np.nan, None)
    df_temp['estimatedQ_alt'] = df_temp['estimatedQ_alt'].astype(bool).replace(np.nan, None)
    
    df_temp['netWeight'] = df_temp['netWeight'].astype(float).replace(np.nan, None)
    df_temp['estimatedGrossWeight'] = df_temp['estimatedGrossWeight'].astype(bool).replace(np.nan, None)
    
    #df_temp['valueCIF'] = (df_temp['valueCIF'].astype(float) / 1000000).replace(np.nan, None)
    #df_temp['valueFOB'] = (df_temp['valueFOB'].astype(float) / 1000000).replace(np.nan, None)
    #df_temp['PrimaryValue'] = (df_temp['PrimaryValue'].astype(float) / 1000000).replace(np.nan, None)
    
    df_temp['valueCIF'] = (df_temp['valueCIF'].astype(float)).replace(np.nan, None)
    df_temp['valueFOB'] = (df_temp['valueFOB'].astype(float)).replace(np.nan, None)
    df_temp['PrimaryValue'] = (df_temp['PrimaryValue'].astype(float)).replace(np.nan, None)
    
    df_temp['legacyEstimationFlag'] = df_temp['legacyEstimationFlag'].astype(bool).replace(np.nan, None)
    df_temp['reportedFlag'] = df_temp['reportedFlag'].astype(bool).replace(np.nan, None)
    df_temp['aggregateFlag'] = df_temp['aggregateFlag'].astype(bool).replace(np.nan, None)
    
    df_temp['updateMark'] = updateMark
    df_temp['updateMark_TZ'] = updateMark_TZ
            
    #- DBO.FT_GOODS
    query = '''INSERT INTO dbo.ft_intComTrade(typeCode, un_freqCode, calendarCode, reporterCode,
        								  	  flowCode, partnerCode, secondPartnerCode, classificationDesc,
        								  	  un_code_l2, un_customs_code, un_mot_code, un_units_code, Q,
        								 	  estimatedQ, un_units_code_alt, AtlQty, estimatedQ_alt,
        									  netWeight, estimatedNetWeight, grossWeight,
        									  estimatedGrossWeight, valueCIF, valueFOB, PrimaryValue,
        									  legacyEstimationFlag, reportedFlag, aggregateFlag,pk_trx,
                                              updateMark, updateMark_TZ) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
               
    data = [(x.typeCode, x.un_freqCode, x.calendarCode, x.reporterCode, x.flowCode, x.partnerCode, x.secondPartnerCode, x.classificationDesc, x.un_code_l2, x.un_customs_code, x.un_mot_code, x.un_units_code, x.Q, x.estimatedQ, x.un_units_code_alt, x.AtlQty, x.estimatedQ_alt, x.netWeight, x.estimatedNetWeight, x.grossWeight, x.estimatedGrossWeight, x.valueCIF, x.valueFOB, x.PrimaryValue, x.legacyEstimationFlag, x.reportedFlag, x.aggregateFlag, x.pk_trx, x.updateMark, x.updateMark_TZ) for x in df_temp.itertuples()]

    cursor.executemany(query, data)
    conn.commit() 
    
    df = pd.concat([df,df_temp])  
    
    shutil.move(wd_in + i,wd_destination) #Moving the file to the stagging folder
    
    #df.to_csv(wd_stg + '/ft_intComTrade.csv.gz', mode = 'a' , index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
    print('ft_intComTrade exported')
    
    del(raw_data, encodingType, df_temp, df)    
    
    print(f'file {counter} of {filesQ}, {np.round((counter / filesQ) * 100,1)}% completed, {np.round(sizeStatus,1)}% of size uploaded')
    
    #time.sleep(1)

conn.close() 

   
#-LOCAL EXPORT
#df.to_csv(wd_stg + '/ft_intComTrade.csv.gz', mode = 'a' , index = csvAttr_exp['index'], sep = csvAttr_exp['sep'] , encoding = csvAttr_exp['encoding'])
#print('ft_intComTrade exported')

print('process finished and connection closed')
