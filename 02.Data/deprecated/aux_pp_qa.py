import pandas as pd
import numpy as np

##---- Definimos Funciones para QA
def dfGenProfileiscero(x):
    return (x.astype(str) == '0').sum()

def dfVarProfileminstrlength(x):
    return x.astype(str).str.len().min()

def dfVarProfilemaxstrlength(x):
    return x.astype(str).str.len().max() 

def dfVarProfileMinNumVal(x):
    return x.min()

def dfVarProfileMaxNumVal(x):
    return x.max() 

def dfGenProfile(df):  
    dfGenProfile = {'Number of variables': df.shape[1],
                    'Number of observations': df.shape[0],
                    'Data Fields' : df.shape[0]*df.shape[1],
                    'Missing values': df.isna().sum().values.sum(),
                    'Missing values (%)': round(df.isna().sum().values.sum() / (df.shape[0]*df.shape[1]),2) * 100,
                    'Values in 0': df.apply(dfGenProfileiscero).sum(),
                    'Values in 0 (%)': round(df.apply(dfGenProfileiscero).sum() / (df.shape[0]*df.shape[1]),2) * 100,
                    'Number of numeric variables': df.select_dtypes(include=np.number).shape[1],
                    'Number of datatime variables': df.select_dtypes(include=np.datetime64).shape[1],
                    'Number of categorical variables': df.select_dtypes(include=object).shape[1],
                    'Number of duplicated values': df.shape[0] - df.drop_duplicates().shape[0]
    }

    dfGenProfile = pd.DataFrame(list(dfGenProfile.items()),columns = ['variable','measure'])
    
    return dfGenProfile

def dfVarProfile(df):    
    dfVarProfile = pd.DataFrame(df.dtypes, columns = ['dataType'])
    dfVarProfile['observations'] = df.count()
    dfVarProfile['nullValues'] = df.isna().sum()
    dfVarProfile['%nullValues'] = np.round((dfVarProfile.nullValues / dfVarProfile.observations) * 100, 2)
    dfVarProfile['ceroValues'] = df.apply(dfGenProfileiscero)
    dfVarProfile['%ceroValues'] = np.round((dfVarProfile.ceroValues / dfVarProfile.observations) * 100, 2)
    dfVarProfile['uniqueValues'] = df.nunique()
    dfVarProfile['%uniqueValues'] = np.round((dfVarProfile.uniqueValues / dfVarProfile.observations) * 100, 2)
    dfVarProfile['PK'] = np.where(dfVarProfile['%uniqueValues'] == 100, 1 , 0)
    dfVarProfile['minStrLength'] = df.apply(dfVarProfileminstrlength)
    dfVarProfile['maxStrLength'] = df.apply(dfVarProfilemaxstrlength)
    dfVarProfile['varNormalized'] = np.where(dfVarProfile['minStrLength'] == dfVarProfile['maxStrLength'], 1 , 0)
    
    return dfVarProfile.sort_values(['%uniqueValues','varNormalized'], ascending = False)