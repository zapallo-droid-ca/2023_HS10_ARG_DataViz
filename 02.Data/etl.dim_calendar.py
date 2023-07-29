### --- CALENDAR TABLES PIPELINE: ETL

##-- Environment Setting
import pandas as pd
import datetime
import time

import os

import pyodbc #To create the dabase connection and operate queries

#Work Directories
wd = 'C:/Users/jrab9/OneDrive/08.Github/2023.HS10-ARG.DataViz/02.Data/'
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
##-- AUX DATA
starts_at = '1990-01-01'
ends_at = '2022-12-31'

#------#------#------#------#------#
##-- TABLE CONSTRUCTION: Daily
#dim_calendar_d

dim_calendar = pd.DataFrame({'date': pd.date_range(starts_at, ends_at)})
dim_calendar['code'] = dim_calendar['date'].dt.strftime("%Y%m%d")
dim_calendar['day_of_year'] = dim_calendar.date.dt.dayofyear.astype(int)
dim_calendar['day_of_week'] = dim_calendar.date.dt.weekday.astype(int)
dim_calendar['day_name'] = dim_calendar.date.dt.day_name(locale = 'English')
dim_calendar['week_of_year'] = dim_calendar.date.dt.isocalendar().week.astype(int)
dim_calendar['month'] = dim_calendar.date.dt.month.astype(int)
dim_calendar['month_name'] = dim_calendar.date.dt.month_name(locale = 'English')
dim_calendar['day_of_month'] = dim_calendar.date.dt.day.astype(int)
dim_calendar['quarter'] = dim_calendar.date.dt.quarter.astype(int)
dim_calendar['year'] = dim_calendar.date.dt.year.astype(int)
dim_calendar['month_year'] = dim_calendar['month'].astype(str) + '-' + dim_calendar['year'].astype(str)
dim_calendar['month_name_year'] = dim_calendar['month_name'] + '-' + dim_calendar['year'].astype(str)
dim_calendar['date'] = dim_calendar['date'].dt.strftime("%Y-%m-%d")

dim_calendar.to_csv(wd_out + 'dim_calendar_d.csv.gz', index = False)



##-- TABLE CONSTRUCTION: Monthly
#dim_calendar_m
dim_calendar_m = dim_calendar.drop(columns = ['date','code','day_of_year','day_of_week','day_name','week_of_year','day_of_month']).copy()
dim_calendar_m.drop_duplicates(inplace = True)
dim_calendar_m['code'] = dim_calendar_m['year'].astype(str) + dim_calendar_m['month'].astype(str).str.pad(width = 2, side = code_lpadSide, fillchar = fillchar)
dim_calendar_m.to_csv(wd_out + 'dim_calendar_m.csv.gz', index = False)



##-- TABLE CONSTRUCTION: Yearly
#dim_calendar_y
dim_calendar_y = dim_calendar_m.drop(columns = ['month','month_name','quarter','month_year','month_name_year','code']).copy()
dim_calendar_y.drop_duplicates(inplace = True)

dim_calendar_y.to_csv(wd_out + 'dim_calendar_y.csv.gz', index = False)


###---------
##-- DATA LOADING
#- DB CONNECTION
conn = pyodbc.connect(serverConnectString)

cursor = conn.cursor()

#- DBO.DIM_CALENDAR_D
query = '''INSERT INTO dbo.dim_calendar_d(date ,code ,day_of_year ,day_of_week ,day_name ,week_of_year ,month ,month_name ,
                                          day_of_month ,quarter ,year ,month_year ,month_name_year, updateMark, updateMark_TZ)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

dim_calendar['updateMark'] = updateMark
dim_calendar['updateMark_TZ'] = updateMark_TZ

           
data = [(x.date ,x.code ,x.day_of_year ,x.day_of_week ,x.day_name ,x.week_of_year ,x.month ,x.month_name ,x.day_of_month ,x.quarter ,x.year ,x.month_year ,x.month_name_year, x.updateMark, x.updateMark_TZ) for x in dim_calendar.itertuples()]

cursor.executemany(query, data)
conn.commit() 



#- DBO.DIM_CALENDAR_M
query = '''INSERT INTO dbo.dim_calendar_m(month ,month_name ,quarter ,year ,month_year ,month_name_year, code, updateMark, updateMark_TZ)
           VALUES (?,?,?,?,?,?,?,?,?)'''

dim_calendar_m['updateMark'] = updateMark
dim_calendar_m['updateMark_TZ'] = updateMark_TZ

           
data = [(x.month ,x.month_name ,x.quarter ,x.year ,x.month_year ,x.month_name_year, x.code, x.updateMark, x.updateMark_TZ) for x in dim_calendar_m.itertuples()]

cursor.executemany(query, data)
conn.commit() 



#- DBO.DIM_CALENDAR_Y
query = '''INSERT INTO dbo.dim_calendar_y(year, updateMark, updateMark_TZ)
           VALUES (?,?,?)'''

dim_calendar_y['updateMark'] = updateMark
dim_calendar_y['updateMark_TZ'] = updateMark_TZ

           
data = [(x.year, x.updateMark, x.updateMark_TZ) for x in dim_calendar_y.itertuples()]

cursor.executemany(query, data)
conn.commit() 


conn.close()
                                          
































