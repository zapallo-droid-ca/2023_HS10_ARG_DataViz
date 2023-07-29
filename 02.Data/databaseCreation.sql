--## Database Tables Creation
--# Dimensiuon Tables
USE iDevelopStg

DROP TABLE IF EXISTS dbo.dim_HSclass_l1;
CREATE TABLE dim_HSclass_l1(
pk_l1 VARCHAR(20) PRIMARY KEY,
un_pk_l1 VARCHAR(20),
un_code_l1 VARCHAR(10),
classificationDesc VARCHAR(10),
classificationType VARCHAR(20),
typeCode VARCHAR(5),
desc_l1 TEXT,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_HSclass_l2;
CREATE TABLE dim_HSclass_l2(
pk_l1 VARCHAR(20),
pk_l2 VARCHAR(20) PRIMARY KEY,
un_pk_l2 VARCHAR(20),
un_code_l2 VARCHAR(10),
classificationDesc VARCHAR(10),
classificationType VARCHAR(20),
typeCode VARCHAR(5),
desc_l2 TEXT,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_HSclass_l3;
CREATE TABLE dim_HSclass_l3(
pk_l2 VARCHAR(20),
pk_l3 VARCHAR(20) PRIMARY KEY,
un_pk_l3 VARCHAR(30),
un_code_l3 VARCHAR(10),
classificationDesc VARCHAR(10),
classificationType VARCHAR(20),
typeCode VARCHAR(5),
desc_l3 TEXT,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_country;
CREATE TABLE dim_country(
unComtrade_id VARCHAR(10) PRIMARY KEY,
unComtrade_text TEXT,
unComtrade_textNote TEXT,
alpha2ISO VARCHAR(10),
alpha3ISO VARCHAR(10),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_comTradeFlow;
CREATE TABLE dim_comTradeFlow(
un_flow_code VARCHAR(10),
flow_desc VARCHAR(50),
flow_code VARCHAR(10) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_frequency;
CREATE TABLE dim_frequency(
un_freq_code VARCHAR(10),
freq_desc VARCHAR(50),
freq_code VARCHAR(10) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_comTradeMot;
CREATE TABLE dim_comTradeMot(
un_mot_code VARCHAR(10),
mot_desc VARCHAR(50),
mot_code VARCHAR(10) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.dim_units;
CREATE TABLE dim_units(
un_units_code VARCHAR(10),
units_desc_short VARCHAR(30),
units_desc_large VARCHAR(60),
units_code VARCHAR(10) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.ft_intComTrade;
CREATE TABLE ft_intComTrade(
typeCode VARCHAR(4), 
un_freqCode VARCHAR(4), 
calendarCode VARCHAR(8), 
reporterCode VARCHAR(10),
flowCode VARCHAR(10), 
partnerCode VARCHAR(10), 
secondPartnerCode VARCHAR(10), 
classificationDesc VARCHAR(10),
un_code_l2 VARCHAR(10), 
un_customs_code VARCHAR(10), 
un_mot_code VARCHAR(10), 
un_units_code VARCHAR(10), 
Q NUMERIC,
estimatedQ BIT, 
un_units_code_alt VARCHAR(10), 
AtlQty NUMERIC, 
estimatedQ_alt BIT,
netWeight NUMERIC, 
estimatedNetWeight BIT, 
grossWeight NUMERIC,
estimatedGrossWeight BIT, 
valueCIF FLOAT(53), 
valueFOB FLOAT(53), 
PrimaryValue FLOAT(53),
legacyEstimationFlag BIT, 
reportedFlag BIT, 
aggregateFlag BIT,
pk_trx VARCHAR(80) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.ft_natIndicators;
CREATE TABLE ft_natIndicators(
country_code VARCHAR(10), 
series_name TEXT, 
series_code VARCHAR(30), 
year_code VARCHAR(10), 
measure NUMERIC,
measure_transform VARCHAR(10),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.aux_natIndicators;
CREATE TABLE aux_natIndicators(
series_name TEXT, 
series_code VARCHAR(30) PRIMARY KEY, 
unit_of_measure VARCHAR(20), 
meaning_Desc TEXT,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);

--CALENDARS
DROP TABLE IF EXISTS dbo.dim_calendar_d;
CREATE TABLE dim_calendar_d(
"date" DATE,
code VARCHAR(10) PRIMARY KEY,
day_of_year VARCHAR(10),
day_of_week VARCHAR(10),
day_name VARCHAR(20),
week_of_year VARCHAR(10),
"month" VARCHAR(10),
month_name VARCHAR(20),
day_of_month VARCHAR(10),
"quarter" VARCHAR(10),
"year" VARCHAR(10),
month_year VARCHAR(20),
month_name_year VARCHAR(30),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);

DROP TABLE IF EXISTS dbo.dim_calendar_m;
CREATE TABLE dim_calendar_m(
"month" VARCHAR(10),
month_name VARCHAR(10),
"quarter" VARCHAR(10),
"year" VARCHAR(10),
month_year VARCHAR(20) PRIMARY KEY,
month_name_year VARCHAR(20),
code VARCHAR(10),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);

DROP TABLE IF EXISTS dbo.dim_calendar_y;
CREATE TABLE dim_calendar_y(
"year" VARCHAR(10) PRIMARY KEY,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);

DROP TABLE IF EXISTS dbo.dim_partners;
CREATE TABLE dbo.dim_partners(
unComtrade_id VARCHAR(10) PRIMARY KEY,
unComtrade_text TEXT,
alpha3ISO VARCHAR(10),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
)

DROP TABLE IF EXISTS dbo.dim_reporters;
CREATE TABLE dbo.dim_reporters(
unComtrade_id VARCHAR(10) PRIMARY KEY,
unComtrade_text TEXT,
alpha3ISO VARCHAR(10),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);

DROP TABLE IF EXISTS dbo.ft_inComTradeHS10_Annual;
CREATE TABLE dbo.ft_inComTradeHS10_Annual(
calendarCode_year VARCHAR(8), 
reporterCode VARCHAR(10),
flowCode VARCHAR(10), 
partnerCode VARCHAR(10), 
totalAnnualValue FLOAT(53),
totalValueHS10 FLOAT(53),
totalValue1001 FLOAT(53),
totalValue1002 FLOAT(53),
totalValue1003 FLOAT(53),
totalValue1004 FLOAT(53),
totalValue1005 FLOAT(53),
totalValue1006 FLOAT(53),
totalValue1007 FLOAT(53),
totalValue1008 FLOAT(53),
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);


DROP TABLE IF EXISTS dbo.aux_countryContinent;
CREATE TABLE aux_countryContinent(
alpha3ISO VARCHAR(10) PRIMARY KEY,
continent TEXT,
updateMark DATETIME,
updateMark_TZ VARCHAR(30)
);