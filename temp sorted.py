import pandas as pd
from sqlalchemy import create_engine
import pymysql

df = pd.read_csv('E:\Pythoon\data\glbtemp.csv')
df.rename(columns={'dt': 'Year',
                   'LandAverageTemperature': 'Avg Temp',
                   'LandAverageTemperatureUncertainty': 'Avg Temp Uncertainty',
                   'LandMaxTemperature': 'Max Temp',
                   'LandMaxTemperatureUncertainty': 'Max Temp Uncertainty',
                   'LandMinTemperature': 'Min Temp',
                   'LandMinTemperatureUncertainty': 'Min Temp Uncertainty',
                   'LandAndOceanAverageTemperature': 'L & O Avg Temp',
                   'LandAndOceanAverageTemperatureUncertainty': 'L & O Avg Temp Uncertainty'}, inplace=True)
df['Year'] = pd.to_datetime(df['Year'])
df['Year'] = pd.DatetimeIndex(df['Year']).year
df = df.groupby('Year')[['Avg Temp', 'Avg Temp Uncertainty', 'Max Temp',
                         'Max Temp Uncertainty', 'Min Temp', 'Min Temp Uncertainty',
                         'L & O Avg Temp', 'L & O Avg Temp Uncertainty']].mean().reset_index()
df.drop([('Avg Temp Uncertainty'),('Max Temp Uncertainty'),('Min Temp Uncertainty'),('L & O Avg Temp Uncertainty'),
         'L & O Avg Temp',],axis=1, inplace=True)
#df = df[df['Year'] >= 1850].reset_index()
df = df.fillna(0)
df = df.round(3)
df[('Year')]= df[('Year')].astype(int)
df[('Avg Temp')]= df[('Avg Temp')].astype(int)
df[('Max Temp')]= df[('Max Temp')].astype(int)
df[('Min Temp')]= df[('Min Temp')].astype(int)
print(df)

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="123456",
                               db="temp_sorted"))

df.to_sql('temp_new', con = engine, if_exists = 'append', chunksize = 1000)