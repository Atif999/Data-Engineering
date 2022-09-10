from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd
from math import dist, radians, cos, sin, asin, sqrt
import json
import time
from sqlalchemy import Table, Column, Integer, String, MetaData,Float
from datetime import datetime

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here
while True:
    try:
        dest_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MYSQL successful.')

metadata_obj = MetaData()
devices_data = Table(
            'devices_data', metadata_obj,
            Column('device_id', String(100)),
            Column('DATA_POINTS', Integer),
            Column('MAX_TEMPERATURE', Integer),
            Column('MOVEMENT', Float),
        )
metadata_obj.create_all(dest_engine)


Conn = psql_engine.connect()
Conn.execute("TRUNCATE TABLE devices")

#function to get movement by devices
def distance(lat1, lat2, lon1, lon2):
    lat1=float(lat1)
    lat2=float(lat2)
    lon1=float(lon1)
    lon2=float(lon2)     
    
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
      
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
 
    c = 2 * asin(sqrt(a))
    
    r = 6371
      
    
    return(c * r)


table_name='devices'

#ETL Function
def etl(engine,tableName,destination_engine):
    time.sleep(60)

    # extraction of data
    table_df = pd.read_sql_table(
                tableName,
                con=engine)

    table_df['time']=table_df['time'].astype(str).astype(int)

    # transformation and aggregations of the extracted data
    max_df=table_df.iloc[table_df.groupby('device_id').apply(lambda x: x['temperature'].idxmax())]
    max_df=max_df.drop(columns=['time', 'location'])

    new_df=table_df.groupby('device_id').size()
    new_df=new_df.to_frame()

    final_df=new_df.join(max_df.set_index("device_id"), on="device_id")
    final_df.columns = ['DATA_POINTS', 'MAX_TEMPERATURE']

    #getting location of devices   
    min_loc=table_df.iloc[table_df.groupby('device_id').apply(lambda x: x['time'].idxmin())]
    max_loc=table_df.iloc[table_df.groupby('device_id').apply(lambda x: x['time'].idxmax())]

    min_loc=min_loc.drop(columns=['time', 'temperature'])
    max_loc=max_loc.drop(columns=['time', 'temperature'])

      
    lat_long_1=min_loc.iat[0,1]
    lat_long_2=min_loc.iat[1,1]
    lat_long_3=min_loc.iat[2,1]
    
    #parsing the the location
    lat_long_1=json.loads(lat_long_1)
    lat_long_2=json.loads(lat_long_2)
    lat_long_3=json.loads(lat_long_3)


    lat_long_4=max_loc.iat[0,1]
    lat_long_5=max_loc.iat[1,1]
    lat_long_6=max_loc.iat[2,1]
    
    #parsing the the location
    lat_long_4=json.loads(lat_long_4)
    lat_long_5=json.loads(lat_long_5)
    lat_long_6=json.loads(lat_long_6)

    #calculating movement
    dist1=distance(lat_long_1["latitude"],lat_long_4["latitude"],lat_long_1["longitude"],lat_long_4["longitude"])
    dist2=distance(lat_long_2["latitude"],lat_long_5["latitude"],lat_long_2["longitude"],lat_long_5["longitude"])
    dist3=distance(lat_long_3["latitude"],lat_long_6["latitude"],lat_long_3["longitude"],lat_long_6["longitude"])
    
    final_df = final_df.assign(MOVEMENT=[dist1,dist2,dist3])


    #loading the aggregations in mysql database
    final_df.to_sql(name='devices_data', con=destination_engine, if_exists = 'append', chunksize = 1000)

# getting devices aggregation data every hour
while True:
    etl(psql_engine,table_name,dest_engine)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print('Devices Aggregations Added at time:',current_time)
    time.sleep(3600)
