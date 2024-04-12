import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Date

stations_df = pd.read_csv('clean_stations.csv')
measure_df = pd.read_csv('clean_measure.csv')

engine = create_engine('sqlite:///air_quality.db', echo=True)
conn = engine.connect()

metadata = MetaData()

stations_table = Table('stations', metadata,
                       Column('station_id', Integer, primary_key=True),
                       Column('date', Date),
                       Column('latitude', String),
                       Column('longitude', String),
                       Column('altitude', Integer),
                       Column('city', String),
                       Column('country', String),
                       Column('precip', String),
                       Column('tobs', String)
                      )

metadata.create_all(engine)

stations_df.to_sql('stations', conn, if_exists='replace', index=False)


result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
print(result)
