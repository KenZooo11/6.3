import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Date, select, update, delete

def main():
    stations_df = pd.read_csv('clean_stations.csv')
    measure_df = pd.read_csv('clean_measure.csv')

    engine = create_engine('sqlite:///air_quality.db', echo=True)
    conn = engine.connect()
    
    metadata_stations = MetaData()
    stations_table = Table('stations', metadata_stations,
                           Column('station_id', Integer, primary_key=True),
                           Column('latitude', String),
                           Column('longitude', String),
                           Column('altitude', Integer),
                           Column('city', String),
                           Column('country', String)
                          )

    metadata_measure = MetaData()
    measure_table = Table('measure', metadata_measure,
                         Column('id', Integer, primary_key=True),
                         Column('station', String),
                         Column('date', Date),
                         Column('precip', String),
                         Column('tobs', Integer)
                        )

    metadata_stations.create_all(engine)
    metadata_measure.create_all(engine)


    stations_df.to_sql('stations', conn, if_exists='replace', index=False)
    measure_df.to_sql('measure', conn, if_exists='replace', index=False)

    result_select = conn.execute(select([stations_table]).limit(5)).fetchall()
    print("First 5 records from stations table:")
    print(result_select)

    result_select_measure = conn.execute(select([measure_table]).limit(5)).fetchall()
    print("\nFirst 5 records from measure table:")
    print(result_select_measure)

    result_select_where = conn.execute(select([measure_table]).where(measure_table.c.station == 'USC00519397')).fetchall()
    print("\nSELECT with WHERE clause:")
    print(result_select_where)

    stmt_update = update(measure_table).where(measure_table.c.station == 'USC00519397').values(precip='0.5')
    conn.execute(stmt_update)

    stmt_delete = delete(measure_table).where(measure_table.c.station == 'USC00519397')
    conn.execute(stmt_delete)

    result_updated = conn.execute(select([measure_table]).where(measure_table.c.station == 'USC00519397')).fetchall()
    print("\nAfter UPDATE and DELETE operations:")
    print(result_updated)

if __name__ == "__main__":
    main()
