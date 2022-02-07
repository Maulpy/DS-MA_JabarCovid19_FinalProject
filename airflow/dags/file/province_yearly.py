import pandas as pd
import numpy
import pymysql
from sqlalchemy import create_engine

def mysql_ingestion():
    conn = create_engine('mysql+pymysql://user:password@192.168.1.10:3306/dbmysql_ch43')
    sql="SELECT kode_prov, nama_prov, SUM(suspect) AS suspect_total, SUM(suspect_diisolasi) AS suspect_diisolasi_total,\
         YEAR(tanggal) AS tahun FROM mysql_ddl GROUP BY tahun, kode_prov, nama_prov;"
    df = pd.read_sql(sql, conn)
    return df

def postgresql_loader():
    df = mysql_ingestion()
    table = 'province_yearly_table'
    engine = create_engine('postgresql+psycopg2://postgres:password@192.168.1.10:5439/dbpostgresql_ch43')
    df.to_sql(table, engine, if_exists='replace')

if __name__=="__main__":
    postgresql_loader()