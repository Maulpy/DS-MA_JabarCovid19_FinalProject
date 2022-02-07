import pandas as pd
import numpy
import pymysql
from sqlalchemy import create_engine

def mysql_ingestion():
    conn = create_engine('mysql+pymysql://user:password@192.168.1.10:3306/dbmysql_ch43')
    sql="SELECT kode_prov, nama_prov, SUM(suspect) AS suspect_total, SUM(suspect_diisolasi) AS suspect_diisolasi_total,\
         YEAR(tanggal) AS tahun, MONTH(tanggal) AS bulan FROM mysql_ddl GROUP BY tahun, bulan, kode_prov, nama_prov;"
    df = pd.read_sql(sql, conn)
    return df

def postgresql_loader():
    df = mysql_ingestion()
    table = 'province_monthly_table'
    engine = create_engine('postgresql+psycopg2://postgres:password@192.168.1.10:5439/dbpostgresql_ch43')
    df.to_sql(table, engine, if_exists='replace')

if __name__=="__main__":
    postgresql_loader()











# def mysql_ingestion(): #Failed, whatever with date_format
#     engine = create_engine('mysql+pymysql://user:password@localhost:3306/dbmysql_ch43')
#     sql="SELECT date_format(tanggal, '%m-%Y') as monthYear, SUM(suspect) FROM mysql_ddl GROUP BY year(tanggal), month(tanggal);"
#     df = pd.read_sql(sql, engine)
#     print(df)

# def mysql_ingestion():

#     conn = pymysql.connect(host='localhost', user='user', password='password', charset='utf8', database='dbmysql_ch43')
#     #sql="SELECT * FROM mysql_ddl;" #success
#     sql="SELECT MONTH(tanggal), YEAR(tanggal),  SUM(suspect) \
#         FROM mysql_ddl GROUP BY YEAR(tanggal), MONTH(tanggal);"
#     df = pd.read_sql(sql, conn)
#     print(df)
    
# def postgresql_aggregation():

#     df = mysql_ingestion()
#     engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5439/dbpostgresql_ch43')
#     df.to_sql('province_monthly_table', engine)