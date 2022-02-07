import pandas as pd
import numpy
import pymysql
from sqlalchemy import create_engine
import psycopg2

def mysql_ingestion():
    conn = create_engine('mysql+pymysql://user:password@192.168.1.10:3306/dbmysql_ch43')
    sql="SELECT kode_prov, nama_prov, SUM(suspect) AS suspect_total, SUM(suspect_diisolasi) AS suspect_diisolasi_total,\
         tanggal FROM mysql_ddl GROUP BY tanggal, kode_prov, nama_prov;"
    df = pd.read_sql(sql, conn)
    return df

def postgresql_loader():
    df = mysql_ingestion()
    table = 'province_daily_table'
    engine = create_engine('postgresql+psycopg2://postgres:password@192.168.1.10:5439/dbpostgresql_ch43')
    df.to_sql(table, engine, if_exists='replace')

if __name__=="__main__":
    postgresql_loader()










    # print(mysql_ingestion())


#conn = pymysql.connect(user="root", password="12password21", charset="utf8", database="dbmysql_ch43") #host="localhost", port=3306, , 
#sql="SELECT * FROM mysql_ddl;" #success

# def postgresql_ingestion():
#     covid19 = mysql_ingestion()
#     conn = psycopg2.connect(host="192.168.1.10", user="postgres", password="password", port="5439", database="dbpostgresql_ch43")
#     cursor = conn.cursor()

#     for i, row in covid19.iterrows():
#         table = 'province_daily_table'
#         columns = ",".join(["kode_prov", "nama_prov", "suspect_total", "suspect_diisolasi_total", "tanggal"])
#         values = "VALUES ({})".format(",".join(['%s' for x in range(5)]))
#         sql="DROP TABLE IF EXISTS {}; CREATE TABLE {} (kode_prov INT, nama_prov varchar(10), suspect_total INT,\
#                                                        suspect_diisolasi_total INT, tanggal date);".format(table, table)
#         cursor.execute(sql)
#         sql = "INSERT INTO {}({}) {};".format(table, columns, values)
#         cursor.execute(sql, tuple(row))
#         conn.commit()

# def mysql_ingestion(): #Failed, whatever with date_format
#     engine = create_engine('mysql+pymysql://user:password@localhost:3306/dbmysql_ch43')
#     sql="SELECT date_format(tanggal, '%m-%Y') as monthYear, SUM(suspect) FROM mysql_ddl GROUP BY year(tanggal), month(tanggal);"
#     df = pd.read_sql(sql, engine)
#     print(df)

# def postgresql_aggregation():

#     df = mysql_ingestion()
#     engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5439/dbpostgresql_ch43')
#     df.to_sql('province_daily_table', engine)

# def ingestionv0():
    
#     path = os.getcwd() + '/rekapcovid19jabar.csv'
#     covid19 = pd.read_csv(path)
#     engine = create_engine('mysql+pymysql://user:password@localhost:3306/dbmysql_ch43')
#     covid19.to_sql('mysql_ddl', con=engine)
