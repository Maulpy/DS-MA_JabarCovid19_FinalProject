import pandas as pd
import requests
import os
from sqlalchemy import create_engine
import pymysql
import csv
from datetime import date

def ingestionv0():
    
    path = os.getcwd() + f'./Covid19RecapitulationFiles/rekapcovid19jabar{date.today()}.csv'
    covid19 = pd.read_csv(path)
    table = 'mysql_ddl'
    engine = create_engine('mysql+pymysql://user:password@localhost:3306/dbmysql_ch43')
    covid19.to_sql(table, engine, if_exists='replace')

if __name__=="__main__":
    ingestionv0()











    

# def ingestionv1():

#     path = os.getcwd() + f'/rekapcovid19jabar{date.today()}.csv'
#     # covid19 = csv.reader(open(path))
#     covid19 = pd.read_csv(path)
#     conn = pymysql.connect(host='localhost', user='user', password='password', charset='utf8',\
#                            database='dbmysql_ch43')
#     cursor = conn.cursor()

#     for i, row in covid19.iterrows():
#         table = 'mysql_ddl'
#         columns = ",".join(["tanggal", "kode_prov", "nama_prov", "suspect", "suspect_diisolasi",\
#                             "suspect_discarded", "CLOSECONTACT", "closecontact_dikarantina",\
#                             "closecontact_discarded","probable_discarded", "probable_diisolasi",\
#                             "probable_meninggal","CONFIRMATION", "confirmation_diisolasi",\
#                             "confirmation_selesai","confirmation_meninggal", "suspect_meninggal_harian",\
#                             "closecontact_meninggal_harian"])
#         values = "VALUES ({})".format(",".join(['%s' for x in range(18)]))
#         sql="DROP TABLE {} IF EXISTS INSERT INTO {} ({}) {}".format(table, table, columns, values)
#         cursor.execute(sql, tuple(row))
#         conn.commit()
    
