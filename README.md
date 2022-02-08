# DS-MA_JabarCovid19_FinalProject

This Scripts serve to extract data from covid19 API. 
(https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian)

# Prequisites:

- IDE, preferably Visual Studio Code.
- Installed Docker and Docker Compose. (Preferrably with Docker Dekstop 4.4.4)
- Installed DBeaver (please later configure database host with your current ipv4 address)
- Install Python Dependencies inside docker-airflow worker terminal using pip : SQLAlchemy, pymysql, psycopg2, pandas.
- 

# How To Run:

1. Configure the docker compose with your own preference, it is better to set MySQL host to 3306 and PostgreSQL to 5432.
2. Docker-compose up -d for each folder (airflow, postgresql and mysql).
3. Open airflow webserver from docker desktop.
4. Set connection for postgresql manually.
5. Set connection for mysql by running "./airflow/dags/airflow_dbs_conn.py" inside IDE, it will trigger DAG to connect to Airflow.
6. To extract data from API to Database.
   - Run "./airflow/dags/file/api_extractor.py" and "./airflow/dags/file/mysql_dbwriter.py"
7. For Aggregation and loading to PostgreSQL you can either.
   - Run "./airflow/dags/dag_postgreswriter_provincemonthly.py" to trigger automated DAG.
   - Run "./airflow/dags/dag_postgreswriter_provincedaily.py" to execute one-timer.
