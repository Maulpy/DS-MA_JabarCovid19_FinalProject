import pandas as pd
import requests
from datetime import date

http_command = "https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian"

def ingestion():
    
    
    req = requests.get(http_command)
    df = pd.DataFrame(req.json()['data']['content'])
    df.to_csv(f'./Covid19RecapitulationFiles/rekapcovid19jabar{date.today()}.csv', index = False)
    
if __name__=="__main__":
    ingestion()