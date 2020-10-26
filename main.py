import pandas as pd
import yfinance as yf
from google.cloud import bigquery

def my_function(request):
    msft = yf.Ticker("MSFT")
    hist = msft.history(period="5d") 
    df = pd.DataFrame(data=hist)
    df = df.rename(columns={"Stock Splits": "StockSplits"})

    #Create BQ client
    client = bigquery.Client()

    #BQ Table data
    dataset_id = 'cloudFunctionETL'
    table_id = 'pandastrial'

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    # job_config.field_delimiter=(";")
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    job.result()  # Waits for table load to complete.
    print("The file has been successfully uploaded to BigQuery!")