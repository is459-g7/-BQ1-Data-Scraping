import pandas as pd
import boto3

# Initialize S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'bq1-airflow-scraping-bucket'

# Wikipedia page URL
url = "https://en.wikipedia.org/wiki/Fuel_economy_in_aircraft"

def upload_data():
    # Scrape the tables
    tables = pd.read_html(url)
    
    # Define the tables to save, excluding the "Commuter_flights.csv" table
    tables_to_save = {
        "Regional_Flights.csv": tables[2],
        "Short_Haul_Flights.csv": tables[3],
        "Medium_Haul_Flights.csv": tables[4]
    }

    # Save each table as a CSV file and upload to S3
    for filename, table in tables_to_save.items():
        file_path = f'/tmp/{filename}'
        table.to_csv(file_path, index=False)
        s3.upload_file(file_path, S3_BUCKET_NAME, f'aircraft_fuel_data/{filename}')
        print(f"Uploaded {filename} to S3")

if __name__ == "__main__":
    upload_data()
