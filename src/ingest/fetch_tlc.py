import requests
import datetime
import boto3

def build_url(taxi_type: str, year: int, month: int) -> str:
    taxi_type.lower() = taxi_type
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    # validate taxi_type
    if taxi_type not in ("yellow", "green", "fhv", "fhvhv"):
        raise ValueError("taxi_type param needs to be 'yellow', 'green', 'fhv', or 'fhvhv'")
    
    # validate year
    if year < 2009 or year > datetime.date.today().year:
        raise ValueError
    
    # validate month
    if month < 1 or month > 12:
        raise ValueError
    
    # build url
    finalURL = URL + taxi_type + '_' + 'tripdata' + '_' + str(year) + '-' +str(month) + '.parquet'

    return finalURL


def download_with_retries(url, out_path):
    pass

def upload_to_s3(local_path, bucket, key):
    pass

def main():

    # TODO: compute dt (date)

    # TODO: fetch from nyc taxi 

    # TODO: upload files to s3
    pass