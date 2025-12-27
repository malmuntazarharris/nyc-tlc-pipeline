import requests
import os
import datetime
import time
import logging
import boto3
import posixpath
import re

def build_url(taxi_type: str, year: int, month: int) -> str:
    # example 
    # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
    taxi_type  = taxi_type.lower()
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
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
    finalURL = URL + taxi_type + '_' + 'tripdata' + '_' + str(year) + '-' +str(month).zfill(2) + '.parquet'

    return finalURL

def download_with_retries(url: str, out_path: str, retries: int) -> str:     
    logger = logging.getLogger(__name__)

    filename = url.split('/')[-1]
    tmp_filename = filename + '.tmp'

    fullFilePath_tmp = os.path.join(out_path, tmp_filename)
    fullFilePath_final = os.path.join(out_path, filename)

    for attempt in range(1, retries + 1):
        try: 
            logger.info(f'Attempting download of {filename}. Attempt: {attempt}/{retries}')
            
            # remove any existing tmp files
            if os.path.exists(fullFilePath_tmp):
                os.remove(fullFilePath_tmp)

            # download to temp 
            with requests.get(url, stream=True, timeout=(5, 60)) as r:
                r.raise_for_status()
                with open(fullFilePath_tmp, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk: # write only if chunk is not empty
                                f.write(chunk)

            # Replace final for idempotency
            if os.path.exists(fullFilePath_final):
                os.remove(fullFilePath_final)
            os.rename(fullFilePath_tmp, fullFilePath_final)

                    
            logger.info(f"Download success. Saved to {fullFilePath_final}")
            return fullFilePath_final
        
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Transient error on attempt {attempt}/{retries}: {e}")
            if attempt == retries:
                raise
            time.sleep(min(10, 2 ** (attempt - 1)))
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error downloading {url}: {e}")
            raise


def upload_to_s3(filepath: str, bucket: str):
    logger = logging.getLogger(__name__)

    # declare vars
    S3_PREFIX="tlc/raw"

    file_name = os.path.basename(filepath)

    # get year and month strings
    match = re.search(r'(\d{4})-(\d{2})', file_name)

    if match:
        year = match.group(1)   
        month = match.group(2)  
    
    else:
        raise ValueError("Filename does not have year-month (YYYY-MM)")
    
    # create local file path
    object_key = posixpath.join(S3_PREFIX, f"year={year}", f"month={month}", file_name)

    # Upload file
    s3_client = boto3.client('s3')
    s3_client.upload_file(
            file_name, 
            bucket,
            object_key
    )
    logger.info(f"Uploading to s3://{bucket}/{object_key}")

def main(outpath, taxiColor, year, month, retries):
    downloadLink = build_url(taxiColor, year, month)
    fileName = download_with_retries(downloadLink, outpath, retries)
    upload_to_s3(os.path.basename(fileName), "nyc-taxi-bucket-mah-east-1")

if __name__ == "__main__":
    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    main('./', 'yellow', 2025, 1, 3)