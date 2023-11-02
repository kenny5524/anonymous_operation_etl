import requests
import pandas as pd
import boto3
import logging
import io
from io import BytesIO
from zipfile import ZipFile
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import time
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from botocore.exceptions import NoCredentialsError
from utils.data_mappings import event_codes_map, event_root_codes_map, event_base_codes_map, map_fips_to_iso2_map



# Load environment variables from .env file
load_dotenv('.env')

# Set up logging configuration
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_format = '%(asctime)s - %(levelname)s - %(message)s'
log_filename = f'process_etl_{timestamp}.log'
logging.basicConfig(filename=log_filename, level=logging.INFO,format=log_format)

# Now, when you log messages, they will go to the file "process_log.log"
current_time = datetime.now()
logging.info(f"ETL job started at {current_time}")


# Load environment variables
aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name =os.getenv('BUCKET_NAME')
geo_data_url=os.getenv('GEO_DATA_URL')
gdelt_data_url=os.getenv('GDELT_DATA_URL')

# Initialize S3 client with provided credentials
s3_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
s3 = s3_session.client('s3')


def upload_to_s3(dataframe, filename):
    """
    Uploads a pandas DataFrame to a specified S3 bucket.
    
    Args:
    - dataframe (pd.DataFrame): The DataFrame to be uploaded.
    - filename (str): The desired filename for the S3 object.

    Returns:
    - None
    """
    try:
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=filename, Body=csv_buffer.getvalue())
        logging.info(f"Data uploaded to S3: {filename}")
    except NoCredentialsError:
        logging.error("Credentials not available for S3 upload")
    except Exception as e:
        logging.error(f"Error uploading data to S3: {e}")


def download_file(url):
    """
    Downloads a file from the specified URL.

    Args:
    - url (str): URL of the file to be downloaded

    Returns:
    - content of the file if download is successful, else None
    """
    try:
        logging.info(f"Downloading file from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f"HTTP Request failed: {e}")
        return None

# Fetching the GDELT data URL list
try:
    response = requests.get(gdelt_data_url, timeout=30)
    response.raise_for_status()
    data_list = response.text.split('\n')
    export_data_url = next((line.split(' ')[-1] for line in data_list if "export" in line), None)
except requests.RequestException as e:
    logging.error(f"Failed to fetch the data list: {e}")
    export_data_url = None

def extract_content_from_zip(zip_content):
    """
    Extracts content from a given ZIP archive.

    Args:
    - zip_content (Bytes): Content of the ZIP file

    Returns:
    - Decoded content of the CSV file inside the ZIP if successful, else None
    """
    try:
        with ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
            csv_file_name = zip_ref.namelist()[0]
            return zip_ref.read(csv_file_name).decode('utf-8')
    except Exception as e:
        logging.error(f"Error extracting content from zip: {e}")
        return None

def transform_data(content):
    """
    Transforms the raw content into a structured pandas DataFrame.
    
    Args:
    - content (str): Raw content from the downloaded CSV

    Returns:
    - DataFrame after transformation
    """
    try:
        df = pd.read_csv(io.StringIO(content), delimiter='\t', header=None)

        # Selecting columns
        columns = ['GLOBALEVENTID', 'SQLDATE', 'EventCode', 'EventBaseCode', 'EventRootCode', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_Lat', 'ActionGeo_Long', 'DATEADDED', 'SOURCEURL']
        df = df.iloc[:, [0, 1, 26, 27, 28, 52, 53, 56, 57, 59, 60]]
        df.columns = columns

        # Data enrichment with mappings
        df['EventCode'] = df['EventCode'].map(event_codes_map).fillna('Unknown')
        df['EventBaseCode'] = df['EventBaseCode'].map(event_base_codes_map).fillna('Unknown')
        df['EventRootCode'] = df['EventRootCode'].map(event_root_codes_map).fillna('Unknown')
        df['ActionGeo_CountryCode'] = df['ActionGeo_CountryCode'].map(map_fips_to_iso2_map).fillna('Unknown')
        logging.info('Data enrichmemnt completed with no issues')
        return df
    except Exception as e:
        logging.error(f"Error transforming the data: {e}")
        return df

def data_quality_checks(df):
    """
    Conducts data quality checks on the DataFrame.

    Args:
    - df (DataFrame): DataFrame to be checked

    Returns:
    - bool: True if checks passed, False otherwise
    """
    # Check if column count is 10
    if len(df.columns) != 11:
        logging.error(f"Unexpected number of columns in the dataframe. Expected: 11, Found: {len(df.columns)}.")
        return False

    # Check if GLOBALEVENTID has missing values, if yes then remove those rows
    if df['GLOBALEVENTID'].isnull().any():
        logging.error("Missing values detected in the GLOBALEVENTID column. Removing rows with missing values.")
        df.dropna(subset=['GLOBALEVENTID'], inplace=True)

    # Check if GLOBALEVENTID values are unique
    if df['GLOBALEVENTID'].duplicated().any():
        logging.error("Duplicate values detected in the GLOBALEVENTID column.")
        return False

    # Check if ActionGeo_CountryCode is 'US' only
    #if not df['ActionGeo_CountryCode'].eq('US').all():
     ##   return False

    return True

def geospatial_processing(df):
    """
    Processes data geospatially to determine which events took place within the US.

    Args:
    - df (DataFrame): DataFrame containing global event data

    Returns:
    - GeoDataFrame: Subset of data corresponding to events within the US
    """
    try:
        us_counties = gpd.read_file(geo_data_url)
        geometry = [Point(xy) for xy in zip(df['ActionGeo_Long'], df['ActionGeo_Lat'])]
        geo_df = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")  # Setting CRS to match `us_counties`
        us_events = gpd.sjoin(geo_df, us_counties, predicate="within")  # Replacing op with predicate
        logging.info('previewing the us_events data')
        logging.info(us_events.head(10))
        return us_events
    except Exception as e:
        logging.error(f"Geo-processing error: {e}")
        return None

def push_to_postgis(df):
    """
    Loads the data into a PostGIS database.

    Args:
    - df (DataFrame/GeoDataFrame): Data to be loaded

    Returns:
    - None
    """
    DATABASE_USERNAME = os.getenv('DATABASE_USERNAME')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_HOST = os.getenv('DATABASE_HOST')
    DATABASE_PORT = os.getenv('DATABASE_PORT')
    DATABASE_NAME = os.getenv('DATABASE_NAME')
    
    DATABASE_URI = f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}'
    engine = create_engine(DATABASE_URI)
    
    n=10
    logging.info(f'checking the first {n} columns') # check first 10 columns
    logging.info(df.iloc[:,:10].tail(n)) # check first 10 columns
    logging.info(f'checking the last {n} columns')
    logging.info(df.iloc[:,-10:].tail(n))  #check last 10 columns
    
    try:

        #df.to_sql('base_data', engine, if_exists='append', index=False, schema='base_test', dtype={'geometry': Geometry('POINT', srid=4326)})
        df.to_sql('base_data', engine, if_exists='append', index=False, schema='base_test')
        logging.info(f"Data ingested into PostGIS successfully! {df.shape[0]} rows loaded.")
    except Exception as e:
        logging.error(f"Error pushing data to PostGIS: {e}")

def process_data(url):
    start=time.time()
    """
    Orchestrates the entire data processing pipeline.

    Args:
    - url (str): URL of the GDELT data to be processed

    Returns:
    - None
    """
    zip_content = download_file(url)
    if not zip_content:
        return

    csv_content = extract_content_from_zip(zip_content)
    if not csv_content:
        return

    df = transform_data(csv_content)
    if df is None or not data_quality_checks(df):
        logging.error("Data transformation or data quality checks failed.")
        upload_to_s3(df, "data_quality_issues.csv")
        #Will implement an alert here to notify stake holders
        return

    us_events_df = geospatial_processing(df)
    if us_events_df is not None:
        push_to_postgis(us_events_df)
    
    stop=time.time()
    logging.info(f"ETL job finished at {datetime.now()}")
    logging.info(f"ETL job executed in {stop-start} seconds")

# Trigger the data processing pipeline if a valid export data URL is found
if export_data_url:
    process_data(export_data_url)
else:
    logging.error("Couldn't find the required export data URL.")
