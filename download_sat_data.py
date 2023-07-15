import datetime
from datetime import datetime, timezone, timedelta
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pathlib
from glob import glob
from math import floor
import time
import bz2
import eumdac
import shutil
import requests

""" 
    Himawari and GOES data can be found on AWS, but good, preprocessed image files are available in high-resolution from 
    https://www.star.nesdis.noaa.gov/GOES/fulldisk.php?sat=G16

    this code can get the most recently uploaded files for given channels to the appropriate 
    folders in the GOES-16, GOES-18, and Himawari AWS buckets.
"""

def aws_data_download(client, bucket, folder, file_prefix, filename):
    file_dir = folder + filename

    client.download_file(bucket, file_dir, file_prefix + filename)

def eumetsat_get_token():
    key = 'a2KSFPSidmgngwaV0KtVUeWop80a'
    secret = 'Hf7tk5qlevRY5dTjk9PCKFmHrVAa'

    credentials = (key, secret)

    try:
        token = eumdac.AccessToken(credentials)
    except requests.exceptions.HTTPError as error:
        print(f"Error when tryng the request to the server: '{error}'")
    
    return token

def eumetsat_data_download(token, satellite, data_file_path, existing_files):
    datastore = eumdac.DataStore(token)

    if (satellite == 'meteosat_10'):
        #0 degree longitude satellite
        collection_id = 'EO:EUM:DAT:MSG:HRSEVIRI'
    elif (satellite == 'meteosat_9'):
        #45 degree longitude "indian ocean" satellite
        collection_id = 'EO:EUM:DAT:MSG:HRSEVIRI-IODC'
    else:
        raise ValueError('Invalid satellite option.')

    try:    
        selected_collection = datastore.get_collection(collection_id)
    except eumdac.datastore.DataStoreError as error:
        print(f"Error related to the data store: '{error.msg}'")
    except eumdac.collection.CollectionError as error:
        print(f"Error related to the collection: '{error.msg}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")

    product = selected_collection.search().first()
    native_name = f'{product}.nat'

    #if the data file is not already downloaded
    if(not any(native_name in x for x in existing_files)):
        remove_files(existing_files)
        #download the single product
        try:
            with product.open(entry=native_name) as fsrc, \
                open(f'{data_file_path}/{fsrc.name}', mode='wb') as fdst:
                shutil.copyfileobj(fsrc, fdst)
                print(f'Download of file {fsrc.name} finished.')
        except eumdac.product.ProductError as error:
            print(f"Error related to the product '{product}' while trying to download it: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")
    else:
        print (f'File {native_name} already exists.')

def unzip_files(files):
    for file in files:
        zipfile = bz2.BZ2File(file)
        data = zipfile.read()
        newfilepath = file[:-4]
        open(newfilepath, 'wb').write(data)
        rem_file = pathlib.Path(file)
        rem_file.unlink()

def get_latest_channel_files(client, bucket, folder, channel):
    response = client.list_objects_v2(Bucket=bucket, Prefix=folder)
    latest_files = []
    channel_files = []
    modified_dates = []

    for content in response.get('Contents', []):
        if (channel in content['Key']):
            channel_files.append(content['Key'])
            modified_dates.append(content['LastModified'])

    if (bucket == 'noaa-himawari9'):
        for file in channel_files:
            latest_files.append(file.split('/')[-1])
    else:
        for i in range(len(channel_files)):
            if (modified_dates[i] == max(modified_dates)):
                latest_files = [channel_files[i].split('/')[-1]]

    return latest_files

def get_latest_himawari_bucket_folder(client, bucket, file_prefix, floored_timestamp):
    #Find the folder containing the most recent file, then select the folder preceding this one
    #get the timestamp of the current day/hour/minute
    latest_folder = floored_timestamp.strftime('/%Y/%m/%d/%H%M/')
    #determine if there are sufficient files for a full image in this timestamp folder
    #there are 16 channels with ten slices each, for 160 total files
    filepath = file_prefix + latest_folder
    file_response = client.list_objects_v2(Bucket=bucket, Prefix=filepath)

    #if not sufficient files, get the previous folder
    if (len(file_response.get('Contents', [])) < 160):
        previous_timestamp = floored_timestamp - timedelta(minutes=10)
        latest_folder = get_latest_himawari_bucket_folder(client, bucket, file_prefix, previous_timestamp)
    
    return latest_folder

def get_latest_goes_bucket_folder(client, bucket, file_prefix, floored_timestamp):
    #if it is the beginning of a new hour, the file will be in the previous hour's folder
    if (floored_timestamp.strftime('%M') == '00'):
        floored_timestamp -= timedelta(minutes=5)

    latest_folder = floored_timestamp.strftime('/%Y/%j/%H/')
    #get most recent files in this folder
    filepath = file_prefix + latest_folder
    file_response = client.list_objects_v2(Bucket=bucket, Prefix=filepath)

    #if not sufficient files, get recursively run the program pushing it back each time
    if (not file_response.get('Contents', [])):
        previous_timestamp = floored_timestamp - timedelta(minutes=10)
        latest_folder = get_latest_goes_bucket_folder(client, bucket, file_prefix, previous_timestamp)

    return latest_folder
    
def remove_files(files):
    for file in files:
        rem_file = pathlib.Path(file)
        rem_file.unlink()
    
def download_sat_data(satellite, channels, client):
    local_dir_pre = ''
    """Himawari data are quite a bit behind and are sorted into folders containing the minute
    (10 minute intervals). However, based on my experience, upload consistency is poor in this aws bucket.
    Thus, we must utilize a function to get the latest folder uploaded to the Himawari-9 bucket.
    At the beginning of every new hour, GOES data is actually published to the previous folder
    this makes things slightly more complicated than they need to be, but still much better than Himawari"""
    data_file_path = local_dir_pre + f'data/{satellite}/'
    existing_data_files = glob(data_file_path + '*')
    now = datetime.now(timezone.utc)
    #get the amount of time elapsed since the most recent minute multiple of 10
    floored_timestamp = now - timedelta(minutes=(now.minute - floor(now.minute / 10.) * 10.))

    if (satellite == 'goes_east'):
        aws_file_prefix = 'ABI-L1b-RadF'
        bucket = 'noaa-goes16'
        folder = get_latest_goes_bucket_folder(client, bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder

    elif (satellite == 'goes_west'):
        aws_file_prefix = 'ABI-L1b-RadF'
        bucket = 'noaa-goes18'
        folder = get_latest_goes_bucket_folder(client, bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder

    elif (satellite == 'himawari'):
        aws_file_prefix = 'AHI-L1b-FLDK'
        bucket = 'noaa-himawari9'
        folder = get_latest_himawari_bucket_folder(client, bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder

    elif(satellite == 'meteosat_10'):
        token = eumetsat_get_token()
        bucket = 'none'
        filepath = 'none'
        channel = ['none']

    elif(satellite == 'meteosat_9'):
        token = eumetsat_get_token()
        bucket = 'none'
        filepath = 'none'
        channel = ['none']

    else:
        raise ValueError(f'{satellite} is not a valid satellite option. Use "himawari", "goes_east", "goes_west", "meteosat_9", or "meteosat_10" instead.')

    #meteosat only provides all channels at once, so we cannot select specific channels
    if (not 'meteosat' in satellite):
        #remove existing raw data and download the new data
        for channel in channels:
            filenames = get_latest_channel_files(client, bucket, filepath, channel)
            local_ch_filenames = [(data_file_path + i) for i in filenames]
            #himawari is zipped, so the last four characters must be removed
            if (satellite == 'himawari'):
                local_ch_filenames = [i[:-4] for i in local_ch_filenames]

            delete_files = True

            #if channel filenames are not in the existing data files already, download them
            if (not set(local_ch_filenames).issubset(existing_data_files)):
                try:
                    for filename in filenames:
                        aws_data_download(client, bucket, filepath, data_file_path, filename)
                except:
                    #we do not want this to continue attempting to download
                    delete_files = False
                    break
            else:
                print(f'{satellite} {channel} files already exist.')

        if (delete_files):
            remove_files(existing_data_files)
        else:
            updated_file_list = glob(data_file_path + '*')
            rem_files = [i for i in updated_file_list if i not in existing_data_files]
            remove_files(rem_files)

    else:
        eumetsat_data_download(token, satellite, data_file_path, existing_data_files)

    #unzip compressed himawari files and delete the bz2 file
    if (satellite == 'himawari'):
        zipped_files = glob('data/himawari/*.bz2')
        unzip_files(zipped_files)