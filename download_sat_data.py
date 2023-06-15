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

""" 
    Himawari and GOES data can be found on AWS.
    this code can get the most recently uploaded files for given channels to the appropriate 
    folders in the GOES-16, GOES-18, and Himawari AWS buckets.
"""

def aws_data_download(bucket, folder, file_prefix, filename):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    file_dir = folder + filename

    s3.download_file(bucket, file_dir, file_prefix + filename)

def unzip_files(filepath):
    zipfile = bz2.BZ2File(filepath)
    data = zipfile.read()
    newfilepath = filepath[:-4]
    open(newfilepath, 'wb').write(data)

def get_latest_channel_files(bucket, folder, channel):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)

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

def get_latest_himawari_bucket_folder(bucket, file_prefix, floored_timestamp):
#we will have to find the folder containing the most recent file, then select the folder preceding this one
#to save time we will search for files beginning 40 minutes before now, if the folder does not exist or is empty,
#we will search the previous day's entries.

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

#get the timestamp of the current day/hour/minute
    latest_folder = floored_timestamp.strftime('/%Y/%m/%d/%H%M/')

#determine if there are sufficient files for a full image in this timestamp folder
#there are 16 channels with ten slices each, for 160 total files
    filepath = file_prefix + latest_folder
    file_response = s3.list_objects_v2(Bucket=bucket, Prefix=filepath)

#if not sufficient files, get the previous folder
    if (len(file_response.get('Contents', [])) < 160):
        previous_timestamp = floored_timestamp - timedelta(minutes=10)
        latest_folder = previous_timestamp.strftime('/%Y/%m/%d/%H%M/')
        previous_filepath = file_prefix + latest_folder

    #if previous folder doesn't work, push back to second to last folder
        if (len(s3.list_objects_v2(Bucket=bucket, Prefix=previous_filepath)) < 160):
            previous_timestamp = previous_timestamp - timedelta(minutes=10) #push it back 20 minutes
            latest_folder = previous_timestamp.strftime('/%Y/%m/%d/%H%M/')
        else:
            raise ValueError('Unable to locate recent Himawari-9 images.')

    return latest_folder

def get_latest_goes_bucket_folder(bucket, file_prefix, floored_timestamp):
   
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    latest_folder = floored_timestamp.strftime('/%Y/%j/%H/')

#get most recent files in this folder
    filepath = file_prefix + latest_folder
    file_response = s3.list_objects_v2(Bucket=bucket, Prefix=filepath)

#if not sufficient files, get the previous folder
    if (not file_response.get('Contents', [])):
        previous_timestamp = floored_timestamp - timedelta(minutes=10)
        latest_folder = previous_timestamp.strftime('/%Y/%j/%H/')
        previous_filepath = file_prefix + latest_folder

    #if previous folder doesn't work, push back to second to last folder
        if (not (s3.list_objects_v2(Bucket=bucket, Prefix=previous_filepath)).get('Contents', [])):
            previous_timestamp = previous_timestamp - timedelta(minutes=10) #push it back 20 minutes
            latest_folder = previous_timestamp.strftime('/%Y/%j/%H/')
        else:
            raise ValueError('Unable to locate recent GOES images.')

    return latest_folder
    

def remove_files(files):
    for file in files:
        rem_file = pathlib.Path(file)
        rem_file.unlink()
    

#this way leaves it open to downloading other channels
def download_sat_data(satellite, channels):
    required_goes_channels = ['C01', 'C02', 'C03']
    required_himawari_channels = ['B01', 'B02', 'B03', 'B04']

    """
    Himawari data are quite a bit behind and are sorted into folders containing the minute
    (10 minute intervals). However, based on my experience, upload consistency is poor in this aws bucket.
    Thus, we must utilize a function to get the latest folder uploaded to the Himawari-9 bucket.
    
    At the beginning of every new hour, GOES data is actually published to the previous folder
    this makes things slightly more complicated than they need to be, but still much better than Himawari
    """

    now = datetime.now(timezone.utc)

    #get the amount of time elapsed since the most recent minute multiple of 10
    floored_timestamp = now - timedelta(minutes=(now.minute - floor(now.minute / 10.) * 10.))

    if (satellite == 'goes_east'):
        if (not all(i in required_goes_channels for i in channels)):
            raise ValueError('Insufficient or inappropriate channels for true color image output.')
        
        local_file_prefix = 'data/goes_east/'
        existing_data_files = glob('data/goes_east/*')
        aws_file_prefix = 'ABI-L1b-RadF'
        bucket = 'noaa-goes16'
        folder = get_latest_goes_bucket_folder(bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder


    elif (satellite == 'goes_west'):
        if (not all(i in required_goes_channels for i in channels)):
            raise ValueError('Insufficient or inappropriate channels for true color image output.')
        
        local_file_prefix = 'data/goes_west/'
        existing_data_files = glob('data/goes_west/*')
        aws_file_prefix = 'ABI-L1b-RadF'
        bucket = 'noaa-goes18'
        folder = get_latest_goes_bucket_folder(bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder

        
    elif (satellite == 'himawari'):
        if (not all(i in required_himawari_channels for i in channels)):
            raise ValueError('Insufficient or inappropriate channels for true color image output.')
        
        local_file_prefix = 'data/himawari/'
        existing_data_files = glob('data/himawari/*')
        aws_file_prefix = 'AHI-L1b-FLDK'
        bucket = 'noaa-himawari9'
        folder = get_latest_himawari_bucket_folder(bucket, aws_file_prefix, floored_timestamp)
        filepath = aws_file_prefix + folder
    else:
        raise ValueError('Invalid satellite option. Use "himawari", "goes_east", or "goes_west" instead.')

    #remove existing raw data and see what the timestamp is for the images we have (if any)
    if existing_data_files:
        remove_files(existing_data_files)

    sat_isPresent = False
    images = glob('images/*')

    for channel in channels:
        filenames = get_latest_channel_files(bucket, filepath, channel)
        for filename in filenames:
            try:
                print('Downloading:', bucket + '/' + filepath + filename)
                aws_data_download(bucket, filepath, local_file_prefix, filename)

                #unzip compressed himawari files and delete the bz2 file
                if (satellite == 'himawari'):
                    unzip_files(local_file_prefix + filename)
                    rem_file = pathlib.Path(local_file_prefix + filename)
                    rem_file.unlink()
            except:
                print("Unable to locate file: ", filename)
                time.sleep(5)
                download_sat_data(satellite, channels)