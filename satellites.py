from satpy import Scene
from satpy import config
from satpy.resample import get_area_def
from pyresample import create_area_def
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import eumdac
import requests
import shutil
from glob import glob
from math import floor
from datetime import datetime, timezone, timedelta
from PIL import Image
import cv2
import numpy as np
from pathlib import Path

#create a generic class for processing satellite data
#this class will be inherited by the specific satellite classes
class Satellite:
    def __init__(self, satellite, composites, get_projections=False) -> None:
        #generate the required channels for the satellite based on the 
        #desired image types (composites)
        self.satellite = satellite
        self.composites = composites
        self.channels = self._generate_channels()
        self.get_projections = get_projections
        self.local_dir_pre = ''
        self.website_dir_pre = ''
        self.data_file_path = self.local_dir_pre + f'data/{self.satellite}/'
        self.existing_data_files = glob(self.data_file_path + '*')

    def process_images(self):
        self._generate_image_from_data()

        if(self.get_projections == True):
            base_img = self.website_dir_pre + f'images/projected/{self.satellite}_projected'
            #combine visible and ir images
            Satellite._combine_images(base_img + '_ir.png', base_img + '.png', base_img + '.png')
            Satellite._remove_files([base_img + '_ir.png'])
            #then convert to jpg and apply the blending mask
            self._to_jpg(base_img)
            files = [self.website_dir_pre + base_img + '.jpg']
            self._apply_blending_masks()
            Satellite._remove_files(files)

    def _generate_channels(self):
        #generate the required channels for the satellite based on the 
        #desired image types (composites)
        required_channels = []

        if (self.satellite == 'himawari'):
            for composite in self.composites:
                if ('natural_color' in composite):
                    required_channels.append(['B02', 'B03', 'B04', 'B05'])
                elif ('true_color' in composite):
                    required_channels.append(['B01', 'B02', 'B03', 'B04'])
                elif (composite == 'night_ir_alpha'):
                     required_channels.append(['B07', 'B13', 'B15'])
        elif ('goes' in self.satellite):
            for composite in self.composites:
                if ('natural_color' in composite):
                    required_channels.append(['C05', 'C02', 'C03'])
                elif ('true_color' in composite):
                    required_channels.append(['C01', 'C02', 'C03'])
                elif (composite == 'night_ir_alpha'):
                    required_channels.append(['C07', 'C13', 'C15'])
        elif ('meteosat' in self.satellite):
            return ['none']
        else:
            raise ValueError('Invalid satellite option. Use "himawari", "goes_east", "goes_west", meteosat_10, or meteosat_9 instead.')
        
        total_channels = []
        for channels in required_channels:
            for channel in channels:
                total_channels.append(channel)
        
        return total_channels
    
    def _get_satpy_kwargs(self):
        if (self.satellite == 'himawari'):
            config.set({"array.chunk-size": "1000kiB"})
            mode = 'native'
            reader = 'ahi_hsd'
            resample_area = create_area_def("himawari_area_def", area_extent=(-5500000.0355, -5500000.0355, 5500000.0355, 5500000.0355), projection='+proj=geos +h=35785831.0 +lon_0=140.7 +sweep=y', height=2750, width=2750)

        elif (self.satellite == 'goes_east'):
            reader = 'abi_l1b'
            mode = 'native'
            resample_area = get_area_def('goes_east_abi_f_4km')

        elif (self.satellite == 'goes_west'):
            reader = 'abi_l1b'
            mode = 'native'
            resample_area = get_area_def('goes_west_abi_f_4km')

        elif (self.satellite == 'meteosat_10'):
            resample_area = 'none'#'msg_seviri_fes_3km'
            mode = 'native'
            reader = 'seviri_l1b_native'

        elif (self.satellite == 'meteosat_9'):
            resample_area = 'none'#'msg_seviri_iodc_3km'
            mode = 'native'
            reader = 'seviri_l1b_native'
        else:
            raise ValueError('Invalid satellite option. Use "himawari", "goes_east", "goes_west", meteosat_10, or meteosat_9 instead.')
        
        kwargs = {'mode': mode, 'reader': reader, 'resample_area': resample_area}
        return kwargs    
       
    def _generate_image_from_data(self):
        config.set({"array.chunk-size": "1024kiB"})
        config.set(num_workers=2)

        #we must refresh the list of existing data files in case new files were downloaded
        existing_data_files = glob(self.data_file_path + '*')

        if (self.get_projections):
            output_file_name = self.website_dir_pre + f'images/projected/{self.satellite}_projected'
        else:
            output_file_name = self.website_dir_pre + f'images/fd/latest_{self.satellite}_FD'

        kwargs = self._get_satpy_kwargs()
        scn = Scene(filenames=existing_data_files, reader=kwargs['reader'])

        for composite in self.composites:
            scn.load([composite], generate=False)
        
        if (kwargs['resample_area'] == 'none'):
            kwargs['resample_area'] = scn.coarsest_area()

        resampled_scn = scn.resample(kwargs['resample_area'], resampler=kwargs['mode'], reduce_data=False)

        if (self.get_projections == True):
            #resample to projection
            kwargs['resample_area'] = get_area_def('worldeqc3km73')
            resampled_scn = resampled_scn.resample(kwargs['resample_area'], resampler='nearest', reduce_data=False)

        try:
            for composite in self.composites:
                print(composite)
                if (composite == 'night_ir_alpha'):
                    resampled_scn.save_dataset(dataset_id=composite, filename=output_file_name + '_ir.png')
                else:
                    resampled_scn.save_dataset(dataset_id=composite, filename=output_file_name + '.png')
        except:
            raise ValueError(f'Failed to save {self.satellite} images.')
        
    def _combine_images(image1, image2, filename):
        background = Image.open(image1)
        foreground = Image.open(image2)
        Image.alpha_composite(background, foreground).save(filename)

    def _apply_blending_masks(self):
        website_dir_pre = self.website_dir_pre
        satellite = self.satellite
        jpg = cv2.imread(website_dir_pre + f'images/projected/{satellite}_projected.jpg')
        # First create the image with alpha channel
        rgba = cv2.cvtColor(jpg, cv2.COLOR_RGB2RGBA)
        alpha_vals = np.loadtxt(website_dir_pre + f'images/projected/blended_overlays/{satellite}_alpha_mask.txt')
        # Then assign the mask to the last channel of the image
        rgba[:, :, 3] = alpha_vals
        cv2.imwrite(website_dir_pre + f'images/projected/{satellite}_projected.png', rgba)

    def _to_jpg(self, file):
        website_dir_pre = self.website_dir_pre
        #combine with black background
        Satellite._combine_images(website_dir_pre + 'images/projected/blended_overlays/background.png', file + '.png', file + '.png')
        image = cv2.imread(file + '.png')
        # convert to jpg
        cv2.imwrite(file + '.jpg', image, [int(cv2.IMWRITE_JPEG_QUALITY), 100])

    def _remove_files(files):
        for file in files:
            rem_file = Path(file)
            rem_file.unlink()

class Himawari(Satellite):
    def __init__(self, satellite, composites, get_projections=False) -> None:
        super().__init__(satellite, composites, get_projections)
        self.bucket = 'noaa-himawari9'
        self.aws_prefix = 'AHI-L1b-FLDK'
        self.client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        self.get_projections = get_projections

    def download_data(self):
        now = datetime.now(timezone.utc)
        #get the amount of time elapsed since the most recent minute multiple of 10
        floored_timestamp = now - timedelta(minutes=(now.minute - floor(now.minute / 10.) * 10.))        
        folder = self._get_latest_bucket_folder(floored_timestamp)
        existing_data_files = self.existing_data_files
        aws_filepath = self.aws_prefix + folder
        files = []

        for channel in self.channels:
            file_list = self._get_latest_channel_files(aws_filepath, channel)

            for file in file_list:
                files.append(file.split('/')[-1])
                
        #remove existing raw data and download the new data
        #himawari is zipped, so the last four characters must be removed
        local_ch_filenames = [self.data_file_path + i for i in files]
        delete_files = True
        #if channel filenames are not in the existing data files already, download them
        if (not existing_data_files or not set(local_ch_filenames).issubset(existing_data_files)):
            try:
                for filename in files:
                    self._aws_data_download(aws_filepath + filename, self.data_file_path + filename)
            except:
                #we do not want this to continue attempting to download
                delete_files = False
                
        else:
            print(f'{self.satellite} files already exist.')
            delete_files = False

        if (delete_files):
            Satellite._remove_files(existing_data_files)
        else:
            updated_file_list = glob(self.data_file_path + '*')
            rem_files = [i for i in updated_file_list if i not in existing_data_files]
            Satellite._remove_files(rem_files)
        
    def _get_latest_bucket_folder(self, floored_timestamp):
        #Find the folder containing the most recent file, then select the folder preceding this one
        #get the timestamp of the current day/hour/minute
        latest_folder = floored_timestamp.strftime('/%Y/%m/%d/%H%M/')
        #determine if there are sufficient files for a full image in this timestamp folder
        #there are 16 channels with ten slices each, for 160 total files
        filepath = self.aws_prefix + latest_folder
        file_response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=filepath)

        #if not sufficient files, get the previous folder
        if (len(file_response.get('Contents', [])) < 160):
            previous_timestamp = floored_timestamp - timedelta(minutes=10)
            latest_folder = self._get_latest_bucket_folder(previous_timestamp)
        
        return latest_folder
    
    def _get_latest_channel_files(self, folder, channel):
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=folder)
        channel_files = []

        for content in response.get('Contents', []):
            if (channel in content['Key']):
                channel_files.append(content['Key'])

        channel_files = [i.split('/')[-1] for i in channel_files]
        return channel_files
    
    def _aws_data_download(self, aws_filepath, local_path):
        self.client.download_file(self.bucket, aws_filepath, local_path)

#the GOES class is used for both GOES-East and GOES-West
class GOES(Satellite):
    def __init__(self, satellite, composites, get_projections=False) -> None:
        super().__init__(satellite, composites, get_projections)

        if (satellite == 'goes_east'):
            self.bucket = 'noaa-goes16'

        if (satellite == 'goes_west'):
            self.bucket = 'noaa-goes18'

        self.aws_prefix = 'ABI-L1b-RadF'
        self.client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    def download_data(self):
        now = datetime.now(timezone.utc)
        #get the amount of time elapsed since the most recent minute multiple of 10
        floored_timestamp = now - timedelta(minutes=(now.minute - floor(now.minute / 10.) * 10.))        
        folder = self._get_latest_bucket_folder(floored_timestamp)
        existing_data_files = self.existing_data_files
        aws_filepath = self.aws_prefix + folder
        files = []

        for channel in self.channels:
            file_list = self._get_latest_channel_files(aws_filepath, channel)

            for file in file_list:
                files.append(file.split('/')[-1])
                
        #remove existing raw data and download the new files
        local_ch_filenames = [self.data_file_path + i for i in files]
        delete_files = True

        #if channel filenames are not in the existing data files already, download them
        if (not existing_data_files or not set(local_ch_filenames).issubset(existing_data_files)):
            try:
                for filename in files:
                    self._aws_data_download(aws_filepath + filename, self.data_file_path + filename)
            except:
                #we do not want this to continue attempting to download
                delete_files = False
                
        else:
            print(f'{self.satellite} files already exist.')
            delete_files = False

        if (delete_files):
            Satellite._remove_files(existing_data_files)
        else:
            updated_file_list = glob(self.data_file_path + '*')
            rem_files = [i for i in updated_file_list if i not in existing_data_files]
            Satellite._remove_files(rem_files)
        
    def _get_latest_bucket_folder(self, floored_timestamp):
        #if it is the beginning of a new hour, the file will be in the previous hour's folder
        if (floored_timestamp.strftime('%M') == '00'):
            floored_timestamp -= timedelta(minutes=5)

        latest_folder = floored_timestamp.strftime('/%Y/%j/%H/')
        #get most recent files in this folder
        filepath = self.aws_prefix + latest_folder
        file_response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=filepath)
        #goes files are uploaded to the hour's folder in ten minute intervals
        #we just need to find the most recent upload. So we look for the most recent folder
        #and if empty, recursively run the program pushing it back each time
        if (not file_response.get('Contents', [])):
            previous_timestamp = floored_timestamp - timedelta(minutes=10)
            latest_folder = self._get_latest_bucket_folder(previous_timestamp)

        return latest_folder
    
    def _get_latest_channel_files(self, aws_filepath, channel):
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=aws_filepath)
        latest_files = []
        channel_files = []
        modified_dates = []

        #loop through the hour's files and make a list of all files for the desired channel
        #then find the most recent file in this list
        for content in response.get('Contents', []):
            if (channel in content['Key']):
                channel_files.append(content['Key'])
                modified_dates.append(content['LastModified'])

        for i in range(len(channel_files)):
            if (modified_dates[i] == max(modified_dates)):
                latest_files = [channel_files[i].split('/')[-1]]

        return latest_files
    
    def _aws_data_download(self, aws_filepath, local_path):
        self.client.download_file(self.bucket, aws_filepath, local_path)

#the Meteosat class is used for both Meteosat-9 and Meteosat-10,
#however, native files have all channels by default, so the channels attribute is set to 'none'
class Meteosat(Satellite):
    def __init__(self, satellite, composites, get_projections=False) -> None:
        super().__init__(satellite, composites, get_projections)
        self.token = self._eumetsat_get_token()

    def download_data(self):
        satellite = self.satellite
        existing_data_files = self.existing_data_files
        token = self.token
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
        if(not any(native_name in x for x in existing_data_files)):
            Satellite._remove_files(existing_data_files)
            #download the single product
            try:
                with product.open(entry=native_name) as fsrc, \
                    open(f'{self.data_file_path}/{fsrc.name}', mode='wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                    print(f'Download of file {fsrc.name} finished.')
            except eumdac.product.ProductError as error:
                print(f"Error related to the product '{product}' while trying to download it: '{error.msg}'")
            except requests.exceptions.RequestException as error:
                print(f"Unexpected error: {error}")
        else:
            print (f'File {native_name} already exists.')

    def _eumetsat_get_token(self):
        key = 'your key here'
        secret = 'your secret here'
        credentials = (key, secret)

        try:
            token = eumdac.AccessToken(credentials)
        except requests.exceptions.HTTPError as error:
            print(f"Error when tryng the request to the server: '{error}'")
        
        return token




