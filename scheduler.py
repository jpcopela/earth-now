import sched
import time
from download_sat_data import download_sat_data, remove_files
from process_images import generate_image_from_data

g_channels = ['C01', 'C02', 'C03']
h_channels = ['B01', 'B02', 'B03', 'B04']

#this takes ~10:40 to complete... not fast enough, we will try threading
#downloading data: ~4:00
#processing hi-res images: ~6:40

def test():
    try:
        print('Downloading Himawari-9 data...')
        time.sleep(1)
        download_sat_data('himawari', h_channels)
    except:
        raise ValueError('Failed to download Himawari-9 image data.')
    
    try:
        print('Downloading GOES-16 data...')
        time.sleep(1)
        download_sat_data('goes_east', g_channels)
    except:
        raise ValueError('Failed to download GOES-16 image data.')

    try:
        print('Downloading GOES-18 data...')
        time.sleep(1)
        download_sat_data('goes_west', g_channels)
    except:
        raise ValueError('Failed to download GOES-18 image data.')
    
    try:
        print('Decoding Himawari-9 data...')
        time.sleep(1)
        generate_image_from_data('himawari', 'true_color')
    except:
        raise ValueError('Failed to process Himawari-9 image data.')
    
    try:
        print('Decoding GOES-16 data...')
        time.sleep(1)
        generate_image_from_data('goes_east', 'true_color')
    except:
        raise ValueError('Failed to process GOES-16 image data.')

    try:
        print('Decoding GOES-18 data...')
        time.sleep(1)        
        generate_image_from_data('goes_west', 'true_color')
    except:
        raise ValueError('Failed to process GOES-18 image data.')
    
test()