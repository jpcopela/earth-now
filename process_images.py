from satpy import Scene
from satpy.modifiers import angles
from satpy.utils import debug_on 
from satpy.resample import get_area_def
from pyresample import create_area_def
from download_sat_data import remove_files
from glob import glob
from dask import config
from xarray import DataArray
import numpy as np
from PIL import Image
import cv2
from datetime import datetime, timedelta

#we are on the local machine, use relative paths
local_dir_pre = '' 
website_dir_pre = ''

def generate_image_from_data(satellite, image_types, get_projections):
    config.set({"array.chunk-size": "1024kiB"})
    config.set(num_workers=2)

    data_files = local_dir_pre + f'data/{satellite}/'
    existing_data_files = glob(data_files + '*')

    if (get_projections):
        output_file_name = website_dir_pre + f'images/projected/{satellite}_projected'
    else:
        output_file_name = website_dir_pre + f'images/fd/latest_{satellite}_FD'

    if (satellite == 'himawari'):
        config.set({"array.chunk-size": "1000kiB"})
        mode = 'native'
        reader = 'ahi_hsd'
        resample_area = create_area_def("himawari_area_def", area_extent=(-5500000.0355, -5500000.0355, 5500000.0355, 5500000.0355), projection='+proj=geos +h=35785831.0 +lon_0=140.7 +sweep=y', height=2750, width=2750)

    elif (satellite == 'goes_east'):
        reader = 'abi_l1b'
        mode = 'native'
        resample_area = get_area_def('goes_east_abi_f_4km')

    elif (satellite == 'goes_west'):
        reader = 'abi_l1b'
        mode = 'native'
        resample_area = get_area_def('goes_west_abi_f_4km')

    elif (satellite == 'meteosat_10'):
        resample_area = 'none'#'msg_seviri_fes_3km'
        mode = 'native'
        reader = 'seviri_l1b_native'

    elif (satellite == 'meteosat_9'):
        resample_area = 'none'#'msg_seviri_iodc_3km'
        mode = 'native'
        reader = 'seviri_l1b_native'

    else:
        raise ValueError('Invalid satellite option. Use "himawari", "goes_east", "goes_west", meteosat_10, or meteosat_9 instead.')

    scn = Scene(filenames=existing_data_files, reader=reader)
    
    for composite in image_types:
        scn.load([composite], generate=False)

    if (resample_area == 'none'):
        resample_area = scn.coarsest_area()

    resampled_scn = scn.resample(resample_area, resampler=mode, reduce_data=False)

    if (get_projections == True):
        #resample to projection
        resample_area = get_area_def('worldeqc3km73')

        resampled_scn = resampled_scn.resample(resample_area, resampler='nearest', reduce_data=False)

    try:
        for composite in image_types:
            print(composite)
            if (composite == 'night_ir_alpha'):
                resampled_scn.save_dataset(dataset_id=composite, filename=output_file_name + '_ir.png')
            else:
                resampled_scn.save_dataset(dataset_id=composite, filename=output_file_name + '.png')
    except:
        raise ValueError(f'Failed to save {satellite} images.')

    #remove_files(existing_data_files)

def create_alpha_masks(satellite, scn, image_type):
    #it is good practice to send only individual channels to this function because Himawari breaks
    #for composites, but works for single channels. It is also much faster to do it this way
    zenith_angles = angles.get_satellite_zenith_angle(scn[image_type])
    angle = zenith_angles.to_numpy().astype(dtype=np.float32)
    shape = angle.shape
    alpha_vals = np.empty(shape).astype(dtype=np.float32)
    lim = 70.
    max_angle = 85.

    #code to generate an "alpha image". Angle values are normalized to between a limit value
    #and a maximum angle value. The fine tuning of these parameters allows one to create an
    #alpha gradient near the edge of the image.
    for i in range(0, shape[0]):
        for j in range(0, shape[1]):
            if (angle[i, j] > lim and angle[i, j] < max_angle):
                alpha_vals[i, j] = 255 * (1. - ((angle[i, j] - lim) / (max_angle - lim)))
            elif(angle[i, j] > max_angle):
                alpha_vals[i, j] = 0
            else:
                alpha_vals[i, j] = 255
    
    np.savetxt(f'images/projected/blended_overlays/{satellite}_alpha_mask.txt', alpha_vals)

def apply_blending_masks(satellite):
    jpg = cv2.imread(website_dir_pre + f'images/projected/{satellite}_projected.jpg')
    # First create the image with alpha channel
    rgba = cv2.cvtColor(jpg, cv2.COLOR_RGB2RGBA)
    alpha_vals = np.loadtxt(website_dir_pre + f'images/projected/blended_overlays/{satellite}_alpha_mask.txt')
    # Then assign the mask to the last channel of the image
    rgba[:, :, 3] = alpha_vals
    cv2.imwrite(website_dir_pre + f'images/projected/{satellite}_projected.png', rgba)

def combine_images(image1, image2, filename):
    background = Image.open(image1)
    foreground = Image.open(image2)
    Image.alpha_composite(background, foreground).save(filename)

def stitch_images():
    goes_east = website_dir_pre +   'images/projected/goes_east_projected.png'
    goes_west = website_dir_pre +   'images/projected/goes_west_projected.png'
    himawari =  website_dir_pre +   'images/projected/himawari_projected.png'
    meteosat_9 = website_dir_pre +  'images/projected/meteosat_9_projected.png'
    meteosat_10 = website_dir_pre + 'images/projected/meteosat_10_projected.png'
    goes_comb = website_dir_pre + 'images/projected/tmp/goes_comb.png'
    gh_comb = website_dir_pre +   'images/projected/tmp/gh_comb.png'
    m_comb = website_dir_pre +    'images/projected/tmp/m_comb.png'
    stitched = website_dir_pre +  'images/projected/global_mosaic.png'
    background = website_dir_pre + 'images/projected/blended_overlays/background.png'

    #first, combine goes west and goes east images
    #then combine this image with himawari
    #then combine meteosat images
    #then overlay the goes/himawari images on top of meteosat ones.
    combine_images(goes_east, goes_west, goes_comb)
    combine_images(goes_comb, himawari, gh_comb)
    combine_images(meteosat_9, meteosat_10, m_comb)
    combine_images(m_comb, gh_comb, stitched)
    rem_files = glob(website_dir_pre + 'images/projected/tmp/*')
    remove_files(rem_files)
    #Load .png image
    image = cv2.imread(stitched)
    out_name = website_dir_pre + 'images/projected/global_mosaic.jpg'
    #combine_images(background, stitched, stitched)
    cv2.imwrite(out_name, image, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
    remove_files([stitched])

def to_jpg(file):
    #combine with black background
    combine_images(website_dir_pre + 'images/projected/blended_overlays/background.png', file + '.png', file + '.png')
    image = cv2.imread(file + '.png')
    # convert to jpg
    cv2.imwrite(file + '.jpg', image, [int(cv2.IMWRITE_JPEG_QUALITY), 100])

def process_images(satellite, image_type, generate_projections : bool):
    generate_image_from_data(satellite, image_type, generate_projections)

    if(generate_projections == True):
        base_img = website_dir_pre + f'images/projected/{satellite}_projected'
        #combine visible and ir images
        combine_images(base_img + '_ir.png', base_img + '.png', base_img + '.png')
        remove_files([base_img + '_ir.png'])
        #then convert to jpg and apply the blending mask
        to_jpg(base_img)
        files = [website_dir_pre + base_img + '.jpg']
        apply_blending_masks(satellite)
        remove_files(files)
        
def generate_background():
    data = np.full((2048, 4096, 3), 0, dtype=np.uint8)
    jpg = Image.fromarray(data, 'RGB')
    jpg.save('images/projected/blended_overlays/background.jpg')
    jpg = cv2.imread('images/projected/blended_overlays/background.jpg')
    rgba = cv2.cvtColor(jpg, cv2.COLOR_RGB2RGBA)
    alpha_vals = np.full((2048, 4096), 255)
    # Then assign the mask to the last channel of the image
    rgba[:, :, 3] = alpha_vals
    cv2.imwrite('images/projected/blended_overlays/background.png', rgba)
    remove_files(['images/projected/blended_overlays/background.jpg'])