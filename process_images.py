from satpy import Scene
from satpy.resample import get_area_def
from glob import glob
from dask import config
from logging import debug

def generate_image_from_data(satellite, image_type):

    if (image_type != "true_color"):
        raise ValueError('Invalid image option. Use "true_color" instead.')
    
    if (satellite == 'himawari'):
        filepath = 'data/himawari'
        files = glob(filepath + "/*.DAT")
        reader = 'ahi_hsd'
        resample_area = 'none'
        output_file_name = "images/himawari_AHI_F.jpg"
    elif (satellite == 'goes_east'):
        filepath = "./data/goes_east"
        files = glob(filepath + "/*")
        reader = 'abi_l1b'
        resample_area = get_area_def('goes_east_abi_f_4km')
        output_file_name = "images/GOES_16_ABI_F.jpg"
    elif (satellite == 'goes_west'):
        filepath = './data/goes_west'
        files = glob(filepath + "/*")
        reader = 'abi_l1b'
        resample_area = get_area_def('goes_west_abi_f_4km')
        output_file_name = "images/GOES_18_ABI_F.jpg"
    else:
        raise ValueError('Invalid satellite option. Use "himawari", "goes_east", or "goes_west" instead.')

    config.set({"array.chunk-size": "1024kiB"})
    config.set(num_workers=8)

    debug('Using files', files, 'from', filepath)
    
    scn = Scene(filenames=files, reader=reader)
    scn.load(['true_color'], generate=False)

    #new_scn = scn.resample(scn.min_area(), resampler='native', reduce_data=True)
    if (resample_area == 'none'):
        resampled_scn = scn.resample(scn.coarsest_area(), resampler='native', reduce_data=True)
    else:
        resampled_scn = scn.resample(scn.coarsest_area(), resampler='native', reduce_data=False)

    resampled_scn.save_dataset(dataset_id=image_type, filename=output_file_name, fill_value=0)

