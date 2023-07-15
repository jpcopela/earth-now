from datetime import datetime
from process_images import stitch_images
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from satellites import GOES, Himawari, Meteosat

goes_east = GOES('goes_east', ['true_color_day', 'night_ir_alpha'], True)
goes_west = GOES('goes_west', ['true_color_day', 'night_ir_alpha'], True)
himawari = Himawari('himawari', ['true_color_day', 'night_ir_alpha'], True)
meteosat_9 = Meteosat('meteosat_9', ['natural_color_day', 'night_ir_alpha'], True)
meteosat_10 = Meteosat('meteosat_10', ['natural_color_day', 'night_ir_alpha'], True)

satellites = [goes_east, goes_west, himawari, meteosat_9, meteosat_10]

def download(satellite):
    try:
        print(f'Downloading {satellite.satellite} data...')
        satellite.download_data()
    except:
        raise ValueError(f'Failed to download {satellite.satellite} data.')
    
def process(satellite):
    try:
        print(f'Processing {satellite.satellite} into {satellite.composites} image.')
        satellite.process_images()
    except:
        raise ValueError(f'Failed to process {satellite.composites} image.')

def parallel_activities():
    cpus = cpu_count()

    try:
        cpus = cpu_count()

        pool = ThreadPool(cpus)
        results = pool.imap_unordered(download, satellites)

        for result in results:
            pass

    except:
        raise ValueError('Failed to create thread pool for downloads.')
    
    for satellite in satellites:
        satellite.process_images()
    
    stitch_images()

t1 = datetime.now()
parallel_activities()
t2 = datetime.now()
delta = t2 - t1

print('#########################################################')
print('Finished! Elapsed time: ', (delta.total_seconds()) / 60.)