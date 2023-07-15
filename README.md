# earth-now
Python scripts to download and decode raw geostationary satellite data in real-time and create a mosaic from the images. Currently used to generate a new global mosaic every 30 minutes at www.earth-now.net

![hippo](https://github.com/jackcop/earth-now/blob/main/looped_512x1024.gif)

Images are pulled from GOES-16, GOES-18, Himawari-9, Meteosat-9, and Meteosat-10. For more information, see the writeup on my github.io page:
https://jackcop.github.io

Images are blended together using custom alpha masks based on satellite zenith angles. You must generate these files on your own, this can be done
by opening `helpers.py` and uncommenting the code near the bottom.

It is recommended that you create a virtual environment in the project directory and use pip to download the required depencencies. 
For each of the following, run:

`pip install <dependency>`

## Dependencies
satpy  
netcdf4  
opencv-python  
boto3  
eumdac  





