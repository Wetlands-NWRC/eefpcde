import ee


# Alias type hints
S1DVImage = ee.Image

#################################
# Image preprocessing functions #
#################################
Boxcar = ee.Kernel.square
def boxcar(window_size: int):
    """Create a boxcar kernel of the given size.

    Args:
        window_size: The size of the kernel.

    Returns:
        A kernel of the given size.
    """
    return ee.Kernel.square(window_size, 'pixels', False)


def register(img_list = list[S1DVImage]) -> list[S1DVImage]:
    """ Register a list of images to the first image in the list. """
    master = img_list.pop(0)
    registered = [img.register(master, 10) for img in img_list]
    registered.insert(0, master)
    return registered    


def insert_date(img: S1DVImage):
    """ Insert the date of the image as a property. """
    date = ee.Date(img.get('system:time_start'))
    fmt_date = date.format('YYYY_MM_dd')
    sat_name = 'S1_'
    sat_mode = '_IW'
    eestr = ee.String(sat_name).cat(fmt_date).cat(sat_mode)
    return img.set('filename', eestr)


#################################
# Training Data Pre Processing  #
#################################


def insert_xy(point: ee.Feature):
    """ Insert the x and y coordinates of the feature as properties. """
    coords = point.geometry().coordinates()
    x = ee.Number(coords.get(0))
    y = ee.Number(coords.get(1))
    return point.set('x', x).set('y', y)


#################################
# PROCESSING FUNCTIONS          #
#################################


ImageCollection = list[ee.Image]
TrainingData = ee.FeatureCollection
TimeSeries = list[ee.FeatureCollection]


def generate_time_series(training_data: TrainingData, img_list: ImageCollection) -> TimeSeries:
    """generate a time series of training data from a list of images """
    ts = TimeSeries()
    
    for img in img_list:
        sample = img.sampleRegions(
            collection=training_data,
            tileScale=16,
            scale=10
        ).map(lambda f: f.set('date', img.date().format('YYYY-MM-dd')))
        ts.append(sample)
        sample = None
    return ts


#################################
# POST PROCESSING FUNCTIONS     #
#################################

def export_time_series(time_series: TimeSeries, root:str, bucket: str):
    """ Export a time series of training data to a folder. """
    for i, ts in enumerate(time_series):
        name = f'{root}/training_data/{name}_{i}'
        task = ee.batch.Export.table.toCloudStorage(
            collection=ts,
            bucket=bucket,
            description="",
            fileNamePrefix=name,
            fileFormat='CSV'
        )
        task.start()


def export_image_list_to_cloud(img_list: ImageCollection, bucket: str, root: str):
    """ Export a list of images to a folder. """
    for i, img in enumerate(img_list):
        filename = img.get('filename').getInfo()
        name = f'{root}/img/{filename}/{filename}-'
        task = ee.batch.Export.image.toCloudStorage(
            image=img,
            fileNamePrefix=name, 
            description="",
            bucket=bucket,
            scale=10,
            shardSize=64,
            fileDimensions=[64, 64],
            maxPixels=1e13,
            skipEmptyTiles=True
        )
        task.start()


