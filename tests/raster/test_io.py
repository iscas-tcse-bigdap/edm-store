import os.path

import numpy as np


from edm_store.dm.raster._io import (
    reproject_by_gdal, read_from_access_path, _from_memory,

)
from edm_store.dm.vector.core import gen_geobox
from edm_store.utils.pixel_type import global_data_type
from edm_store.utils.tools import get_resample_method

_transform = [12834619, 30, 0, 5011732, 0, -30]
_shape = [2000, 2000]
_proj = "EPSG:3857"


def gen_random_array(
    x_size: int = 10, y_size: int = 10, min_value: int = 0, max_value: int = 100
) -> np.ndarray:
    return np.random.randint(min_value, max_value, (y_size, x_size))


def test_read_from_url():
    file = os.path.abspath("./data/test.tif")
    data = read_from_access_path(file, [0, 0, 30, 20])
    assert list(data.shape) == [20, 30]


def test_reproject():
    array = gen_random_array(20, 30)
    geo = gen_geobox(_transform, array.shape, s_crs="EPSG:3857")
    geo = geo.transform("EPSG:4326")
    min_x, max_x, min_y, max_y = geo.export_to_ogr_geometry().GetEnvelope()

    another_transform = [min_x, (max_x - min_x) / 60, 0, max_y, 0, (max_y - min_y) / 60]
    another_shape = [60, 60]
    result = reproject_by_gdal(
        array,
        _transform,
        _proj,
        0,
        array.shape,
        another_transform,
        "EPSG:4326",
        another_shape,
        0,
        global_data_type.get("int32").gdal_type,
        get_resample_method(),
    )
    assert list(result.shape) == another_shape


def test_read_from_bytes():
    with open(os.path.abspath("./data/test.tif"), "rb") as file:
        d1 = _from_memory(file.read(), [0, 0, 30, 20])
    d2 = read_from_access_path(os.path.abspath("./data/test.tif"), [0, 0, 30, 20])
    assert (d1 == d2).all()
