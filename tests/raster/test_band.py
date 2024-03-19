import os.path

import numpy as np

from edm_store import config, exist, unlink, Dataset
from edm_store.dm.raster import RasterIoBand

base_store_type = config.base_store_type
band_name = "/edm_store/test/L5-TM-224-001-20060629-LSR-B2.BAND"
file_name = os.path.abspath("./data/L5-TM-224-001-20060629-LSR-B2.TIF")


def setup_module():
    config.datasource_mapper_list["test"] = {"name": "default", "auth": "rw"}
    config.create_allowed.append("test")
    config.delete_allowed.append("test")
    config.storage_config["test_fs"] = {
        "BASE_DIRECTORY": "/opt/test/data/",
        "CONSTRUCT": "fs",
    }
    config.base_store_type = "test_fs"

    raster_band = RasterIoBand(file_name)

    if exist(band_name):
        unlink(band_name)

    raster_band.create_dataset(
        band_name, raster_band.datatype(1), raster_band.get_nodata_value()
    )
    dst = Dataset(band_name)
    for tile in raster_band.get_tiles():
        dst.write_tile(tile[0], tile[1], raster_band.read_tile(tile[0], tile[1]))
    raster_band.close()


def teardown_module():
    if exist(band_name):
        unlink(band_name)

    config.create_allowed.remove("test")
    config.delete_allowed.remove("test")
    config.datasource_mapper_list["test"] = {"name": "default", "auth": "rw"}

    config.storage_config["test_fs"] = {
        "BASE_DIRECTORY": "/opt/test/data/",
        "CONSTRUCT": "fs",
    }
    config.base_store_type = base_store_type


def test_band_read():
    dst = Dataset(band_name)
    dst.get_tile_info(0, 0)

    assert dst.get_band_path() == band_name
    assert dst.datatype == "int16"
    assert dst.get_raster_count() == 1

    for tile_info in dst.get_all_tile_infos():
        tile, transform, shape = tile_info
        t1, s1 = dst.get_tile_info(tile[0], tile[1])
        assert list(transform) == list(t1)
        assert list(shape) == list(s1)

    assert len(dst.get_tiles()) == len(dst.get_all_tile_infos())

    assert list(dst.read_tile(0, 0).shape) == [2048, 2048]

    transform, shape = dst.get_tile_info(0, 0)
    assert list(dst.read_tile(0, 0).shape) == [2048, 2048]
    assert list(dst.read_region(transform, shape[1], shape[0]).shape) == [2048, 2048]


def test_band_write():
    dst = Dataset(band_name)
    dst.get_tile_info(0, 0)
    data = np.zeros((2048, 2048), dtype=np.int16)
    assert dst.write_tile(0, 0, data)[0] == True
    assert np.asarray(dst.read_tile(0, 0) == data).all()

    transform, shape = dst.get_tile_info(0, 0)
    data = np.empty((4096, 4096), dtype=np.int16)
    data.fill(1)
    result = np.empty((2048, 2048), dtype=np.int16)
    result.fill(1)
    assert dst.write_region(transform, data)
    assert np.asarray(dst.read_tile(0, 0) == result).all()
    assert np.asarray(dst.read_tile(1, 0) == result).all()
    assert np.asarray(dst.read_tile(0, 1) == result).all()
    assert np.asarray(dst.read_tile(1, 1) == result).all()


def test_band_create():
    dst = Dataset(band_name)
    new_band = "/edm_store/test/temp.BAND"
    if exist(band_name):
        unlink(band_name)
    assert exist(new_band) == False

    dst.create_dataset(new_band, dst, dst.nodata)
    assert exist(new_band)
    unlink(new_band)
    assert exist(new_band) == False
