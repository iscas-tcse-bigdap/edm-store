import os.path

from edm_store.dm.raster import RasterIoBand
from edm_store import config, exist, unlink


def test_raster_band_read():
    file = os.path.abspath("./data/L5-TM-224-001-20060629-LSR-B2.TIF")
    raster_band = RasterIoBand(file)

    assert raster_band.get_band_path() == file
    assert raster_band.datatype() == "int16"
    assert raster_band.get_raster_count() == 1

    for tile_info in raster_band.get_all_tile_infos():
        tile, transform, shape = tile_info
        t1, s1 = raster_band.get_tile_info(tile[0], tile[1])
        assert list(transform) == list(t1)
        assert list(shape) == list(s1)

    assert len(raster_band.get_tiles()) == len(raster_band.get_all_tile_infos())

    assert list(raster_band.read_tile(0, 0).shape) == [2048, 2048]

    transform, shape = raster_band.get_tile_info(0, 0)
    assert list(raster_band.read_tile(0, 0).shape) == [2048, 2048]
    assert list(raster_band.read_region(transform, shape[1], shape[0]).shape) == [
        2048,
        2048,
    ]
    raster_band.close()


def test_raster_band_create():
    config.datasource_mapper["test"] = {"name": "default", "auth": "rw"}
    config.create_allowed = ["test"]
    config.delete_allowed = ["test"]
    file = os.path.abspath("./data/L5-TM-224-001-20060629-LSR-B2.TIF")
    raster_band = RasterIoBand(file)
    new_band = "/edm_store/test/L5-TM-224-001-20060629-LSR-B2.TIF"
    if exist(new_band):
        unlink(new_band)

    assert exist(new_band) == False
    raster_band.create_dataset(new_band, raster_band.datatype(1), 0)
    assert exist(new_band) == True
    unlink(new_band)
    assert exist(new_band) == False
