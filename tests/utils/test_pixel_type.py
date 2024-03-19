from osgeo import gdal
from edm_store.utils.pixel_type import global_data_type


def test_pixel_type():
    # int -> int32
    src_type = "int"
    datatype = global_data_type.get(src_type)
    assert datatype.rasterio_type == "int32"
    assert datatype.gdal_type == gdal.GDT_Int32

    # float -> float64
    src_type = "float"
    datatype = global_data_type.get(src_type)
    assert datatype.rasterio_type == "float64"
    assert datatype.gdal_type == gdal.GDT_Float64

    # empty or an error will get gdal.GDT_Unknown -> 'uint8'
    datatype = global_data_type.get("")
    assert datatype.gdal_type == gdal.GDT_Unknown
    assert datatype.rasterio_type == "uint8"
    assert (
        global_data_type.get_data_type_name_in_gdal(datatype.gdal_type)
        == datatype.rasterio_type
    )

    datatype = global_data_type.get("float32")
    assert datatype.gdal_type == gdal.GDT_Float32
    assert datatype.rasterio_type == "float32"
    assert global_data_type.get_data_type_name_in_gdal(datatype.gdal_type) == "float32"

    datatype = global_data_type.get("int16")
    assert datatype.gdal_type == gdal.GDT_Int16
    assert datatype.rasterio_type == "int16"
    assert global_data_type.get_data_type_name_in_gdal(datatype.gdal_type) == "int16"
