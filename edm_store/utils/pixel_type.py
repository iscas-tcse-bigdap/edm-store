# ====================================== LICENCE ======================================
# Copyright (c) 2024
# edm_store is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

from osgeo import gdal


class DataTypeMetadata(object):
    def __init__(self, metadata_doc=None):
        self.gdal_type = metadata_doc['gdal_type']
        self.rasterio_type = metadata_doc['rasterio_type']


class DataType:
    def __init__(self):
        self._pixelType = {
            'ubyte': DataTypeMetadata({'gdal_type': gdal.GDT_Byte, 'rasterio_type': 'ubyte'}),
            'uint8': DataTypeMetadata({'gdal_type': gdal.GDT_Byte, 'rasterio_type': 'ubyte'}),
            'bool': DataTypeMetadata({'gdal_type': gdal.GDT_Byte, 'rasterio_type': 'ubyte'}),
            'uint16': DataTypeMetadata({'gdal_type': gdal.GDT_UInt16, 'rasterio_type': 'uint16'}),
            'int16': DataTypeMetadata({'gdal_type': gdal.GDT_Int16, 'rasterio_type': 'int16'}),
            'int8': DataTypeMetadata({'gdal_type': gdal.GDT_Int16, 'rasterio_type': 'int16'}),
            'byte': DataTypeMetadata({'gdal_type': gdal.GDT_Int16, 'rasterio_type': 'int16'}),
            'uint32': DataTypeMetadata({'gdal_type': gdal.GDT_UInt32, 'rasterio_type': 'uint32'}),
            'uint': DataTypeMetadata({'gdal_type': gdal.GDT_UInt32, 'rasterio_type': 'uint32'}),
            'uint64': DataTypeMetadata({'gdal_type': gdal.GDT_UInt32, 'rasterio_type': 'uint32'}),
            'int32': DataTypeMetadata({'gdal_type': gdal.GDT_Int32, 'rasterio_type': 'int32'}),
            'int': DataTypeMetadata({'gdal_type': gdal.GDT_Int32, 'rasterio_type': 'int32'}),
            'int64': DataTypeMetadata({'gdal_type': gdal.GDT_Int32, 'rasterio_type': 'int32'}),
            'float32': DataTypeMetadata({'gdal_type': gdal.GDT_Float32, 'rasterio_type': 'float32'}),
            'float16': DataTypeMetadata({'gdal_type': gdal.GDT_Float32, 'rasterio_type': 'float32'}),
            'float64': DataTypeMetadata({'gdal_type': gdal.GDT_Float64, 'rasterio_type': 'float64'}),
            'float': DataTypeMetadata({'gdal_type': gdal.GDT_Float64, 'rasterio_type': 'float64'}),
            'unknown': DataTypeMetadata({'gdal_type': gdal.GDT_Unknown, 'rasterio_type': 'uint8'})
        }
        self._gdalDataName = ['uint8', 'uint8', 'uint16', 'int16', 'uint32', 'int32', 'float32', 'float64']

    def __getattr__(self, item) -> DataTypeMetadata:
        val = self._pixelType.get(item)
        if isinstance(val, dict):
            return DataTypeMetadata(val)
        return val

    def get(self, d_type: str) -> DataTypeMetadata:
        if d_type in self._pixelType.keys():
            return self.__getattr__(d_type)
        return self.__getattr__('unknown')

    def get_data_type_name_in_gdal(self, typeid: int):
        if typeid >= len(self._gdalDataName):
            return 'float32'
        return self._gdalDataName[typeid]


global_data_type = DataType()
