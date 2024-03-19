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

from typing import Union

import numpy as np

from osgeo import ogr, osr
from edm_store.dm.raster import Band, RasterIoBand
from edm_store.dm.vector.core import is_same_crs

__EQUAL_AREA_PROJECTION = '+proj=aea +lat_1=15 +lat_2=65 +lat_0=30 +lon_0=95 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'


def _get_pixel_area(geoTransform: tuple,
                    shape,
                    project='EPSG:4326',
                    equalAreaProjection=None):
    ySize, xSize = shape
    _projection = osr.SpatialReference()
    _projection.SetFromUserInput(project)

    if equalAreaProjection is None:
        equalAreaProjection = __EQUAL_AREA_PROJECTION

    _equalAreaProjection = osr.SpatialReference()
    _equalAreaProjection.SetFromUserInput(equalAreaProjection)

    # 创建一个空的ogr多边形对象 创建多多边形
    grid_geom = ogr.CreateGeometryFromJson('{"type": "MultiPolygon", "coordinates": []}')
    # 为多边形指定投影
    grid_geom.AssignSpatialReference(_projection)

    # 柱形投影而言，维度相同的像素面积必然相同，因此只需要算一列即可
    if is_same_crs(project, 'EPSG:4326') or is_same_crs(project, 'EPSG:3857') \
            and is_same_crs(equalAreaProjection, __EQUAL_AREA_PROJECTION):
        xSize_ = 1
    else:
        xSize_ = xSize

    for y in range(ySize):
        t = geoTransform[3] + y * geoTransform[5]
        b = t + geoTransform[5]
        for x in range(xSize_):
            l = geoTransform[0] + x * geoTransform[1]
            r = l + geoTransform[1]
            co = [[[l, b], [l, t], [r, t], [r, b], [l, b]]]
            geojson = '{"type": "Polygon", "coordinates": %s}' % co
            grid_geom.AddGeometry(ogr.CreateGeometryFromJson(geojson))

    # 将新创建的ogr多边形转换到等面积投影上
    suc = grid_geom.TransformTo(_equalAreaProjection)
    if ogr.OGRERR_NONE != suc:
        raise TypeError("Transform Fail")
    # 计算每个像素的面积并生成列表
    pixelArea_flat = [grid_geom.GetGeometryRef(i).GetArea() for i in range(grid_geom.GetGeometryCount())]
    # 把像素面积列表转化为长宽合适的numpy矩阵
    pixelArea = np.array(pixelArea_flat).reshape(ySize, xSize_)
    if xSize_ != xSize:
        pixelArea = np.broadcast_to(pixelArea, (ySize, xSize))
    return pixelArea


class PixelAreaBand:

    def __init__(self,
                 crs: str = 'epsg:4326',
                 transform: Union[tuple, list] = None,
                 shape: Union[tuple, list] = None,
                 equal_area_projection: str = None):

        self.crs = osr.SpatialReference()
        self.crs.SetFromUserInput(crs if crs is not None else 'epsg:4326')
        self._equal_area_projection = equal_area_projection

        self.datatype = 'uint16'

        self.transform = [180, 1, 0, -90, 0, -1] if transform is None else list(transform)
        self.shape = [180, 90] if shape is None else list(shape)

    def apply(self, band: Union[Band, RasterIoBand]):
        self.crs = band.crs
        self.datatype = band.get_raster_data_type()
        self.shape = band.get_size()
        return self

    def get_geo_transform(self):
        return self.transform

    def get_projection(self) -> str:
        return self.crs.ExportToProj4()

    def get_projection_as_proj4(self) -> str:
        return self.crs.ExportToProj4()

    def get_projection_as_wkt(self) -> str:
        return self.crs.ExportToWkt()

    def get_raster_data_type(self) -> str:
        return self.datatype

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None):
        transform = self.transform if transform is None else transform

        projection = self.get_projection() if project is None else project

        return _get_pixel_area(transform, [ySize, xSize], projection, self._equal_area_projection)


