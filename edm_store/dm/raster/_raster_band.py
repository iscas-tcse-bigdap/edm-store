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

import json
import math
from typing import Union, Dict, List, Any

import numpy as np
import rasterio
from affine import Affine
from osgeo import gdal
from rasterio import MemoryFile, mask

from edm_store.config import get_config
from edm_store.dm.raster._global_tile import GlobalTileInfo
from edm_store.dm.raster._io import reproject_by_gdal
from edm_store.dm.vector.core import GeometryGenerator, gen_geobox, calculate_tiles_info_by_input
from edm_store.storage import storage_client_mapper
from edm_store.utils.pixel_type import global_data_type
from edm_store.utils.tools import verify_and_rebuild_path, get_resample_method


class RasterIoBand:
    """
    This is a class that specifically uses rasterio to open and process raster files，Raster files are also processed in
    blocks, but a rasterio dataset object is maintained internally.
    """

    def __init__(self, filePath, tileSize: int = 2048):
        self._dst = None
        self.gti = None
        self.filename = filePath
        self.tileSize = tileSize
        self._openfile()

    def _openfile(self):
        try:
            if not str(self.filename).startswith('http'):
                self._dst = gdal.Open(self.filename)
            else:
                self._dst = gdal.Open('/vsicurl/' + self.filename)

            self.shape = (self._dst.RasterYSize, self._dst.RasterXSize)
            self.gti = GlobalTileInfo(self._dst.GetGeoTransform(),
                                      self.shape[1],
                                      self.shape[0],
                                      self.tileSize,
                                      hasPyramid=True)

        except Exception as e:
            raise ValueError(f"Can't open dataset in : {self.filename}\n"
                             f"Error may cause by {e}")

    def get_band_path(self) -> str:
        return self.filename

    def get_tile_offsets(self) -> tuple:
        return self.gti.get_tile_offset()

    def get_nodata_value(self, band: int = 1) -> [int, float]:
        if self._dst is None:
            self._openfile()
        nodata = self._dst.GetRasterBand(band).GetNoDataValue()
        if nodata is None:
            return 0
        return nodata

    def get_geo_transform(self) -> tuple:
        if self._dst is None:
            self._openfile()
        return self.gti.get_grid_info()[0]

    def get_projection(self) -> str:
        if self._dst is None:
            self._openfile()
        return str(self._dst.GetProjectionRef())

    def get_projection_as_proj4(self) -> str:
        if self._dst is None:
            self._openfile()
        return self._dst.GetProjection()

    def get_extent(self) -> dict:
        if self._dst is None:
            self._openfile()
        return gen_geobox(self.get_geo_transform(), self.get_size()).export_to_ogr_geometry().GetEnvelope()

    def get_extent_as_json(self) -> Dict[str, Union[str, List[List[List[Any]]]]]:
        if self._dst is None:
            self._openfile()
        min_x, max_x, min_y, max_y = self.get_extent()
        return {"type": "Polygon",
                "coordinates": [
                    [[min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y],
                     [min_x, max_y]]
                ]
                }

    def transform_coords(self, cords: [list, tuple]) -> list:
        _transform = self._dst.GetGeoTransform()
        total = int(len(cords) / 2)
        points = []
        for idx in range(total):
            x_val = cords[idx * 2]
            y_val = cords[idx * 2 + 1]
            points.append(_transform[0] + _transform[1] * x_val)
            points.append(_transform[3] + _transform[5] * y_val)
        return points

    def get_size(self) -> tuple:
        return self.gti.get_grid_info()[1][1], self.gti.get_grid_info()[1][0]

    def get_raster_count(self) -> int:
        return self._dst.RasterCount

    def get_tiles(self) -> list:
        return self.gti.get_tiles()

    def get_tile_info(self, x: int, y: int) -> tuple:
        return self.gti.get_tile_info(x, y)

    def get_all_tile_infos(self) -> list:
        return self.gti.get_all_tile_infos()

    def read_tile(self, x: int, y: int, band: int = 1):
        transform, shape = self.gti.get_tile_info(x, y)
        base_array = self.read_region(transform, shape[1], shape[0], band=band)
        return base_array

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None,
                    band: int = 1
                    ):

        if xSize <= 0 or ySize <= 0: return [[]]

        project = self._dst.GetProjection() if project is None else project

        resample = get_resample_method(resample)

        n_transform, n_shape, need, zoom = self.gti.rebuild_transform_to_target_crs(transform, (ySize, xSize), project,
                                                                                    self._dst.GetProjection())
        array = np.zeros((ySize, xSize), self.datatype(band))
        for newTile in calculate_tiles_info_by_input(n_transform, n_shape):
            read_info, fill_info = self.gti.calculate_read_window_of_unsliced_band(newTile[1], newTile[0][1],
                                                                                   newTile[0][0], zoom=zoom)

            if read_info is None:
                continue

            base_array = np.empty(newTile[0], self.datatype(band))
            base_array.fill(self.get_nodata_value(band))

            window = (
                read_info[0], read_info[2], int(read_info[1] - read_info[0] + 1), int(read_info[3] - read_info[2] + 1))
            _a = self._dst.GetRasterBand(band).ReadAsArray(*window)

            base_array[fill_info[2]:int(fill_info[3] + 1), fill_info[0]:int(fill_info[1] + 1)] = _a

            destination = reproject_by_gdal(
                base_array,
                n_transform,
                self._dst.GetProjection(),
                self.get_nodata_value(band),
                n_shape,
                transform,
                project,
                (ySize, xSize),
                0,
                self.get_raster_data_type(band),
                resample
            )
            array = array + destination

        array[array == 0] = self.get_nodata_value(band)
        return array

    def datatype(self, band: int = 1):
        d_type = self._dst.GetRasterBand(band).DataType
        if d_type is None:
            return 'uint8'
        return global_data_type.get_data_type_name_in_gdal(d_type)

    def get_raster_data_type(self, band: int = 1) -> str:
        d_type = self._dst.GetRasterBand(band).DataType
        if d_type is None:
            return global_data_type.get('uint8').gdal_type
        return global_data_type.get(d_type).gdal_type

    def query_tiles(self, cutLine: str, crs: str) -> list:
        t_box = GeometryGenerator(cutLine).set_crs(crs).transform(
            self.get_projection()).export_to_ogr_geometry()

        l_x, r_x, b_y, t_y = t_box.GetEnvelope()

        # 计算确定可能与图像有交集的tile范围
        transform = self.get_geo_transform()
        start_x = self.gti.firstTileLeftX
        start_y = self.gti.firstTileLeftY
        step_x = self.gti.reSize * transform[1]
        step_y = self.gti.reSize * transform[5]

        # 计算可能相交的x，y的范围 (sx --- ex) (sy --- ey)
        # x
        sx = int(abs((l_x - start_x) / step_x))
        ex = int(abs(math.ceil((r_x - start_x) / step_x)))
        # y
        sy = int(abs((t_y - start_y) / step_y))
        ey = int(abs(math.ceil((b_y - start_y) / step_y)))
        res = []
        for x in range(sx, ex + 1):
            for y in range(sy, ey + 1):
                min_x = start_x + x * step_x
                max_x = start_x + (x + 1) * step_x
                max_y = start_y + y * step_y
                min_y = start_y + (y + 1) * step_y
                box = GeometryGenerator({"type": "Polygon",
                                         "coordinates": [
                                             [[min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y],
                                              [min_x, max_y]]
                                         ]
                                         }).export_to_ogr_geometry()
                if t_box.Intersection(box):
                    res.append((x, y))
        return res

    def create_dataset(
            self,
            newPath: str,
            dataType: str,
            nodata: Union[int, float],
            tileSize: int = 2048,
            imagePath: str = None
    ):
        newPath = verify_and_rebuild_path(newPath)

        client = storage_client_mapper[get_config().base_store_type]

        # 将路径转换成目录
        dir_name = newPath[:newPath.rindex('.')]

        crs = self.get_projection_as_proj4()
        transform = self.get_geo_transform()
        shape = self.get_size()

        info = GlobalTileInfo(transform, shape[1], shape[0], tileSize)
        client.mk_dirs(dir_name)
        transform, shape = info.get_grid_info()

        from edm_store.dm.meta._api_impl import create_band

        create_band(crs,
                    shape,
                    transform,
                    newPath,
                    nodata,
                    dataType,
                    image_path=imagePath,
                    tile_size=tileSize)

    def mask_tile(self, x, y, cut_line, s_crs,  fill_val=0, band: int = 1):
        t_box = GeometryGenerator(cut_line).set_crs(s_crs).transform(
            self.get_projection()).export_to_geojson()

        data = self.read_tile(x, y, band)

        transform, shape = self.get_tile_info(x, y)
        # 创建内存文件将需要读取的部分预先读取到内存中

        with MemoryFile() as mem_file:
            # 对内存中的文件进行写入
            with mem_file.open(driver='GTiff', count=1, width=shape[1], height=shape[0],
                               dtype=self.datatype, nodata=fill_val,
                               transform=Affine.from_gdal(*transform), crs=self.get_projection()) as dataset:
                dataset.write(data, 1)
            # 对文件进行掩码
            with mem_file.open(driver='GTiff') as tmp:
                out_image, out_transform = rasterio.mask.mask(tmp,
                                                              [json.loads(t_box)],
                                                              crop=False,
                                                              nodata=fill_val)

        out_meta = {"search": "GTiff", "height": shape[0], "width": shape[1], "transform": transform}

        return out_image[0], out_meta

    def __del__(self):
        del self

    def close(self):
        self.__del__()
