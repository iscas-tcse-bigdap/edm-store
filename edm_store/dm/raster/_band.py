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
import warnings
import numpy as np

from typing import Union, List

import rasterio
from osgeo import osr
from affine import Affine
from rasterio import MemoryFile
import rasterio.mask
from concurrent.futures import ThreadPoolExecutor

from rasterio.enums import Resampling

from edm_store.config import get_config
from edm_store.dm.raster._io import reproject_by_gdal, read_from_access_path, global_thread_pool_executor
from edm_store.dm.raster._global_tile import GlobalTileInfo
from edm_store.dm.vector.core import GeometryGenerator
from edm_store.dm.metadata import Metadata
from edm_store.utils.pixel_type import global_data_type
from edm_store.utils.tools import verify_and_rebuild_path, rebuilt_path, get_resample_method
from edm_store.storage import storage_client_mapper


class Band:
    """
    This class is designed to work with real-world stored raster data, which can be
    created from data in edm_store storage, or created from local or remote data;

    This class will manage the raster data according to Tile, mainly used for reading,
    writing, creating, masking, querying;

    It is important to note that when the raster data source of the created class is
    a complete raster data, it's writing method is limited, which is to protect the
    source data we provide from accidental modification;

    If you do need to modify a complete raster data, you can work with it by providing
    the creation method, which creates a blank grid data object of the same size that
    can be modified;

    @param metadata: edm_store.dm.raster._RasterMetadata.Metadata An object contains
                     current dataset metadata, and can get properties of dataset
                     by __getattr__ function.
    @param tile_size: int  The tile size of the raster data, and should in list
                    [256, 512, 1024, 2048] if bigger than 2048, it may cause
                    unexpected error.If not set ,it will be 2048.

    """

    def __init__(self,
                 metadata: Metadata,
                 tile_size: int = None
                 ):

        self.metadata = metadata
        self.crs = osr.SpatialReference()
        self.crs.SetFromUserInput(self.metadata.crs)
        self.gti = GlobalTileInfo(self.metadata.transform, self.metadata.shape[1],
                                  self.metadata.shape[0], self.metadata.tile_size)

        if tile_size is not None and tile_size != self.metadata.tile_size:
            self.gti.resize(tile_size)

        self._client = storage_client_mapper.get(self.metadata.backend.type)

        self._datatype = global_data_type.get(self.metadata.dtypes[0])

    @property
    def client(self):
        return self._client

    @property
    def transform(self) -> Union[tuple, List]:
        return self.metadata.transform

    @property
    def nodata(self) -> Union[int, float]:
        """ 当前数据的nodata """
        return self.metadata.nodata[0]

    @property
    def datatype(self) -> str:
        return self._datatype.rasterio_type

    def get_band_path(self) -> str:
        return self.metadata.band_path

    def get_tile_offsets(self) -> tuple:
        """
       获取当前数据集进行全球剖分后 x轴方向 和 y轴方向上的tile总数

       :return tuple: (x_size, y_size)
           x_size x轴方向上面的tile总数 x方向标识范围为 (0 -> x_size - 1)
           y_size y轴方向上面的tile总数 y方向标识范围为 (0 -> y_size - 1)
       """
        return self.gti.get_tile_offset()

    def get_nodata_value(self) -> [int, float]:
        """
        获得数据对象的nodata值

        :return int/float
        e.g.
            -9999  landsat对象
        """
        return self.metadata.nodata[0]

    def get_geo_transform(self) -> tuple:
        """
        获取当前数据的变换矩阵 (获取的是经过全球网格剖分之后的变换矩阵)
        仿生变换矩阵gdal和rasterio两种的形式,两种形式存在差异.当前数据返回的是gdal中使用的transform形式

        :return tuple: (minx, pixel_x, 0, min_y, 0, pixel_y)
                (min_x, min_y) 是当前数据集的左上角的点坐标
                pixel_{} 代表着当前数据集某一轴上面的分辨率
        e.g.
            (180,30,0,-90,0,-30)
        """
        return self.metadata.transform

    def get_projection(self) -> str:
        """
        获取当前数据的投影坐标系
        :return str 返回当前数据的投影坐标系的proj4的形式
        e.g.
            "PROJCS[\"WGS 84 / UTM zone 50N\",GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\"]]"
        """
        return self.crs.ExportToProj4()

    def get_projection_as_proj4(self) -> str:
        return self.crs.ExportToProj4()

    def get_projection_as_wkt(self) -> str:
        """
        获取当前数据的投影坐标系
        :return str 返回当前数据的投影坐标系的wkt的形式
        """
        return self.crs.ExportToWkt()

    def get_extent(self) -> tuple:
        """
        返回当前数据的最大范围（该范围为扩充后的范围）
        :return tuple
        """
        min_x, max_x, min_y, max_y = (GeometryGenerator(self.metadata.extent.to_dict())
                                      .export_to_ogr_geometry().GetEnvelope())
        return min_x, min_y, max_x, max_y

    def get_extent_as_geojson(self) -> str:
        """
        获取当前数据的标识范围的geojson形式
        :return str: 当前数据标识范围的geojson形式
        e.g.
            '{"type": "Polygon","coordinates":
            [[[166035,4290915],[166035,4598115],[473235,4598115],
                                                [473235,4290915],[166035,4290915]]]}'
        """
        return (GeometryGenerator(self.metadata.extent.to_dict())
                .export_to_ogr_geometry().ExportToJson())

    def transform_coords(self,
                         cords: [list, tuple]
                         ) -> list:
        _transform = self.metadata.transform
        total = int(len(cords) / 2)
        points = []
        for idx in range(total):
            x_val = cords[idx * 2]
            y_val = cords[idx * 2 + 1]
            points.append(_transform[0] + _transform[1] * x_val)
            points.append(_transform[3] + _transform[5] * y_val)
        return points

    def get_size(self) -> tuple:
        return self.metadata.shape

    def get_raster_count(self) -> int:
        return self.metadata.raster_count

    def get_tiles(self) -> list:
        return self.gti.get_tiles()

    def get_tile_info(self,
                      x: int,
                      y: int
                      ) -> tuple:
        return self.gti.get_tile_info(x, y)

    def get_all_tile_infos(self) -> list:
        return self.gti.get_all_tile_infos()

    def read_tile(self,
                  x: int,
                  y: int
                  ):
        pass

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None):
        pass

    def write_tile(self,
                   x: int,
                   y: int,
                   data,
                   concurrency: bool = False
                   ):
        pass

    def write_region(self,
                     transform: [list, tuple],
                     data,
                     concurrency: bool = False
                     ):

        pass

    def get_grid_info(self,
                      x: int,
                      y: int
                      ):
        return self.gti.get_grid_info(x, y)

    def get_raster_data_type(self) -> int:
        return self._datatype.gdal_type

    def query_tiles(self,
                    cutLine: str,
                    crs: str
                    ) -> list:
        # 将cut_line转换成对应的几何图像并且获得图像的外边框范围
        t_box = GeometryGenerator(cutLine).set_crs(crs).transform(
            self.metadata.crs).export_to_ogr_geometry()

        l_x, r_x, b_y, t_y = t_box.GetEnvelope()

        # 计算确定可能与图像有交集的tile范围
        _transform = self.metadata.transform
        start_x = self.gti.firstTileLeftX
        start_y = self.gti.firstTileLeftY
        step_x = self.gti.reSize * _transform[1]
        step_y = self.gti.reSize * _transform[5]

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

    def create_dataset(self,
                       newPath: str,
                       dataType: str,
                       nodata: Union[int, float],
                       tileSize: int = 2048,
                       imagePath: str = None
                       ):
        newPath = verify_and_rebuild_path(newPath)

        client = storage_client_mapper.get(get_config().base_store_type)
        # 将路径转换成目录
        dir_name = newPath[:newPath.rindex('.')]

        crs = self.metadata.crs
        transform = self.metadata.transform
        shape = self.metadata.shape

        info = GlobalTileInfo(transform, shape[1], shape[0], tileSize)
        client.mk_dirs(dir_name)
        transform, shape = info.get_grid_info()

        from edm_store.dm.meta import create_band

        create_band(crs,
                    shape,
                    transform,
                    newPath,
                    nodata,
                    dataType,
                    image_path=imagePath,
                    tile_size=tileSize)

    def mask_tile(self, x, y, cut_line, s_crs, fill_val=0):
        t_box = GeometryGenerator(cut_line).set_crs(s_crs).transform(
            self.metadata.crs).export_to_geojson()

        data = self.read_tile(x, y)

        _transform, shape = self.get_tile_info(x, y)
        # 创建内存文件将需要读取的部分预先读取到内存中

        with MemoryFile() as mem_file:
            # 对内存中的文件进行写入
            with mem_file.open(driver='GTiff', count=1, width=shape[1], height=shape[0],
                               dtype=self.datatype, nodata=fill_val,
                               transform=Affine.from_gdal(*_transform), crs=self.metadata.crs) as dataset:
                dataset.write(data, 1)
            # 对文件进行掩码
            with mem_file.open(driver='GTiff') as tmp:
                out_image, out_transform = rasterio.mask.mask(tmp,
                                                              [json.loads(t_box)],
                                                              crop=False,
                                                              nodata=fill_val)

        out_meta = {"search": "GTiff", "height": shape[0], "width": shape[1], "transform": _transform}

        return out_image[0], out_meta

    def __del__(self):
        del self

    def close(self):
        self.__del__()


def _get_x_end_and_y_end(fill_info, actual_shape, virtual_shape):
    data_y_size, data_x_size = actual_shape

    # 如果当前读取到的实际的数据尺寸偏大
    x_end = int(fill_info[1] - fill_info[0] + 1) if fill_info[1] < data_x_size + fill_info[
        0] - 1 else data_x_size

    y_end = int(fill_info[3] - fill_info[2] + 1) if fill_info[3] < data_y_size + fill_info[
        2] - 1 else data_y_size

    if int(fill_info[0] + x_end) > virtual_shape[1]:
        x_end = int(virtual_shape[1] - fill_info[0])
    if int(fill_info[2] + y_end) > virtual_shape[0]:
        y_end = int(virtual_shape[0] - fill_info[2])

    return x_end, y_end


class SlicedBand(Band):
    """
    对数据重新切分并且支持数据的读写功能
    """

    def __init__(self, metadata: Metadata, tile_size: int = None):
        super().__init__(metadata, tile_size)

    def read_tile(self, x: int, y: int):
        """
        当前Band会对实际的数据进行切分但是切分的尺寸和打开数据集的尺寸不同

        当当前读取尺寸和实际划分尺寸不同时，会以当前尺寸为主，读取数据

        """

        tiles, windows = self.gti.get_tile_index_and_offset(x, y)

        fa_directory = self.metadata.backend.path

        x, y = tiles
        tile_path = rebuilt_path(f'{fa_directory}/{x}_{y}.tif')

        if self.client.is_accessible(tile_path):
            access_path = self.client.get_access_path(tile_path)
            data = read_from_access_path(access_path, window=windows, cache=True)
        else:
            # current tile doesn't exist
            data = np.empty((self.gti.reSize, self.gti.reSize), self.datatype)
            data.fill(self.nodata)

        return data

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None
                    ):

        # 对读取的尺寸存在限制，只能读取最大(0, 4096]大小的数据
        if xSize <= 0 or ySize <= 0: return [[]]

        if xSize >= 4096 or ySize >= 4096:
            xSize = min(xSize, 4096)
            ySize = min(ySize, 4096)
            warnings.warn('The `read_region` method specifies 4096 as the maximum range of xSize and ySize, '
                          'and the out-of-range will be reset to 4096', stacklevel=2)

        project = self.get_projection_as_wkt() if project is None else project

        resample = get_resample_method(resample)

        # 将输入的transform与shape转换至当前的坐标系下
        virtual_transform, virtual_shape, need_reproject, zoom = self.gti.rebuild_transform_to_target_crs(transform,
                                                                                                          (
                                                                                                          ySize, xSize),
                                                                                                          project,
                                                                                                          self.get_projection_as_wkt())

        infos, actual_transform, actual_shape = self.gti.calculate_read_window_of_sliced_band(virtual_transform,
                                                                                              virtual_shape[1],
                                                                                              virtual_shape[0],
                                                                                              zoom=zoom)

        backend_path = self.metadata.backend.path

        # 多线程运行读取数据
        from concurrent.futures import ThreadPoolExecutor

        repetition_count = 8

        base_array = np.empty(actual_shape, self.datatype)
        base_array.fill(self.nodata)

        def threading_read_from_info(info):
            tiles, read_info, fill_info = info
            object_path = rebuilt_path(f'{backend_path}/{tiles[0]}_{tiles[1]}.tif')
            data = None
            if self.client.is_accessible(object_path):
                url = self.client.get_access_path(object_path)
                windows = (read_info[0],
                           read_info[2],
                           int(read_info[1] - read_info[0] + 1),
                           int(read_info[3] - read_info[2] + 1))
                data = read_from_access_path(url, window=windows, zoom=zoom, cache=True)

            if data is not None:
                x_end, y_end = _get_x_end_and_y_end(fill_info, data.shape, actual_shape)
                base_array[fill_info[2]:int(fill_info[2] + y_end), fill_info[0]:int(x_end + fill_info[0])] = data[
                                                                                                             :y_end,
                                                                                                             :x_end]

        with ThreadPoolExecutor(repetition_count) as pool:
            pool.map(threading_read_from_info, infos)

        # for info in infos:
        #     threading_read_from_info(info)

        if list(actual_shape) == list(virtual_shape) and not need_reproject:
            return base_array
        else:
            return reproject_by_gdal(
                base_array,
                actual_transform,
                self.crs.ExportToWkt(),
                self.nodata,
                actual_shape,
                transform,
                project,
                (ySize, xSize),
                self.nodata,
                self.datatype,
                resample
            )

    def write_tile(self, x: int, y: int, array, concurrency: bool = False):

        if self.metadata.readonly or not self.gti.writeable():
            warnings.warn(f"The current data open size is different from the actual write size, "
                          f"or the current data does not support the write method")
            return False

        array = np.asarray(array)
        if array.shape != (self.gti.tileSize, self.gti.tileSize):
            warnings.warn(f"Data had been clip into tiles with size: ({self.gti.tileSize},{self.gti.tileSize}), "
                          f"but got an array with size:  {array.shape}\n"
                          f"You should input an array with an correct size")
            return False

        base_directory: str = self.metadata.backend.path

        transform, shape = self.get_tile_info(x, y)

        base_directory = base_directory[1:] if base_directory.startswith('/') else base_directory
        base_directory = base_directory[:-1] if base_directory.endswith('/') else base_directory

        with MemoryFile() as mem_file:
            with mem_file.open(driver='GTiff', count=1, width=shape[1], height=shape[0],
                               dtype=self.metadata.dtypes[0], nodata=0,
                               transform=Affine.from_gdal(*transform),
                               crs=self.get_projection_as_proj4(),
                               compress='lzw') as dst:
                dst.write(array, 1)
                print()
                dst.build_overviews(self.gti._factors, Resampling.nearest)
                dst.close()

            ctx = mem_file.read()
            mem_file.close()

        if concurrency:
            global_thread_pool_executor.upload_tiles(self.client, x, y, ctx, base_directory)
            return True
        else:
            return self.client.upload_by_bytes(f'{x}_{y}.tif', ctx, base_directory)

    def write_region(self, transform: [list, tuple], data, concurrency: bool = False):
        if self.metadata.readonly or not self.gti.writeable():
            warnings.warn(f"The current data open size is different from the actual write size, "
                          f"or the current data does not support the write method")
            return False

        data = np.asarray(data, self.datatype)
        data_shape = data.shape
        infos, actual_transform, actual_shape = self.gti.calculate_read_window_of_sliced_band(transform,
                                                                                              data_shape[1],
                                                                                              data_shape[0])

        if infos is None:
            return False

        suc = True
        repetition_count = 8
        resample = get_resample_method()
        if list(actual_transform) != list(transform) or list(actual_shape) != list(data_shape):
            base_data = reproject_by_gdal(
                data,
                transform,
                self.crs.ExportToWkt(),
                self.nodata,
                data_shape,
                actual_transform,
                self.crs.ExportToWkt(),
                actual_shape,
                self.nodata,
                self.datatype,
                resample
            )
        else:
            base_data = data

        def _write_tile(info):
            tile, read_info, fill_info = info
            array = self.read_tile(*tile)
            array[read_info[2]:int(read_info[3] + 1), read_info[0]:int(read_info[1] + 1)] = \
                base_data[fill_info[2]:int(fill_info[3] + 1), fill_info[0]:int(fill_info[1] + 1)]
            return suc and self.write_tile(tile[0], tile[1], array)

        if concurrency:
            with ThreadPoolExecutor(repetition_count) as pool:
                pool.map(_write_tile, infos)
        else:
            for info in infos:
                _write_tile(info)

        return suc


class UnSlicedBand(Band):
    """
    对数据重新切分并且支持数据的读写功能
    """

    def __init__(self, metadata: Metadata, tile_size: int = None):
        super().__init__(metadata, tile_size)

    def read_tile(self, x: int, y: int):
        """
        读取当前数据中(x,y)标记的tile块n_band波段下的数据

        当前数据集进行的是虚拟切分,所以获得当前的数组尺寸可能与tile的默认尺寸不相符,
        这种情况下会填充数组直到满足当前设定的tile的默认尺寸

            :param x int x轴方向上的标识 (从0开始)
            :param y int y轴方向上的标记 (从0开始)

            :return numpy.array tile中的数据
        """
        transform, shape = self.gti.get_tile_info(x, y)
        _array = self.read_region(transform, shape[1], shape[0])
        return _array

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None
                    ):

        if xSize * ySize <= 0: return [[]]

        if xSize > 4096 or ySize > 4096:
            warnings.warn('The readRegion method specifies 4096 as the maximum range of xSzie and ySize, '
                          'and the out-of-range is reset to 4096', stacklevel=2)
            xSize = min(xSize, 4096)
            ySize = min(ySize, 4096)

        project = self.crs.ExportToWkt() if project is None else project

        resample = get_resample_method(resample)

        n_transform, n_shape, need_reproject, zoom = \
            self.gti.rebuild_transform_to_target_crs(transform, (ySize, xSize), project, self.get_projection_as_wkt())

        read_info, fill_info = self.gti.calculate_read_window_of_unsliced_band(n_transform,
                                                                               n_shape[1],
                                                                               n_shape[0],
                                                                               zoom=zoom)

        base_array = np.empty((ySize, xSize), self.datatype)
        base_array.fill(self.nodata)

        if read_info is None:
            return base_array
        if self.client.is_accessible(self.metadata.backend.path):
            access_path = self.client.get_access_path(self.metadata.backend.path)
            data = read_from_access_path(access_path,
                                         window=(read_info[0],
                                                 read_info[2],
                                                 int(read_info[1] - read_info[0] + 1),
                                                 int(read_info[3] - read_info[2] + 1)),
                                         zoom=zoom)
            if data is not None:
                x_end, y_end = _get_x_end_and_y_end(fill_info, data.shape, n_shape)
                base_array[fill_info[2]:int(fill_info[2] + y_end), fill_info[0]:int(fill_info[0] + x_end)] = data[
                                                                                                             :y_end,
                                                                                                             :x_end]

        return base_array if not need_reproject else reproject_by_gdal(
            base_array,
            n_transform,
            self.crs.ExportToProj4(),
            self.nodata,
            n_shape,
            transform,
            project,
            (ySize, xSize),
            self.nodata,
            self.datatype,
            resample)

    def write_tile(self, x: int, y: int, array, concurrency: bool = False):
        warnings.warn('Unsliced Band do not support functions for writing', stacklevel=2)
        return False

    def write_region(self, transform: [list, tuple], data, concurrency: bool = False):
        warnings.warn('Unsliced Band do not support functions for writing', stacklevel=2)
        return False
