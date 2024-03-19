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

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Tuple, List

import numpy
import rasterio
import requests
from osgeo import gdal, osr
from rasterio import MemoryFile
from rasterio.windows import Window

from edm_store.storage import AbsBackendClient
from edm_store.utils.cache import global_cache
from edm_store.utils.pixel_type import global_data_type


def read_from_access_path(access_path: str,
                          window: Union[Tuple, List] = None,
                          zoom: Union[int, float] = None,
                          cache: bool = False):
    """
    打开数据集，读取特定层级，特定窗口中的数据

    @param access_path 远程数据的url或者本地文件系统路径
    @param window 读取数据窗口大小，默认为None读取全部数据
    @param zoom 指定层级，指定当前数据集的金字塔层级

    @return data 返回一个numpy数组

    @raise ValueError 读取数据异常
    """
    if access_path is None:
        return numpy.zeros((window[3], window[2]), dtype=numpy.int16)

    if cache:
        if global_cache.has(access_path):
            return _from_memory(global_cache.get(access_path), window=window, zoom=zoom)
        else:
            return _from_access_path(access_path, window=window, zoom=zoom)

    else:
        if zoom is not None and zoom != 0:
            dst = rasterio.open(access_path, overview_level=int(zoom - 1), )
        else:
            dst = rasterio.open(access_path)
        try:
            if window is not None:
                data = dst.read(1, window=Window(*window))
            else:
                data = dst.read(1)
            dst.close()
        except Exception as e:
            dst.close()
            raise ValueError(f'Read error, cause by {e}')

        return data


def reproject_by_gdal(array,
                      src_transform,
                      src_crs,
                      src_nodata,
                      src_shape,
                      dst_transform,
                      dst_crs,
                      dst_shape,
                      dst_nodata,
                      dst_datatype,
                      resample):
    eType = global_data_type.get(dst_datatype).gdal_type
    src_ds = gdal.GetDriverByName("MEM").Create("", src_shape[1], src_shape[0], eType=eType)
    src_ds.GetRasterBand(1).WriteArray(array)
    src_ds.GetRasterBand(1).SetNoDataValue(src_nodata)

    sr = osr.SpatialReference()
    sr.SetFromUserInput(src_crs)
    src_ds.SetProjection(sr.ExportToWkt())
    src_ds.SetGeoTransform(list(src_transform))

    dst_ds = gdal.GetDriverByName("MEM").Create("", dst_shape[1], dst_shape[0], eType=eType)
    dst_ds.GetRasterBand(1).SetNoDataValue(dst_nodata)
    dst_ds.SetGeoTransform(list(dst_transform))
    dr = osr.SpatialReference()
    dr.SetFromUserInput(dst_crs)
    dst_ds.SetProjection(dr.ExportToWkt())
    gdal.ReprojectImage(
        src_ds, dst_ds, src_ds.GetProjection(), dst_ds.GetProjection(), eResampleAlg=resample,
        options=["SAMPLE_STEPS=21", "UNIFIED_SRC_NODATA=YES", "SAMPLE_GRID=YES", "SOURCE_EXTRA=1", "NUM_THREADS=8"]
    )
    array = dst_ds.GetRasterBand(1).ReadAsArray()
    del src_ds, dst_ds
    return array


def _from_memory(ctx, window=None, zoom=None):
    with MemoryFile(ctx) as mem_file:
        if zoom is not None and zoom != 0:
            dst = mem_file.open(driver='GTiff', mode='r', overview_level=int(zoom - 1))
        else:
            dst = mem_file.open(driver='GTiff', mode='r')

        try:
            if window is not None:
                data = dst.read(1, window=Window(*window))
            else:
                data = dst.read(1)
            dst.close()
        except Exception as e:
            dst.close()
            raise ValueError(f'Read error, cause by {e}')

        mem_file.close()

    return data


def _from_access_path(access_path, window=None, zoom=None):
    if zoom is not None and zoom != 0:
        dst = rasterio.open(access_path, overview_level=int(zoom - 1))
    else:
        dst = rasterio.open(access_path)
    try:
        if window is not None:
            data = dst.read(1, window=Window(*window))
        else:
            data = dst.read(1)
        global_thread_pool_executor.cache_tiles(access_path)
        dst.close()
    except Exception as e:
        dst.close()
        raise ValueError(f'Read error, cause by {e}')
    return data


def _cache_tile(target_path: str):
    if target_path.startswith('http'):
        response = requests.get(target_path)
        if response.status_code == 200:
            global_cache.set(target_path, response.content, 3600)
    elif target_path.startswith('/') or target_path.startswith('.'):
        with open(target_path, 'rb') as file:
            ctx = file.read()
        global_cache.set(target_path, ctx, 3600)
    else:
        return None


def _upload_tile(client: AbsBackendClient, x, y, ctx, fa_directory):
    client.upload_by_bytes(f'{x}_{y}.tif', ctx, fa_directory)


def _delete_tiles(client: AbsBackendClient, x, y, fa_directory):
    client.delete(fa_directory + f"/{x}_{y}.tif")


class LocalThreadPoolExecutor:
    __THREAD_POOL_EXECUTOR = ThreadPoolExecutor(max_workers=8)

    def delete_tiles(self, client: AbsBackendClient, x, y, fa_directory):
        self.__THREAD_POOL_EXECUTOR.submit(_delete_tiles, (client, x, y, fa_directory,))

    def upload_tiles(self, client: AbsBackendClient, x, y, ctx, fa_directory):
        self.__THREAD_POOL_EXECUTOR.submit(_upload_tile, (client, x, y, ctx, fa_directory,))

    def cache_tiles(self, target_path):
        daemon_thread = threading.Thread(target=_cache_tile, args=(target_path,), daemon=True)
        daemon_thread.start()

    def map(self, func, iterables, timeout=None, chunksize=1):
        self.__THREAD_POOL_EXECUTOR.map(func, iterables, timeout=timeout, chunksize=chunksize)

    def close(self):
        self.__THREAD_POOL_EXECUTOR = None


global_thread_pool_executor = LocalThreadPoolExecutor()
