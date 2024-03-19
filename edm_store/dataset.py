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

from edm_store.dm import PixelAreaBand
from edm_store.dm.meta import get_metadata
from edm_store.dm.metadata import Metadata
from edm_store.dm.raster import Band, SlicedBand, UnSlicedBand
from edm_store.utils.cache import global_cache


class Dataset:
    """
    该类专门用于处理实际存储的栅格数据，它可以从edm_store存储中的数据创建而来，也可以依据本地或者远程的数据创建；

    这个类会将栅格数据按照Tile来进行管理，主要用于读取，写入，创建，掩码，查询；

    需要注意的是，当创建的类的栅格数据来源是一个完整的栅格数据时，它的写入方法就会受限，这点是为了保护我们提供的源
    数据不会被意外地修改；

    如果确实需要对一个完整的栅格数据进行修改时，可以通过提供的创建的方法，这会创建一个可以修改的空白的相同尺寸的栅
    格数据对象来提供给您进行处理；
    """

    def __new__(cls, path: str, tile_size: int = None) -> Band:
        return open_dataset(path, tile_size)

    @staticmethod
    def open_pixel_area_band(crs: str = None,
                             transform: Union[tuple, list] = None,
                             shape: Union[tuple, list] = None,
                             equal_area_projection: str = None) -> PixelAreaBand:
        return PixelAreaBand(crs=crs, transform=transform, shape=shape, equal_area_projection=equal_area_projection)


def open_dataset(path: str, tile_size: int = None) -> Band:
    if global_cache.has(str(path)+'_'+str(tile_size)):
        return global_cache.get(str(path)+'_'+str(tile_size))
    band_metadata = get_metadata(path)
    metadata = Metadata(band_metadata)
    dataset = SlicedBand(metadata, tile_size) if metadata.cropped else UnSlicedBand(metadata, tile_size)
    global_cache.set(str(path)+'_'+str(tile_size), dataset, 3600)
    return dataset
