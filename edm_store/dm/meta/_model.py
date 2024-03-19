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

from edm_store.config import get_config
from edm_store.dm.vector.core import geobox_info
from edm_store.utils.tools import gen_format_time


class BandMetadata:
    def __init__(
            self,
            crs: str,
            shape: Union[tuple, list],
            transform: Union[tuple, list],
            bandPath: str,
            realPath: str,
            nodata: Union[int, float, list],
            rasterCount: Union[int, float],
            dataType: Union[str, list],
            tileSize: int = 2048,
            imagePath: str = None,
            storeType: str = None,
            cropped: bool = True,
            readonly: bool = False
    ):

        if not isinstance(nodata, list):
            nodata = [nodata]

        if not isinstance(dataType, list):
            dataType = [dataType]

        backend = {'path': realPath, 'type': get_config().base_store_type if storeType is None else storeType}

        self.metadata = {
            'band_path': bandPath,
            'band_name': bandPath[bandPath.rindex('/') + 1:],
            'crs': crs,
            'shape': list(shape),
            'transform': list(transform),
            'image_path': imagePath,
            'extent': geobox_info(transform, shape, s_crs=crs),
            'tile_size': tileSize,
            'nodata': nodata,
            'raster_count': rasterCount,
            'dtypes': dataType,
            'backend': backend,
            'readonly': readonly,
            'cropped': cropped
        }

    def __getattr__(self, item):
        return self.metadata.get(item)

    def __setitem__(self, key, value):
        self.metadata[key] = value

    def __getitem__(self, item):
        return self.metadata.get(item)

    def export_to_dict(self) -> dict:
        return self.metadata


class ImageMetadata:
    def __init__(
            self,
            crs,
            shape,
            transform,
            imagePath,
            bands,
            systime=None,
            provider='edm_store',
    ):
        imagePath = str(imagePath)

        self.bands = bands if bands is not None else {}

        systime, year = gen_format_time(systime)

        self.metadata = {
            'image_path': imagePath,
            'image_name': imagePath[imagePath.rindex('/') + 1:],
            'wgs_boundary': geobox_info(transform, shape, s_crs=crs, t_crs='epsg:4326'),
            'date': systime,
            'year': year,
            'provider': provider,
            'processing_time': gen_format_time()[0]
        }

    def __getattr__(self, item):
        return self.metadata.get(item)

    def __setitem__(self, key, value):
        self.metadata[key] = value

    def __getitem__(self, item):
        return self.metadata.get(item)

    def export_to_dict(self) -> dict:
        self.metadata['bands'] = self.bands
        return self.metadata

    def add_band_from_name(self, bandName: str, bandPath: str):
        self.bands[bandName] = bandPath
        return self

    def add_band_from_BandMetadata(self, bandName: str, band: BandMetadata):
        self.bands[bandName] = band.bandPath
        return self
