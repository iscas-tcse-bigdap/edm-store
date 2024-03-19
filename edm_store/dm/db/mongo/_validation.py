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

from typing import Any, List, Union, Dict, Optional
from pydantic import BaseModel, Extra


# ----------------------------- Pydantic Models ----------------------------- #


class _Backend(BaseModel):
    path: str
    type: str


class Band(BaseModel, extra=Extra.allow):
    """
    Pydantic Model for validating document entity of `Band`.

    >>> rawBand = {}
    >>> bandModel = Band.parse_obj(rawBand) # to parse a dict into the model
    >>> validatedImage = bandModel.dict() # to retrieve the dict repr of model
    """

    # objectId: str
    band_path: str
    band_name: str
    crs: str
    shape: List[Any]
    transform: List[Any]
    image_path: Optional[str]
    cropped: bool
    readonly: bool
    tile_size: int
    nodata: List[Any]
    raster_count: int
    dtypes: List[str]
    backend: _Backend


class Image(BaseModel, extra=Extra.allow):
    """
    Pydantic Model for validating document entity of `Image`.

    >>> rawImage = {}
    >>> imageModel = Band.parse_obj(rawImage) # to parse a dict into the model
    >>> validatedImage = imageModel.dict() # to retrieve the dict repr of model
    """

    # objectId: str
    image_path: str
    image_name: str
    bands: Dict[str, str]
    date: Union[int, str]
    processing_time: Union[str, int]
    wgs_boundary: Union[dict, str]
    provider: str
    year: int
