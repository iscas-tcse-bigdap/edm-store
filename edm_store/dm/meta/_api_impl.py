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

from typing import Union, Dict, List, Optional

from edm_store.config import get_config
from edm_store.dm.db import get_metadata_resource_instance
from edm_store.dm.meta._model import BandMetadata, ImageMetadata
from edm_store.dm.metadata import Metadata
from edm_store.dm.raster import GlobalTileInfo, RasterIoBand
from edm_store.dm.raster._io import global_thread_pool_executor
from edm_store.storage import storage_client_mapper
from edm_store.utils.pixel_type import global_data_type
from edm_store.utils.tools import verify_and_rebuild_path, rebuilt_path


def get_dataset_source_by_path(dataset_path: str) -> str:
    data_source_type = dataset_path.split('/')[2]
    return _get_dataset_source(data_source_type)


def _get_dataset_source(data_source_type: str) -> str:
    if data_source_type in get_config().datasource_mapper.keys():
        return get_config().datasource_mapper[data_source_type]
    raise KeyError(f'Unknown dataset source type: {data_source_type}')


def find_images(
        filter_json: Union[str, Dict],
        return_props: Optional[List] = None,
        datasource: Optional[str] = None,
        limit: Optional[int] = 2000,
        configs: Optional[Dict] = None
):
    datasource = get_config().base_datasource if datasource is None else datasource
    configs = get_config().db_config if configs is None else configs

    if datasource not in get_config().datasource:
        raise ValueError(f'No such datasource named {datasource}')
    source = get_metadata_resource_instance(configs)
    with source.transaction() as db:
        images = db.findImage(filter_json, return_props, datasource, limit)

    return images


def get_band(
        band_path: str,
        configs: Optional[Dict] = None
):
    configs = get_config().db_config if configs is None else configs

    source = get_metadata_resource_instance(configs)
    try:
        datasource = get_dataset_source_by_path(band_path)
        with source.transaction() as db:
            bandMetadata = db.getBand(band_path, datasource)
            if bandMetadata is None:
                raise ValueError("No such band or image : {bandPath} ".format(bandPath=band_path))
        return bandMetadata
    finally:
        source.close()


def get_image(
        image_path: str,
        configs: Optional[Dict] = None
):
    configs = get_config().db_config if configs is None else configs
    source = get_metadata_resource_instance(configs)
    try:
        datasource = get_dataset_source_by_path(image_path)
        with source.transaction() as db:
            imageMetadata = db.getImage(image_path, datasource)
            if imageMetadata is None:
                raise ValueError("No such band or image : {imagePath} ".format(imagePath=image_path))
        return imageMetadata
    finally:
        source.close()


def create_image(
        crs: str,
        shape: Union[list, tuple],
        transform: Union[list, tuple],
        image_path: str,
        bands: dict,
        configs: Optional[Dict] = None,
        system_time=None,
        provider='edm_store'
):
    """
    将描述同一区域的影像信息保存到数据库中，（crs、shape、t_geoarea）这些参数使用来构建出Image的范围

    通过这些参数会计算并转换到EPSG:4326坐标系下的矢量范围。

    对于一个影像的波段来说，波段不能为空。

    一般来说对于创建影像是在创建波段之后。
    """
    # 检查Bands是否为空
    image_path = verify_and_rebuild_path(image_path)
    configs = get_config().db_config if configs is None else configs

    if bands is None or len(bands.keys()) <= 0:
        raise ValueError(f"Empty bands in image: {image_path}, "
                         f"an image must contain one band at least".format(image_path=image_path))

    # 依据输入创建出Image元数据
    imageMetadata = ImageMetadata(crs, shape, transform, image_path, bands,
                                  systime=system_time, provider=provider)

    source = get_metadata_resource_instance(configs)
    datasource = get_dataset_source_by_path(image_path)

    try:

        with source.transaction() as db:
            image = db.addImage(imageMetadata.export_to_dict(), datasource)
        return image

    finally:
        source.close()


def create_band(
        crs: str,
        shape: Union[list, tuple],
        transform: Union[list, tuple],
        band_path: str,
        nodata: Union[int, float],
        data_type: str,
        image_path: str = None,
        tile_size: int = 2048,
        configs: Optional[Dict] = None,
):
    """
    提供根据输入创建一个新的波段，用户需要输入坐标系，尺寸，仿生转换矩阵，波段的路径名，缺省值，数据类型。
    需要注意的是该函数只能创建一个单独的波段，它的rasterCount默认为1， 如果需要创建多个波段则需要多次调
    用函数。
    需要注意的是该函数创建的是一个逻辑波段，并不会写入真正的数据，只有构建出Dataset并调用Write方法时候才
    会真正写入数据。
    同时还有一个需要注意的点是，该方法只能在默认的数据库（default）中创建数据。
    """
    configs = get_config().db_config if configs is None else configs
    band_path = verify_and_rebuild_path(band_path)

    real_path = band_path[:band_path.rindex('.')]

    client = storage_client_mapper.get(get_config().base_store_type)
    client.mk_dirs(real_path)

    # 构建元数据存储入库
    dataType = global_data_type.get(data_type).rasterio_type
    band_metadata = BandMetadata(crs, shape, transform, band_path, nodata=nodata, rasterCount=1, dataType=dataType,
                                 imagePath=image_path, realPath=real_path, tileSize=tile_size)
    source = get_metadata_resource_instance(configs)
    try:
        with source.transaction() as db:
            msg = db.addBand(band_metadata.export_to_dict(), get_config().base_datasource)
        return msg
    finally:
        source.close()


# def create_vrt_dataset(
#         crs: str,
#         shape: Union[list, tuple],
#         transform: Union[list, tuple],
#         nodata: Union[int, float],
#         data_type: str = 'float64',
#         tile_size: int = 2048,
#         func_meta: dict = None,
#         fa_dst: str = None,
#         band_type: str = 'tmp'
# ):
#     _dirs = _genRandomName()
#     _bandPath = '/edm_store/{}/{}.BAND'.format(band_type, _dirs)
#     _dirs = rebuiltPath(CONST.PRE_FILE + '/' + _dirs)
#     if _dirs.startswith('/'):
#         _dirs = _dirs[1:].split('/')
#     else:
#         _dirs = _dirs.split('/')
#     sub_dir = ''
#     for _dir in _dirs:
#         sub_dir = sub_dir + '/' + _dir
#         _path = rebuiltPath(sub_dir)
#         if not os.path.exists(_path):
#             os.mkdir(_path)
#     bm = BandMetadata(crs, shape, transform, _bandPath, sub_dir, nodata, 1, dataType, tileSize).exportToDict()
#
#     bm['fa_dst'] = fa_dst
#     bm['func'] = func_meta
#
#     with open(sub_dir + '/metadata.json', "w", encoding='utf-8') as f:
#         # json.dump(dict_var, f)  # 写为一行
#         json.dump(bm, f, indent=2, sort_keys=True, ensure_ascii=False)
#
#     return _bandPath


def load_dataset_from_file(data_path: str, tile_size: int = 2048):
    return RasterIoBand(data_path, tile_size)


def exist_band(
        band_path: str,
        configs: Optional[Dict] = None
):
    configs = get_config().db_config if configs is None else configs
    source = get_metadata_resource_instance(configs)
    try:
        datasource = get_dataset_source_by_path(band_path)
        with source.transaction() as db:
            res = db.getBand(band_path, dataset=datasource) is not None
        return res
    finally:
        source.close()


def exist_image(
        image_path: str,
        configs: Optional[Dict] = None
):
    configs = get_config().db_config if configs is None else configs
    datasource = get_dataset_source_by_path(image_path)
    source = get_metadata_resource_instance(configs)
    try:
        with source.transaction() as db:
            res = db.getImage(image_path, dataset=datasource) is not None
        return res
    finally:
        source.close()


def unlink_image(
        image_path: str,
        configs: Optional[Dict] = None,
        concurrency=False
):
    configs = get_config().db_config if configs is None else configs
    source = get_metadata_resource_instance(configs)
    try:
        datasource_type = image_path.split('/')[2]

        if datasource_type not in get_config().delete_allowed:
            raise ValueError("Illegal deletion")

        with source.transaction() as db:
            imageMetadata = db.getImage(image_path)
            if imageMetadata is None:
                raise ValueError("Can't find band or image named:{imagePath} ".format(imagePath=image_path))

            # 如果Image存在需要删除所有的波段
            bands: Dict = imageMetadata['bands']
            for bandKey in bands.keys():
                bandPath = verify_and_rebuild_path(bands.get(bandKey))
                bandMetadata = db.getBand(bandPath)
                if bandMetadata is not None:
                    # 删除栅格数据
                    metadata = Metadata(bandMetadata)
                    client = storage_client_mapper.get(metadata.backend.type)
                    if metadata.cropped:
                        # 如果是分块数据
                        gti = GlobalTileInfo(metadata.transform,
                                             metadata.shape[1],
                                             metadata.shape[0],
                                             metadata.tile_size)
                        tiles = gti.get_tiles()
                        faDir = metadata.backend.path
                        if concurrency:
                            for tile in tiles:
                                global_thread_pool_executor.delete_tiles(client, tile[0], tile[1], faDir)
                        else:
                            for tile in tiles:
                                client.delete(rebuilt_path(faDir + "/{}_{}.tif".format(*tile)))
                    else:
                        client.delete(metadata.backend.path)

                # 删除Band元数据
                db.deleteBand(bandPath)
            return db.deleteImage(image_path)
    finally:
        source.close()


def unlink_band(
        band_path: str,
        configs: Optional[Dict] = None,
        concurrency=False
):
    configs = get_config().db_config if configs is None else configs
    source = get_metadata_resource_instance(configs)
    try:
        datasource_type = band_path.split('/')[2]
        datasource = get_dataset_source_by_path(band_path)
        if datasource_type not in get_config().delete_allowed:
            raise ValueError("Illegal deletion")

        with source.transaction() as db:
            bandMetadata = db.getBand(band_path, dataset=datasource)
            if bandMetadata is None:
                raise ValueError("No such band or image : {bandPath} ".format(bandPath=band_path))

            # 删除栅格数据
            metadata = Metadata(bandMetadata)
            client = storage_client_mapper.get(metadata.backend.type)
            if metadata.cropped:
                # 如果是分块数据
                gti = GlobalTileInfo(metadata.transform, metadata.shape[1], metadata.shape[0], metadata.tile_size)
                tiles = gti.get_tiles()
                faDir = metadata.backend.path
                if concurrency:
                    for tile in tiles:
                        global_thread_pool_executor.delete_tiles(client, tile[0], tile[1], faDir)
                else:
                    for tile in tiles:
                        client.delete(faDir + "/{}_{}.tif".format(*tile))
            else:
                # 如果不是分块数据
                client.delete(metadata.backend.path)

            # 删除Image中对应的波段
            imagePath = bandMetadata['image_path']
            if imagePath is not None:
                imageMetadata = db.getImage(imagePath, dataset=datasource)
                bands: Dict = imageMetadata['bands']
                newBands = {}
                for bandKey in bands.keys():
                    if bands.get(bandKey) == band_path:
                        continue
                    newBands[bandKey] = bands.get(bandKey)

                imageMetadata['bands'] = newBands
                db.updateImage(imageMetadata, dataset=datasource)
                if len(newBands.keys()) <= 0:
                    db.deleteImage(imagePath, dataset=datasource)
            return db.deleteBand(band_path, dataset=datasource)
    finally:
        source.close()


def _normal_image_meta(info: dict):
    info['name'] = info['image_name']
    info['objectid'] = info['image_path']
    info['dataid'] = info['image_path']
    info.pop('image_name')
    info.pop('image_path')
    info.pop('_id')


def query_by_filter(query_json: Union[str, dict],
                    datasource: str,
                    return_prop: list = None,
                    limit: int = 2000):
    infos = find_images(query_json, datasource=datasource, limit=limit)
    imagesInfo = []
    for info in infos['Images']:
        _normal_image_meta(info)
        tmp = {}
        if return_prop is not None:
            for index in return_prop:
                tmp[index] = info.get(index)
        else:
            tmp = info
        imagesInfo.append(tmp)
        del tmp
    infos['Images'] = imagesInfo
    return infos


def get_metadata(path: str):
    ext = path[path.rindex('.') + 1:]

    if ext.upper() == 'IMAGE':
        image = get_image(path.replace(ext, 'IMAGE'))
        _normal_image_meta(image)
        return image

    if ext.upper() in ['BAND', 'TIF', 'TIFF']:
        return get_band(path.replace(ext, 'BAND'))

    raise ValueError('Illegal band path or image path')


def exist(path: str):
    ext = path[path.rindex('.') + 1:]

    if ext.upper() == 'IMAGE':
        return exist_image(path.replace(ext, 'IMAGE'))

    if ext.upper() in ['BAND', 'TIF', 'TIFF']:
        return exist_band(path.replace(ext, 'BAND'))

    raise ValueError('Illegal band path or image path')


def unlink(path: str, concurrency=False):
    ext = path[path.rindex('.') + 1:]

    if ext.upper() == 'IMAGE':
        return unlink_image(path.replace(ext, 'IMAGE'), concurrency=concurrency)

    if ext.upper() in ['BAND', 'TIF', 'TIFF']:
        return unlink_band(path.replace(ext, 'BAND'), concurrency=concurrency)

    raise ValueError('Illegal band path or image path')
