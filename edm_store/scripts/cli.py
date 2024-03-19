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
import logging
import math
import os
import datetime

import click
import psutil
import rasterio
import yaml
from osgeo import gdal
from rasterio.enums import _OverviewResampling

import edm_store
from click import echo
from tqdm import tqdm

from edm_store import load_dataset_from_file
from edm_store.config import LocalConfig, ENVIRONMENT_VARNAME
from edm_store.dm.db import get_metadata_resource_instance
from edm_store.dm.meta._model import BandMetadata, ImageMetadata
from edm_store.storage import storage_client_mapper
from edm_store.utils.tools import rebuilt_path

_LOG = logging.getLogger('edm_store')


@click.group(help="Edm-Store command-line interface")
def cli():
    pass


# ------------------ image ------------------
@cli.group(name='image', help='Image management commands')
def image_cmd():
    pass


@image_cmd.command("query", help='query images in EDM Store')
@click.option('--dataset', '-d', type=str, default=None,
              help='Which dataset')
@click.option('--query_json_file', '-f', type=str, default=None,
              help='Query json file')
@click.option('--query_json_json', '-j', type=str, default=None,
              help='Query json file')
@click.option('--limit', '-l', type=int, default=10, show_default=True,
              help='Number of lines to return')
def query_images(dataset, query_json_file, query_json_json, limit):
    data = None
    if query_json_file is not None:
        if os.path.exists(query_json_file):
            with open(query_json_file, 'r') as file:
                ctx = file.read()
            if query_json_file.endswith('.json'):
                data = json.loads(ctx)
            elif query_json_file.endswith('.yaml'):
                data = yaml.load(ctx, yaml.FullLoader)
            else:
                raise TypeError("Invalid config file format")
    elif query_json_json is not None:
        data = json.loads(query_json_json)
    else:
        data = {}

    click.echo(edm_store.query_by_filter(data, dataset, limit=limit))


# ------------------  band ------------------
@cli.group(name='band', help='Band management commands')
def band_cmd():
    pass


@band_cmd.command('add', help="Add band to the EDM Store", )
@click.option('--image_name', '-m', 'image_name', type=str, default=None,
              help=('Used to specify the image to which the current data belongs.'
                    'If the current image is specified but does not exist, a new image will be created based on it.')
              )
@click.option('--dataset', '-dataset', type=str, default=None,
              help=('Which dataset does the current data belong to'
                    'The common dataset is default')
              )
@click.option('--storage_type', '-s', type=str, default=None,
              help='Which storage type does the current data to use')
@click.option('--prefix', '-p', 'prefix', type=str, default=None,
              help='Specify the prefix for the access path after data upload.'
              )
@click.option('--shard', is_flag=False, default=False,
              help=('Determine whether the data is sharded for storage.'
                    'If true, internal sharding will be used for sharding storage.')
              )
@click.option('--directory', '-d', type=str, default=None,
              help='The directory where the files to be uploaded are located.'
              )
@click.option('--files', '-f', type=str, default=None,
              help='Determine which files in the directory need to be uploaded.'
              )
@click.option('--tile_size', '-t', type=int, default=2048,
              help='Determine which tile size .'
              )
def add(image_name, dataset, storage_type, prefix, shard, directory, files, tile_size):
    __UPLOAD_FILES = []
    mongodb = None
    client = None
    try:
        if directory is None or directory == '' or os.path.exists(directory) is False:
            click.echo('No such directory or directory does not exist : {}'.format(directory))
            return

        config = LocalConfig()

        if storage_type is None or storage_type == '':
            storage_type = config.base_store_type
        else:
            click.echo('No such storage type : {}'.format(storage_type))
            return

        client = storage_client_mapper.get(storage_type)

        mongodb = get_metadata_resource_instance(config.db_config)

        if dataset is None or dataset == '' or dataset not in config.datasource_config.keys():
            click.echo('No such dataset: {}'.format(dataset))
            return

        if files is None or files == '':
            files = [f for f in os.listdir(directory)]
            if len(files) == 0:
                click.echo('No files found')
                return
        else:
            if ',' in files:
                files = str(files).split(',')
            else:
                files = [files]

        tmp_files = []
        for f in files:
            if os.path.isfile(directory + '/' + f) and f.split('.')[-1].lower() in ['tif', 'tiff']:
                tmp_files.append(f)

        files = tmp_files

        index = 1
        with tqdm(total=len(files)) as pbar:
            for file in files:
                pbar.set_description('current file: {}'.format(file))
                if os.path.exists(directory + '/' + file):
                    __UPLOAD_FILES = __gen_metadata(directory + '/' + file,
                                                    file,
                                                    dataset,
                                                    prefix,
                                                    shard,
                                                    f'B{index}',
                                                    image_name,
                                                    config,
                                                    storage_type,
                                                    tile_size,
                                                    client,
                                                    mongodb,
                                                    __UPLOAD_FILES)
                    index += 1
                pbar.update()
        click.echo('+' * 16 + '[ succ: {} , error: {} ]'.format(len(__UPLOAD_FILES),
                                                                str((len(files) - len(__UPLOAD_FILES)))) + '+' * 16)
        for item in __UPLOAD_FILES:
            click.echo(f"* {item['name']}\t{item['path']}")
    finally:
        if mongodb is not None: mongodb.close()
        if client is not None: client.close()


@band_cmd.command(help="Find band metadata", )
@click.option('--band_path', '-b', type=str, default=None,
              help='Which storage type does the current data to use')
def find_band(band_path):
    metadata = edm_store.dm.get_metadata(band_path)
    click.echo(metadata, color=True)


@band_cmd.command(help="Delete band ( Warning: this operation will delete band and data )", )
@click.option('--band_path', '-b', type=str, default=None,
              help='The path access to data')
def delete_band(band_path):
    result = edm_store.unlink(band_path)
    if result:
        click.echo("Delete {} success".format(band_path), color=True)
    else:
        click.echo("Delete {} fail".format(band_path), color=True, err=True)


def __gen_metadata(file_path: str, name: str, dataset: str, prefix: str, shard: bool, band_num: str, image_name: str,
                   config: LocalConfig, storage_type: str, tile_size: int, client, mongodb, __UPLOAD_FILES):
    """
    Generate metadata for tif and upload to ceph and mongodb
    @param file_path: The path for file need to be generated
    @param name: The file name
    @param dataset: The dataset name
    @param prefix: The prefix of the key
    @param shard: The file need shard or not
    @param band_num: The num of file
    @param image_name: The file belong to which image
    """
    messages = []
    # 从文件中构建edm_store数据集
    dst = load_dataset_from_file(file_path, 2048)
    # 上传的数据集名称后缀为BAND
    band_name = name[:name.rindex('.')] + ".BAND"

    # 获得当前dataset的昵称
    dataset_alias = config.datasource_config.get(dataset)['alias']

    if prefix is not None and prefix != '':
        _dir = '/edm_store/' + dataset_alias + '/' + rebuilt_path(prefix)
    else:
        _dir = '/edm_store/' + dataset_alias

    infos = []

    need_create_image = False

    if image_name is None or image_name == "":
        # 当前波段需要存储进image中,但是并没有指定image_name
        need_create_image = True
        image_name = _dir + '/' + str(band_name).replace(".BAND", f".IMAGE")
    else:
        if not str(image_name).endswith('.IMAGE'):
            image_name = str(image_name).replace('.', '_')
            image_name = image_name + '.IMAGE'
        image_name = _dir + '/' + image_name

    imageMetadata = None
    band_names_tmp = {}

    if not need_create_image:
        click.echo('Create image : {image_name}'.format(image_name=image_name))

    if dst.get_raster_count() > 1:
        # 多波段的返回信息
        band_names = {}

        for i in range(1, dst.get_raster_count() + 1):
            # 构建多波段遥感数据中每个波段的信息
            sub_band_name = str(band_name).replace(".BAND", f"_{i}.BAND")
            sub_name = name[:str(name).rindex(".")] + f"_{i}" + name[str(name).rindex("."):]
            band_path = edm_store.utils.tools.rebuilt_path(_dir + '/' + sub_band_name)
            real_path = edm_store.utils.tools.rebuilt_path(_dir + '/' + sub_name)
            band_names[str(i)] = band_path
            band_names_tmp[band_num + f"_{i}"] = band_path
            infos.append({'index': i, "band_path": band_path, "real_path": real_path, "band_name": band_num + f"_{i}"})

        if need_create_image:
            # 需要根据当前数据创建image返回的信息
            imageMetadata = ImageMetadata(dst.get_projection(),
                                          dst.get_size(),
                                          dst.get_geo_transform(),
                                          image_name,
                                          band_names)

    else:
        # 单波段的返回信息
        band_path = edm_store.utils.tools.rebuilt_path(_dir + '/' + band_name)
        real_path = edm_store.utils.tools.rebuilt_path(_dir + '/' + name)
        infos.append({'index': 1, "band_path": band_path, "real_path": real_path, "band_name": band_num})
        band_names_tmp[band_num] = band_path

    if not shard:
        # 不需要进行切分 transform 为原始的 transform
        transform = dst._dst.GetGeoTransform()
    else:
        # 需要进行切分 transform 为扩充后的 transform
        transform = dst.get_geo_transform()

    for info in infos:

        _real = info['real_path']
        if shard:
            _real = info['real_path'].split('.')[0]

        bandMetadata = BandMetadata(
            crs=dst.get_projection(),
            shape=dst.shape,
            transform=transform,
            bandPath=info['band_path'],
            realPath=_real,
            nodata=dst.get_nodata_value(info['index']),
            rasterCount=1,
            dataType=dst.get_raster_data_type(info['index']),
            storeType=storage_type,
            tileSize=2048,
            cropped=shard,
            readonly=True
        )

        if edm_store.exist(info['band_path']):
            edm_store.unlink(info['band_path'])

        if dst.get_raster_count() > 1:
            # 如果当前数据存在多个波段
            name = file_path[:str(file_path).rindex('.')] + f"_{info['index']}" + '.tif'
            factors, x_factors, y_factors = __save_mult_band(dst._dst, name, info['index'],
                                                             dst.get_nodata_value(info['index']), dst.gti._factors)
        else:
            factors = dst.gti._factors
            dst.close()
            factors, x_factors, y_factors = __save_single_band(file_path, factors)
            name = file_path
            dst = load_dataset_from_file(file_path, 2048)

        if not shard:
            upload_res = client.upload_by_file(name, info['real_path'])

            if not upload_res:
                # 上传失败记录
                with open('error_result.txt', 'r') as f:
                    f.write('[{}][ERROR]{} upload fail \n'.format(datetime.datetime, name))
                continue

        else:
            _dst = load_dataset_from_file(name, 4096)
            new_dst = edm_store.Dataset(info['band_path'], 4096)
            for tile in new_dst.get_tiles():
                new_dst.write_tile(tile[0], tile[1], _dst.read_tile(*tile), True)
            new_dst.close()

        metadata = bandMetadata.export_to_dict()
        metadata['factors'] = factors
        metadata['sx_factors'] = x_factors
        metadata['sy_factors'] = y_factors

        if not mongodb.addBand(bandMetadata.export_to_dict(), dataset):
            with open('error_result.txt', 'r') as f:
                f.write('[{}][ERROR]{} metadata upload fail \n'.format(datetime.datetime, name))
            continue

        __UPLOAD_FILES.append({'name': name, 'path': info['band_path']})

    if not need_create_image:
        # 需要将band加入到用户指定的波段中
        image_data = mongodb.getImage(image_name, dataset)
        if image_data is not None:
            bands = image_data['bands']
            bands.update(band_names_tmp)
            image_data['bands'] = bands
            mongodb.updateImage(image_data, dataset)
        else:
            imageMetadata = ImageMetadata(dst.get_projection(),
                                          dst.get_size(),
                                          dst.get_geo_transform(),
                                          image_name,
                                          band_names_tmp)
            image_data = imageMetadata.export_to_dict()

            mongodb.addImage(image_data, dataset)

    else:
        # 需要构建新的image
        if imageMetadata is not None:
            mongodb.addImage(imageMetadata.export_to_dict(), dataset)

    return __UPLOAD_FILES


# ----------------------------------------
# 读取TIF数据的金字塔信息
# 将多波段数据转换为多个单波段数据
# ----------------------------------------

def __memory_usage():
    """
    Calculate the memory usage
    """
    mem_available = psutil.virtual_memory().available >> 20  # 可用内存
    mem_process = psutil.Process(os.getpid()).memory_info().rss >> 20  # 进程内存
    return mem_process, mem_available


def __get_block(width: int, height: int, bands: int):
    """
    Calculate the block size of Tif
    """
    p, a = __memory_usage()
    bl = (a - 2000) / (width * height * bands >> 20)
    if bl > 3:
        block_size = 1
    else:
        block_size = math.ceil(bl) + 4

    bl_height = int(height / block_size)
    mod_height = height % block_size

    return block_size, bl_height, mod_height


def __save_mult_band(ds, out_fn, index, nodata, factors=None):
    """
    clip a multi-bands raster to many single bands
    """
    width, height, bands = ds.RasterXSize, ds.RasterYSize, ds.RasterCount

    # 分块
    bl_size, bl_each, bl_mod = __get_block(width, height, bands)
    # 提取分块区域位置(起点,行数)
    block_region = [(bs * bl_each, bl_each) for bs in range(bl_size)]
    if bl_mod != 0:
        block_region.append([bl_size * bl_each, bl_mod])

    # 输出结果保存
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(out_fn, width, height, 1, ds.GetRasterBand(index).DataType, options=["COMPRESS=LZW"])
    out_ds.SetGeoTransform(ds.GetGeoTransform())
    out_ds.SetProjection(ds.GetProjection())
    out_ds.GetRasterBand(1).SetNoDataValue(nodata)

    # 分块计算并存入计算机
    for h_pos, h_num in block_region:
        arr = ds.GetRasterBand(index).ReadAsArray(0, h_pos, width, h_num)
        out_ds.GetRasterBand(1).WriteArray(arr, 0, h_pos)

    out_ds.FlushCache()
    out_ds = None

    _factors = [1]

    # 用于构建并获取当前数据的金字塔系数
    if factors is not None:
        with rasterio.open(out_fn, 'r+') as dst:
            dst.build_overviews(factors, _OverviewResampling.nearest)
            _factors += dst.overviews(1)
            dst.close()

    return __gen_scales(out_fn, _factors)


def __save_single_band(real_path: str, factors: list):
    """
    Save a single band and create overviews if not exist
    :param real_path: Real path to raster
    :param factors: List of factors you want to create if not exist
    """
    if len(factors) == 1:
        need = False
    else:
        factors = factors[1:]
        need = True

    _factors = [1]

    if factors is not None:
        with rasterio.open(real_path, 'r+', compress='lzw') as dst:
            if need:
                if dst.overviews(1) is None or len(dst.overviews(1)) == 0:
                    dst.build_overviews(factors, _OverviewResampling.nearest)
                _factors += dst.overviews(1)
            dst.close()
    return __gen_scales(real_path, _factors)


def __gen_scales(real_path, _factors):
    """
    Get scale in x and scale in y from Raster
    @param real_path: Real path for raster
    @param _factors: Factors to raster
    """
    x_factors = []
    y_factors = []
    for i in range(len(_factors)):
        # 逐层获取每层x方向与y方向上的分辨率
        if i == 0:
            with rasterio.open(real_path) as dst:
                transform = dst.transform.to_gdal()
                x_factors.append(transform[1])
                y_factors.append(transform[-1])
                dst.close()
        else:
            with rasterio.open(real_path, overview_level=i - 1) as dst:
                transform = dst.transform.to_gdal()
                x_factors.append(transform[1])
                y_factors.append(transform[-1])
                dst.close()
    return _factors, x_factors, y_factors


# ----------------------------------------


# ------------------ config ------------------
@cli.group(name='config', help='Config management commands')
def config_cmd():
    pass


@config_cmd.command('init', help='Initialize Config File')
@click.option('-config-file', '-c', required=True, help='Config file path')
@click.option('--check-only', is_flag=True, default=False, help='Check only')
def init(config_file, check_only):
    try:
        LocalConfig(config_file)
        echo("Successfully Checking Config")
        if not check_only:
            os.environ[ENVIRONMENT_VARNAME] = config_file
            echo("Successfully initialized Config")
    except Exception as e:
        _LOG.error(e)
