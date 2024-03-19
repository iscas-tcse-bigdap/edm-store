## 使用Spark来计算或者读取数据
这里使用一个简单的示例来统计某个波段数据在某个范围下数值等于2的像素的实际面积之后与占该区域的比例

> 使用spark来进行计算或者读取数据时，需要保证在spark的每个节点中edm_store的配置相同

构建在每个node中使用edm_store操作对应Tile的函数

```python
def calculate_area(params):
    """
    在每个节点访问src_band中tile下与target_geojson相交的区域，并将相交的部分用 3 填充
    """
    import edm_store
    from edm_store import PixelAreaBand
    import numpy as np

    src_band, target_geojson, geo_crs, tile = params
    src_dst = edm_store.open_dataset(src_band)
    pixel_area_band = PixelAreaBand().apply(src_dst)
    
    # 用3填充不相交区域
    old_data, _ = src_dst.mask_tile(tile[0], tile[1], target_geojson, geo_crs, 3)

    # 将相交区域全部置为1
    clip_data = np.where(old_data == 3, 0, 1)

    # 将值为2作为计算的像素
    masked_data = np.where(old_data == 2, 1, 0)

    transform, shape = src_dst.get_tile_info(*tile)
    
    # 获取当前范围在等面积坐标系下的面积矩阵
    pixel_area_data = pixel_area_band.read_region(transform, shape[0], shape[1])

    return np.sum(pixel_area_data * masked_data), np.sum(pixel_area_data * clip_data)

```
构建一个用于reduce的函数用于汇总所有节点的数据
```python
def sum_area(x, y):
    return x[0] + y[0], x[1] + y[1]
```

封装rdd，并查看每个节点的结果

```python
import edm_store

band_path = '{ an raster data here }'

src_dst = edm_store.open_dataset(band_path)
pixel_area_band = edm_store.PixelAreaBand().apply(src_dst)

# 需要统计的范围
target_geojson = '{"type":"MultiPolygon","coordinates":[[[[12893719.14014622,4252554.063966703],[12885784.947625166,4016795.2004839457],[13168015.510159813,4104071.318215543],[13054669.902716178,4244619.871445648],[12893719.14014622,4252554.063966703]]]]}'

# target_geojson对应的坐标系
geo_crs = 'EPSG:3857'

# src_dst.query_tiles(target_geojson, geo_crs) 获取到当前数据在该范围下有多少个Tile
params = [(band_path, target_geojson, geo_crs, tile) for tile in src_dst.query_tiles(target_geojson, geo_crs)]

rdd = sc.parallelize(params)
rdd = rdd.map(calculate_area)
rdd = rdd.reduce(sum_area)
sc.stop()

area_target, area_total = rdd

print('target area: {}'.format(area_target))
print('total  area: {}'.format(area_total))
print('percent of target area: {}'.format(area_target / area_total * 100))
```
## 使用Spark来写数据

根据查询条件查询符合条件的影像，并以第一个波段段数据作为蓝本创建新的空白影像，并将每个节点需要的数据封装
```python
import edm_store
from edm_store import query_by_filter, create_image

# 查询条件
query_filter = """
{
  "year": {
    "$in": [2015, 2020]
  }
}"""

# 查询相关数据
source_image_collection = query_by_filter(query_filter, datasource='landsat')['Images']
source_image = source_image_collection[0]

# 获取当前数据的B1波段
b1_band = edm_store.open_dataset(source_image['bands']['B1'])


# create target image
new_image_path = ''


if edm_store.exist(new_image_path):
    edm_store.unlink(new_image_path)

bands_names = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']
band_infos = {}
for band_name in bands_names:
    band = source_image.get(band_name)
    new_band_path = new_image_path.replace('.image', f'_{band_name}.band')
    band.create_dataset(new_band_path, band.datatype, band.nodata, imagePath=new_image_path)
    band_infos[band_name] = new_band_path

create_image(b1_band.get_projection(), b1_band.get_size(), b1_band.transform, new_image_path, band_infos)


# param = (source, tile, transform, shape) 封装每个节点需要处理的tile
params = [(source_image_collection, tile_info[0], tile_info[1], tile_info[2], band_infos) for tile_info in
         b1_band.get_all_tile_infos()]
```
构建每个节点运行的函数
```python
import edm_store
import numpy as np

def gen_mask(src_image, transform, shape):
    # 利用PIXEL-QA来实现去云
    src_band = edm_store.open_dataset(src_image['bands']['PIXEL-QA'])
    data = src_band.read_region(transform,shape[0], shape[1])
    mask_val = 0b1000010
    return np.asarray(data & mask_val == mask_val, 'byte')

def decloud_mapper(params):
    source, tile, transform, shape, target_band_infos = params
    target_data_dict = {}
    print(f"calculate tile : {tile}")
    # 遍历image_collection，对每个image中的每个band读取特定的tile
    for src_image in source:
        mask_array = gen_mask(src_image, transform, shape)
        for inner_band_name in ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']:
            src_band_name = src_image['bands'][inner_band_name]
            src_band = edm_store.open_dataset(src_band_name)
            src_data = src_band.read_region(transform, shape[0], shape[1])
            if inner_band_name in target_data_dict:
                target_data_dict[inner_band_name].append(np.ma.array(src_data,mask=mask_array)
)
            else:
                target_data_dict[inner_band_name] = [np.ma.array(src_data,mask=mask_array)]

    # 将这些tile取中位数保存进新创建的dataset中
    print(f"write tile : {tile}")
    for target_band_name in ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']:
        if target_band_name in target_data_dict:
            target_data_list = target_data_dict[target_band_name]
            target_data = np.median(np.asarray(target_data_list), axis=0)
            # 写入数据
            edm_store.open_dataset(target_band_infos.get(target_band_name)).write_region(transform, target_data)
```
构建rdd，并获取结果
```python
rdd = sc.parallelize(params)
rdd = rdd.map(decloud_mapper)
rdd = rdd.collect()
sc.stop()
```