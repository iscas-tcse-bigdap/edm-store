# Quick Start

## step 1 安装

## step 2 配置
`edm_store`的配置项保存在一个名叫`edm_store_config.json`的文件中(也可以使用yaml的形式存储),主要的配置项如下:

    storage_client_config 存储介质配置
    metadata_config 元数据存储配置
            ├─ db_config 数据库相关配置
            └─ datasource_config datasource配置

一个简单的配置案例如下：

```json
{
  "storage_client_config": {
    "ceph_rgw_landsat": {
      "configure_params": {
        "access_key": "{{ your_access_key }}",
        "secret_key": "{{ your_secret_key }}",
        "host": "{{ your_ceph_rgw_host_ip}}",
        "port": "{{ your_port }}",
        "bucket": "{{ bucket_name }}"
      },
      "type": "{{ ceph_rgw / s3 }}" 
    },

    "edm_fs": {
      "configure_params": {
        "base_directory": "{{ base_directory }}"
      },
      "type": "fs"
    }
  },
  "metadata_config": {
    "db_config": {
      "host": "{{ mongodb host ip }}",
      "port": "{{ mongodb port }}",
      "database": "{{ data_base_name }}",
      "tzAware": true,
      "connect": true,
      "max_pool_size": 8,
      "username": "{{ mongodb_username }}",
      "password": "{{ mongodb_password }}"
    },
    "datasource_config": {
      "default": {
        "alias": "ds0",
        "authority": [
          "create",
          "read",
          "write",
          "delete"
        ]
      },
      "landsat": {
        "alias": "ds1",
        "authority": [
          "read"
        ]
      },
      "sdg": {
        "alias": "ds2",
        "authority": [
          "read"
        ]
      }
    }
  }

}
```
`storage_client_config` 用于配置存储后端目前支持的存储后端为 `s3`/`ceph_rgw`/`fs`三种类型，你可以在`storage_client_config`配置你的
存储后端，并在`type`中指定具体属于哪一种存储后端，对于上述样例中出现的`ceph_rgw_landsat`与`edm_fs`是由你自己进行命名，你可以随意命名这样的
存储后端，只需要在`type`中注释是哪一种存储类型即可，并确保配置与对应的类型相同。

`metadata_config` 用于配置元数据存储相关，其中`db_config`配置与`mongodb`连接相关参数，如果你的mongodb没有设置密码与用户名，就可以不用填写
`username`与`password`两个字段。`datasource_config`用于配置元数据的数据源类型。,当一个数据存储在特定数据源中时，其访问路径会组合成
`/edm_store/{{ datasource.alias }}/{{ path }}`的形式 

你可以将配置文件保存在`/etc/edm_store_config.json`中或者在系统环境变量设置`EDM_STORE_CONFIG_PATH`去指定配置文件的路径，又或者你可以使用
`init_config`方法去初始化配置，具体如下：
```python
import edm_store
config_path = 'your path here'
edm_store.init_config(config_path)

# or use dict
config_dict = {'storage_client_config': '...',
               'metadata_config': '...'}
edm_store.init_config(config_dict)
```

## step3 使用

### Opening a dataset

对于保存进edm_store中的数据可以使用`edm_store.Dataset`类打开，或者使用`edm_store.open_dataset`打开
对于数据的切分方式会返回一个`Band`对象用于操作当前数据:

首先使用 `import edm_store` 引入包。

然后根据当前数据的存储路径获取当前数据集对象

```python
import edm_store

data_path = '/edm_store/test/example.BAND' # 保存在edm_store中的数据路径
dst = edm_store.Dataset(data_path) # 获取当前的数据集对象
```
或者使用`edm_store.open_dataset`打开数据集

```python
import edm_store

dst = edm_store.open_dataset('/edm_store/test/example.BAND')
```
之后在获取当前读取对象之后就可以操作当前的数据集

### Getting attributes

在获取了当前数据的操作对象之后就可以获取遥感数据中常用的属性：

```python
import edm_store

dst = edm_store.open_dataset('/edm_store/test/example.BAND')

dst.get_raster_count() #当前数据的波段数量
dst.get_projection() #当前数据的坐标系
dst.get_size() #当前数据的尺寸 （width, height)
dst.get_geo_transform() #当前数据的transform 
dst.get_nodata_value() #当前数据的nodata
dst.get_raster_data_type() #当前数的datatype
dst.get_extent() #当前数据的范围信息 (min_x,min_y,max_x,max_y)
```
也可以获取当前数据的分块相关信息：

```python
import edm_store

dst = edm_store.open_dataset('/edm_store/test/example.BAND')

dst.get_tiles() # 获取所有的切分块
dst.get_tile_info(0, 0) # 获得（0，0）块的信息 返回该tile的transform shape
dst.get_all_tile_infos() # 获取所有切分块 与其对应的transform和shape
dst.get_grid_info(0,0) # 获得某块的大小信息

```

### Read
`edm_store`提供了两种读取的方式，一种是按照tile来进行读取，另一种是根据特定范围进行读取
```python
import edm_store

band_path = '...'

dst = edm_store.open_dataset(band_path, tile_size=4096)
# read data from 0,0
data = dst.read_tile(0, 0)

transform, shape = dst.get_tile_info(0,0)
data = dst.read_region(transform, shape[1], shape[0])

x, sx,_,y,_,sy = transform
# 将读取的范围向左平移200， 向下移动100
transform = [x + sx*200, sx, 0, y + sy*100, 0, sy]
data = dst.read_region(transform, shape[1], shape[0])
```
如果输入的范围与当前数据不在同一个地理坐标系下，只能使用read_region方法，需要指定当前坐标，以及重采样的方法

```python
import edm_store
band_path = '...'

dst = edm_store.open_dataset(band_path, tile_size=4096)

transform = ...
shape = ...
proj = 'epsg:4326'
resample = 'nearest'

dst.read_region(transform=transform, xSize=shape[1], ySize=shape[0], project=proj, resample=resample)
```
### Write

*注意：当前只有分块数据(`SlicedBand`)才提供写入的方法,并且只有当前数据在元数据中指定了`readonly`为`false`，同时打开数据的尺寸与数据的切分
尺寸相同时才能写入*

写方法也提供了按照tile写入与指定范围写入的方式

```python
import numpy
import edm_store

data = numpy.zeros((20,30))

band_path = '...'
dst = edm_store.open_dataset(band_path)
# 将数据写入（0，0）tile
dst.write_tile(0,0,data)

transform = ...
dst.write_region(transform, data)
```
数据必须与当前数据在同一坐标系下，如果数据分辨率或者不同则会对数据进行重采样，之后再进行写入。

### Create
在`edm_store`中创建一个数据，需要先构建band的元数据信息，之后再按照tile或者region将数据写入至band中

```python
import numpy
import edm_store

band_path = '/edm_store/{{ datasource.alias }}/{{ path }}_B1.BAND'
# 创建image,image中包含有多个band,每个band都会以dataset的形式打开
image_path = '/edm_store/{{ datasource.alias }}/{{ path }}.IMAGE'
edm_store.create_image(crs='epsg:4326', shape=[0, 0], transform=[180, 1, 0, 90, 0, -1], image_path=image_path, bands={
    'b1': '{{ band_path }}'
})
edm_store.create_band(crs='epsg:4326', shape=[0, 0], transform=[180, 1, 0, 90, 0, -1], band_path=band_path, nodata=0,
                      data_type='int8', tile_size=2048)

data = numpy.zeros([0,0])
dst = edm_store.open_dataset(band_path)
dst.write_tile(0,0, data )
```
或者也可以使用已经打开的数据创建一个空白数据
```python
import edm_store

band_path = '/edm_store/{{ datasource.alias }}/{{ path }}_B1.BAND'
dst = edm_store.open_dataset(band_path)

new_band_path = '/edm_store/{{ datasource.alias }}/{{ path }}_B2.BAND'
dst.create_dataset(new_band_path, 'int8', 0)

new_dst = edm_store.open_dataset(new_band_path)
```