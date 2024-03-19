# EDM-STORE

[English](./README.md) / 简体中文

`edm_store` 函数库依据对于并发场景下，针对遥感数据的`读取`、`写入`、`检索`等操作，提供了
统一的接口。支持对不同来源和类型的遥感数据进行处理。根据存储与计算的需求，对部分
遥感数据进行细粒度的切分，实现 `分层分块` 的方式读取数据，从而适应 `分布式` 处理的场景。
此外，`edm_store` 还具有遥感数据的分类管理功能，便于快速 `检索` 所需的数据。
在 `edm_store` 中，会将数据按照指定的范围进行切分，从而实现分块的读取与写入数据。

## 依赖项目

### 系统需求

- `Mongodb`
- `Python 3.9+`
- `GDAL 2.0.0+`
- `rasterio 1.3.9`

### Python包依赖

- 运行依赖清单参见 `/requirements.txt`;
- 开发依赖清单参见 `/requirements-dev.txt`
- 测试依赖清单参见 `/requirements-test.txt`

> 请根据使用目的从以上清单中安装依赖。

## 快速开始

[此处查看快速入门案例](./docs/quick-start.md)

## 应用案例

本项目提供了一个后端由`edm_store`构建的查询与可视化应用案例，
当前仅支持北京市在Landsat8数据集下的查询。

[此处查看应用案例](http://earthdataminer.casearth.cn/map/)
