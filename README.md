# EDM-STORE

English / [简体中文](./README_CN.md)

The `edm_store` library provides a unified interface for
`read` / `write` / `retrieve` operations of remote sensing data
in concurrent scenarios. It supports processing of
remote sensing data from different sources and types.
According to the storage and computation requirements,
some remote sensing data are sliced and diced at a fine granularity,
so that the data can be read in a `layered and chunked` way,
thus adapting to the scenery of `distributed` processing.
In addition, `edm_store` has the function of categorizing and
managing remote sensing data, which makes it easy to quickly
`retrieve` the required data. In `edm_store`, the data is
sliced and diced according to specified ranges,
thus enabling chunked reading and writing of data.

## Requirements

### System-Requirements

- `Mongodb`
- `Python 3.9+`
- `GDAL 2.0.0+`
- `rasterio 1.3.9`

> Please install the dependencies according to your usage.

### Python-Package-Requirements

- see runtime requirements in `/requirements.txt`;
- see development requirements in `/requirements-dev.txt`
- see test requirements in `/requirements-test.txt`

## Quick-Start

[check our quick-start demo HERE](./docs/quick-start.md)

## Application-Demo

Here is a demo of a query and data visualization built on
`edm_store` backend.
Note the demo only supports queries in Beijing city from Landsat8 dataset currently.

[check out application demo HERE](http://earthdataminer.casearth.cn/map/)
