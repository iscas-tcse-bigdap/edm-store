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

from typing import Union, List, Dict, Optional, Protocol


class MetadataOpsProtocol(Protocol):
    """
    Metadata操作所需要支持的操作协议。
    """

    def addImage(self, image_metadata: dict, dataset: str = "default") -> bool:
        """
        添加一条Image元数据, 其字段格式将被校验。

        :param image_metadata: 需要添加的Image元数据
        :param dataset: 数据集名称, 默认值为"default"

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def updateImage(
        self, image_metadata: dict, dataset: str = "default", upsert: bool = False
    ) -> bool:
        """
        更新一条Image元数据, 文档查找依据为'image_path'属性, 用于更新的文档的字段格式将被校验。

        :param image_metadata: 用于更新的的Image元数据
        :param dataset: 数据集名称, 默认值为"default"
        :param upsert: 当'image_path'字段不存在时，是否退化为插入一条新的元数据

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def deleteImage(
        self, image_path: Union[str, List[str]], dataset: str = "default"
    ) -> bool:
        """
        删除一条Image元数据, 文档查找依据为'image_path'属性。

        Example of 'image_path':
            1. '/edm_store/xxx/xxx.tif'
            2. ['/edm_store/xxx/xxx.tif','/edm_store/xxx/xxx.tif','/edm_store/xxx/xxx.tif']

        :param image_path: 对应Image的路径
        :param dataset: 数据集名称, 默认值为"default"

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def findImage(
        self,
        filter_json: Union[str, dict],
        return_props: List[str],
        dataset: str = "default",
        limit: int = 2000,
    ) -> Dict[str, Union[int, List[dict]]]:
        """
        查询影像元数据库, 通过传入的'filerJson', 查询满足条件limit条Image数据,
        返回的文档会根据'returnProp'筛选保留字段。

        Example of arguments and return value:

            - "filter_json":
                - {'sysTime': {'$gte': '2015-01-12', '$lte': '2019-12-30', '$neq': '2019-01-01'}}
                - '{"sysTime": {"$gte": "2015-01-12", "$lte": "2019-12-30", "$neq": "2019-01-01"}}'

            - "return_props":
                - ['ImagePath','sysTime','bands' ... ]

            - "dataset":
                - 'default' => 'image-default'
                - 'default' => 'image-landsat'

            - return:
                - {'Count': 210301, 'Images': [{},{}...]}

        :param filter_json: 查询条件, 可以是一个JSON字符串, 也可以是一个等价的字典
        :param return_props: 需要在返回中保留的字段列表。(不包含在列表中的字段将被全部丢弃)
        :param dataset: 数据集名称, 默认值为"default"
        :param limit: 最大返回记录数量限制, 默认值为2000

        :return: 一个包含 'Count' & 'Images' 两个属性的 `dict`, 前者表示查询到的文档数量, 后者为文档集合.
        """
        ...

    def getImage(self, image_path: str, dataset: str = "default") -> Optional[dict]:
        """
        根据"image_path"获取一条Image文档。
        (不能保证"image_path"是唯一的, 但查找假设其是唯一的, 需要上层确保不出现重复的"image_path")

        :param image_path: 对应Image的路径
        :param dataset: 数据集名称, 默认值为"default"

        Example of "image_path":
            - '/edm_store/xxx/xxx.tif'

        :return: 以`dict`的形式返回一个Image文档 (如果对应'image_path'下未查询到结果, 返回`None`)
        """
        ...

    def addBand(self, band_metadata: dict, dataset: str = "default") -> bool:
        """
        添加一条Band元数据, 其字段格式将被校验。

        :param band_metadata: 需要添加的Band元数据
        :param dataset: 数据集名称, 默认值为"default"

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def updateBand(
        self, band_metadata: dict, dataset: str = "default", upsert: bool = False
    ) -> bool:
        """
        更新一条Band元数据, 文档查找依据为'band_metadata'属性, 用于更新的文档的字段格式将被校验。

        :param band_metadata: 用于更新的的Band元数据
        :param dataset: 数据集名称, 默认值为"default"
        :param upsert: 当'band_metadata'字段不存在时，是否退化为插入一条新的元数据

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def deleteBand(
        self, band_path: Union[str, List[str]], dataset: str = "default"
    ) -> bool:
        """
        删除一条Band元数据, 文档查找依据为'band_path'属性。

        Example of 'band_path':
            - '/edm_store/xxx/xxx.tif'
            - ['/edm_store/xxx/xxx.tif','/edm_store/xxx/xxx.tif','/edm_store/xxx/xxx.tif']

        :param band_path: 对应Band的路径
        :param dataset: 数据集名称, 默认值为"default"

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败
        """
        ...

    def getBand(self, band_path: str, dataset: str = "default") -> Optional[dict]:
        """
        根据"band_path"获取一条Band文档。
        (不能保证"band_path"是唯一的, 但查找假设其是唯一的, 需要上层确保不出现重复的"band_path")

        :param band_path: 对应Band的路径
        :param dataset: 数据集名称, 默认值为"default"

        Example of "band_path":
            - '/edm_store/xxx/xxx.tif'

        :return: 以`dict`的形式返回一个Band文档 (如果对应'band_path'下未查询到结果, 返回`None`)
        """
        ...


class MetadataTransaction(MetadataOpsProtocol, Protocol):
    """
    该接口为一个包装了元数据事务操作的上下文管理器。

    (应当配合`with`语句使用)

    Note: 该接口的实例应当通过`MetadataResource.transaction()`获取, 具体信息请参见对应方法的Docstring。
    """

    def __enter__(self) -> "MetadataTransaction":
        """
        Context-manager所需要的实现的方法, 进入`with`块时被调用。
        (该方法不需要被显示调用)

        :return: `MetadataTransaction`实例的引用
        """
        ...

    def __exit__(self, exc_type, exc_value, exc_traceback) -> bool:
        """
        Context-manager所需要实现的方法, 退出`with`块时被调用。
        (该方法不需要被显示调用)

        :return: 返回一个`bool`, `True`表示成功, `False`表示失败, 若失败则向上传递异常
        """
        ...


class MetadataResource(MetadataOpsProtocol, Protocol):
    """
    该接口包含了一系列用于操作数据库中存储的元数据的行为。
    """

    def transaction(self) -> MetadataTransaction:
        """
        开启一个事务, 返回的`MetadataTransaction` 实现了 Context-manager protocol,
        需要使用`with`语句包裹事务逻辑。

        Example:
            >>> resource: MetadataResource = MetadataResource({"host": "localhost", "port": 27017})
            >>> with resource.transaction() as t:
            ...     # do something with `t`
            ...     # transaction will submit if no exception raised
            ...     # transaction will abort if exception raised
        """
        ...

    def close(self) -> None:
        """
        释放底层维护的所有资源。
        """
        ...
