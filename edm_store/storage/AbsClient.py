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

from abc import ABC, abstractmethod
from typing import Tuple, Union


class AbsBackendClient(ABC):
    """
    后端存储Client
    """

    @abstractmethod
    def mk_dirs(self, directory: str) -> Union[None, str]:
        """
        在当前存储环境中递归创建文件目录
            :param directory str 对象数据的相对路径目录
            e.g.
                _dirs '/edm-store/test/data

            :return (dirs str, path str) path是在当前存储中文件实际的目录路径或者绝对目录路径
            e.g.
                ('edm-store/test/data', '{abs_dirs}/edm-store/test/data')
        """

    @abstractmethod
    def upload_by_bytes(self, object_name: str, stream: bytes, directory: str = '') -> Tuple[bool, str]:
        """
        将一段二进制流上传到该存储的指定目录下
        :param object_name str 保存对象名称
        :param stream bytes 二进制数据
        :param directory str 对象数据的相对路径目录
        e.g.
            object_name '0_0.tif'
            stream  b"just for example"
            _dirs '/edm-store/test/data/'

        :return (True/False, path str) path是在当前存储中文件实际的目录路径或者绝对目录路径
        e.g.
            True, '{abs_dirs}/edm-store/test/data/0_0.tif'
            False, '{abs_dirs}/edm-store/test/data/0_0.tif'
        """

    @abstractmethod
    def upload_by_file(self, file_name, object_name, directory: str = '') -> bool:
        """"""


    @abstractmethod
    def exist(self, object_path: str) -> bool:
        """
        提供当前对象的相对路径查看该数据是否存在
        :param object_path str 对象数据的相对路径
        e.g.
            '/edm-store/path/test/0_0.tif'
        :return True/False
        """

    @abstractmethod
    def is_accessible(self, object_path: str) -> bool:
        """
        提供当前对象的绝对路径查看该数据是否可以正常访问
        :param object_path str 当前数据在该存储中的绝对路径,也可以是路径映射
            e.g.
                '/mnt/cephfs/edm-store/path/test/0_0.tif' linux
                'f:/edm-store/data/edm-store/path/test/0_0.tif' windows

        :return True/False
        """

    @abstractmethod
    def delete(self, object_path: str) -> bool:
        """
        根据提供的信息进行数据删除:
        :param object_path str 对象数据的相对路径
        e.g.
            '/edm-store/path/test/0_0.tif'
        :return True/False
        注意:
        传入目录指定对象的话有些client会将数据该目录下所有的子文件,子目录全部删除,有些不会删除
        """
        ...

    @abstractmethod
    def get_access_path(self,
                        object_path: str
                        ) -> Union[None, str]:
        """
        获取当前目录下对象的访问路径
        :param object_path str 当前数据在该存储中的绝对路径,也可以是路径映射
        e.g.
            '/edm-store/path/test/0_0.tif'
        :return None/str 如果返回None 当前数据无法访问， 否则返回具体路径路径可以是正常的文件目录或者url
        """
        ...

    @abstractmethod
    def size_of(self,
                object_path: str
                ) -> Union[int, None]:
        ...
