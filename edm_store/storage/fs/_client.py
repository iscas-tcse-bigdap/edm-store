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

"""
Backend Client for storing data to filesystem and simply managing it.
This provides functions to store, check, delete and retrieve data
"""
import logging
import os
from typing import Optional, Union

from edm_store.storage import AbsBackendClient

# ============ constant =============
_BASE_DIRECTORY = '/opt/data'


# ===================================

class FSClient(AbsBackendClient):

    def __init__(self, base_config: Optional[dict] = None):
        self.__base_dir = _stitch_paths(_BASE_DIRECTORY) if base_config is None else _stitch_paths(
            base_config['BASE_DIRECTORY'])

    def mk_dirs(self, directory: str) -> Union[None, str]:
        try:
            target_directory = _stitch_paths(self.__base_dir, directory)
            if not os.path.exists(target_directory):
                os.makedirs(target_directory)
            return _stitch_paths('', directory)
        except Exception as e:
            logging.error(e)
            return None

    def get_access_path(self,
                        object_path: str
                        ) -> Union[None, str]:
        target_path = _stitch_paths(self.__base_dir, object_path)
        if self._exist(target_path):
            return target_path

    def size_of(self, object_path: str):
        target_path = _stitch_paths(self.__base_dir, object_path)
        if self._exist(target_path):
            return os.path.getsize(target_path)
        return None

    def upload_by_bytes(self, object_name: str, stream: bytes, directory: str = ''):
        base_directory = _stitch_paths(self.__base_dir, directory)
        target_file_path = _stitch_paths(base_directory, object_name)
        times = 0
        while True:
            try:
                with open(target_file_path, 'wb') as dst:
                    dst.write(stream)
                return True, target_file_path
            except Exception as e:
                logging.error(e)
                times += 1
                if times > 3:
                    return False, target_file_path
                continue

    def upload_by_file(self, file_name, object_name, directory: str = ''):
        base_directory = _stitch_paths(self.__base_dir, directory)
        target_file_path = _stitch_paths(base_directory, object_name)
        times = 0
        while True:
            try:
                with open(file_name, mode='rb') as src:
                    with open(target_file_path, 'wb') as dst:
                        dst.write(src.read())
                return True
            except Exception as e:
                logging.error(e)
                times += 1
                if times > 3:
                    return False
                continue


    def is_accessible(self, object_path: str):
        target_file_path = _stitch_paths(self.__base_dir, object_path)
        return os.path.exists(target_file_path)

    def _exist(self, complete_path: str) -> bool:
        return os.path.exists(complete_path)

    def exist(self, object_path: str):
        target_path = _stitch_paths(self.__base_dir, object_path)
        return self._exist(target_path)

    def delete(self, object_path: str):
        target_path = _stitch_paths(self.__base_dir, object_path)
        if os.path.isfile(target_path):
            os.remove(target_path)
        return not os.path.exists(target_path)


def _stitch_paths(base_dir: str, sub_dir: str = '') -> str:
    pre_dir = ''
    if base_dir.startswith('.'):
        # 如果是相对路径保存前缀
        pre_dir = base_dir[:base_dir.index('/')]
        base_dir = base_dir[base_dir.index('/'):]

    # 路径拼接之后重新构建路径
    src_path = (base_dir + '/' + sub_dir).replace('\\', '/')
    target_path = ''
    for i in src_path.split('/'):
        if i is not None and i != '':
            target_path = target_path + '/' + i

    target_path = pre_dir + target_path

    return target_path
