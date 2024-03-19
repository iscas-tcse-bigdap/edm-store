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

from edm_store.storage import AbsBackendClient


class DummyClient(AbsBackendClient):
    def __init__(self, client):
        self.client = client

    def mk_dirs(self, directory: str):
        return self.client.mk_dirs(directory)

    def get_access_path(self,
                        object_path: str):
        # 读取数据时返回None
        return None

    def size_of(self,
               object_path: str
               ):
        return self.client.size_of(object_path)

    def upload_by_bytes(self, object_name: str, stream: bytes, directory: str = ''):
        return self.client.upload_by_bytes(object_name, stream, directory)

    def upload_by_file(self, file_name, object_name, directory: str = ''):
        return self.client.upload_by_file(file_name, object_name, directory)

    def is_accessible(self, object_path: str):
        return self.client.is_accessible(object_path)

    def exist(self, object_path: str):
        return self.client.exist(object_path)

    def delete(self, object_path: str):
        return self.client.delete(object_path)

    # def _path(self, file_name: str, sub: str = None):
    #     return self.client._path(sub, file_name)
    #
    # def read(self, x: int, y: int, sub: str = None):
    #     # 读取数据时返回None
    #     return None
