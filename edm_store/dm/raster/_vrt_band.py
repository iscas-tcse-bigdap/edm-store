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
用于保存临时Tif数据集，一般用于保存中间计算结果
"""
import pickle

from edm_store.dm.metadata import Metadata
from edm_store.dm.raster import Band


class VirtualBand(Band):
    """
    在基础的Band基础上创建一个band, 该band的作用类似于掩膜的作用，会根据传入的函数对基础band的数据处理
    """

    def __init__(self, metadata: Metadata, tile_size: int = None):
        super().__init__(metadata, tile_size)

        self.func = pickle.loads(self.metadata.func)
        self._params = self.metadata.func.params

        from edm_store.dataset import Dataset
        self._dst = Dataset(self.metadata.fa_dst)

        # the store client of dataset
        # self.client = StoreClientMapper.get(self.metadata.backend.type)

    def read_tile(self, x: int, y: int):
        transform, shape = self.gti.get_tile_info(x, y)
        data = self._dst.read_region(transform, shape[1], shape[0], self.get_projection_as_proj4())
        # 调用
        if self.func is not None:
            data = self.func(self, data, transform, shape[1], shape[0], self.get_projection_as_proj4())
        return data

    def read_region(self,
                    transform: [list, tuple],
                    xSize: int,
                    ySize: int,
                    project: str = None,
                    resample: str = None
                    ):
        array = self._dst.read_region(transform, xSize, ySize, project, resample)

        if self.func is not None:
            array = self.func(self, array, transform, xSize, ySize, project)

        return array

    def __del__(self):
        del self

    def close(self):
        self.__del__()


