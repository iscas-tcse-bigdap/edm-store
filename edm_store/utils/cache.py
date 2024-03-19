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

import atexit
import sys
import time
from collections import OrderedDict
from threading import RLock
from typing import Hashable, Union, Any, Dict, Optional, KeysView, ValuesView, ItemsView


def _get_timestamp(expire: Optional[Union[int, float]] = None) -> int:
    stampNow = int(time.time())

    if expire is not None:
        stampNow += expire

    return stampNow


class Cache(object):
    """
    内存缓存
    """
    _CACHE: OrderedDict
    _EXPIRE_DICT: Dict[Hashable, Union[int, float]]

    # 可重入写保证线程安全， 每次操作上述操作时候加锁
    _LOCK: RLock

    def __init__(self,
                 maxsize: int = 1024 * 1024 * 5,
                 defaultExpire: int = 10,
                 defaultEmptyResult: Any = None):

        self.maxsize = None
        self.defaultExpire = None
        self.freeSize = None
        self.defaultEmptyResult = None

        self.configure(maxsize, defaultExpire, defaultEmptyResult)

        self._CACHE: OrderedDict = OrderedDict()

        self._EXPIRE_DICT = {}
        self._LOCK = RLock()

    def __len__(self):
        with self._LOCK:
            return self.maxsize - self.freeSize

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({list(self.copy().items())})"

    def cacheable(self, dataSize: Union[int, float]):
        return dataSize <= self.maxsize

    def copy(self) -> OrderedDict:
        self._delete_expired()
        with self._LOCK:
            return self._CACHE.copy()

    def clear(self) -> None:
        with self._LOCK:
            self._clear()

    def _clear(self) -> None:
        self._CACHE.clear()
        self._EXPIRE_DICT.clear()
        self.freeSize = self.maxsize

    def full(self) -> bool:
        if self.maxsize is None or self.maxsize <= 0:
            return False
        return self.freeSize <= 0

    def keys(self) -> KeysView:
        return self.copy().keys()

    def values(self) -> ValuesView:
        return self.copy().values()

    def items(self) -> ItemsView:
        return self.copy().items()

    def configure(self,
                  maxsize: Optional[int] = None,
                  defaultExpire: Optional[int] = None,
                  defaultEmptyResult: Any = None):

        if maxsize is not None:
            if not isinstance(maxsize, int):
                raise TypeError("maxsize must be an integer")

            if maxsize <= 0:
                raise ValueError("maxsize must be greater than zero")

            if self.freeSize is None:
                self.freeSize = maxsize
            else:
                self.freeSize += (maxsize - self.maxsize)

            self.maxsize = maxsize

        if defaultExpire is not None:
            if not isinstance(defaultExpire, (int, float)):
                raise TypeError("defaultExpire must be an integer or float")

            if defaultExpire < 0:
                raise ValueError("defaultExpire must be greater than  or equal to zero")

            self.defaultExpire = defaultExpire

        if defaultEmptyResult is not None:
            self.defaultEmptyResult = defaultEmptyResult

    def delete(self, key: Hashable) -> int:
        with self._LOCK:
            return self._delete(key)

    def _delete(self, key: Hashable) -> int:
        tag = 0
        try:
            value_size = sys.getsizeof(self._CACHE[key])
            del self._CACHE[key]
            self.freeSize = self.freeSize + value_size
            tag = 1
        except KeyError:
            pass

        try:
            del self._EXPIRE_DICT[key]
        except KeyError:
            pass

        return tag

    def delete_expired(self):
        with self._LOCK:
            self._delete_expired()

    def _delete_expired(self):
        if self._EXPIRE_DICT is None or len(self._EXPIRE_DICT.keys()) == 0:
            return 0

        count = 0
        expireOn = int(time.time())
        expire_cp = self._EXPIRE_DICT.copy()

        for key, expiration in expire_cp.items():
            if expiration <= expireOn:
                count += self._delete(key)

        return count

    def _evict(self, sizeNeed: int) -> int:
        count = 0
        if sizeNeed > self.freeSize:
            # 空间不够
            sizeLack = int(self.freeSize - sizeNeed)
            with self._LOCK:
                while sizeLack < 0:
                    try:
                        _, value = self._CACHE.popitem(last=False)
                        self.freeSize += sys.getsizeof(value)
                        sizeLack += sys.getsizeof(value)
                        count += 1
                    except KeyError:
                        break
        return count

    def has(self, key: Hashable):
        with self._LOCK:
            return self._has(key)

    def _has(self, key: Hashable):

        self.delete_expired()

        return key in self.keys()

    def set(self, key: Hashable, value: Any, expire: Optional[Union[int, float]] = None) -> None:
        with self._LOCK:
            self._set(key, value, expire)

    def _set(self, key: Hashable, value: Any, expire: Optional[Union[int, float]] = None) -> None:
        # 设置过期时间
        if expire is None:
            expire = self.defaultExpire

        self.delete_expired()

        value_size = sys.getsizeof(value)

        if value_size > self.maxsize:
            raise ValueError(f"value is too large, value should least than {self.maxsize}")

        if key in self.keys():
            self._delete(key)

        # 如果key没有缓存，首先释放空间
        self._evict(value_size)

        self._CACHE[key] = value

        self.freeSize -= value_size

        self._EXPIRE_DICT[key] = _get_timestamp(expire)

    def is_expired(self, key: Hashable) -> bool:
        expiredOn = _get_timestamp()

        if key in self._EXPIRE_DICT.keys():
            return self._EXPIRE_DICT[key] <= expiredOn

        return False

    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._LOCK:
            return self._get(key, default)

    def _get(self, key: Hashable, default: Any = None) -> Any:

        try:
            # 获得key对应的数据如果不存在则为None
            value = self._CACHE[key]

            if self.is_expired(key):
                self._delete(key)
                raise KeyError

        except KeyError:

            if default is None:
                default = self.defaultEmptyResult

            value = default

        return value


global_cache = Cache(1024*1024*1024*1)


def _clear_cache():
    global_cache.clear()


atexit.register(_clear_cache)
