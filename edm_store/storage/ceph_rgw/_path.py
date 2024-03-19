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

import pathlib
import base64

from typing import overload
from typing_extensions import Self

_DEFAULT_ENCODING = "utf-8"


class Path:
    __slots__ = "_processed_path"

    @overload
    def __init__(self, object_name: str, dirs: str):
        ...

    @overload
    def __init__(self, object_path: str):
        ...

    def __init__(self, *args):
        if len(args) == 1:
            self._processed_path = args[0][1:] if args[0].startswith("/") else args[0]
        if len(args) == 2:
            self._processed_path = _make_path(object_name=args[0], dirs=args[1])
        if self._processed_path.startswith('/'):
            self._processed_path = self._processed_path[1:]

    def __str__(self):
        return self._processed_path

    def to_base_64(self) -> str:
        try:
            encoded_bytes: bytes = base64.b64encode(_str_2_byte(self.__str__()))
        except Exception as e:
            raise Exception("Failed to encode path with base64 algorithm") from e
        else:
            return _byte_2_str(encoded_bytes)

    @classmethod
    def from_base_64(cls, base_64: str) -> Self:
        try:
            decoded_bytes: bytes = base64.b64decode(_str_2_byte(base_64))
        except Exception as e:
            raise Exception("Failed to decode path from base64 codec") from e
        else:
            return cls(_byte_2_str(decoded_bytes))


def _make_path(object_name: str, dirs: str) -> str:
    preprocessed_dirs = _preprocess_dirs(dirs)
    if preprocessed_dirs == "": return object_name
    return f"{preprocessed_dirs}/{object_name}"


def _preprocess_dirs(dirs: str) -> str:
    if dirs == "": return ""
    tmp_dirs = str(pathlib.Path(dirs))
    return tmp_dirs[1:] if tmp_dirs.startswith("/") else f"{tmp_dirs}"


def _byte_2_str(b: bytes) -> str:
    return str(b, encoding=_DEFAULT_ENCODING)


def _str_2_byte(s: str) -> bytes:
    return bytes(s, encoding=_DEFAULT_ENCODING)

# if __name__ == '__main__':
#     path = Path("test.txt", "/parent/sub")
#     print(path)
#     print(path.to_base_64())
#     print(Path.from_base_64(path.to_base_64()))
#     print(Path("/parent/sub/test.txt"))
#     print(Path("parent/sub/test.txt"))
#     print(Path("test.txt", "parent/sub"))
