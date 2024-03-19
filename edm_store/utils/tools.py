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

import os
import re
import string

from datetime import datetime
import random
from typing import Tuple, Optional

from edm_store.config import get_config


# -------------------------------时间处理函数--------------------------------

# 用于生成统一的时间格式

def append_zeros(timeStr: str) -> str:
    timeStr = "".join(filter(str.isdigit, timeStr))
    if len(timeStr) < 6 or len(timeStr) > 14:
        raise TypeError("Unrecognized time format, correct formats are as follows:\n"
                        "\t'20000101','2000-01-01','2000/01/01','2000-01-01'\n"
                        "If you want save a specific time, the time should in the format like'%Y-%m-%dT%H:%M:%S'")
    # 不足14位时候补足至14位
    timeStr = timeStr + (14 - len(timeStr)) * '0'
    return timeStr


def format_time(timeStr: str) -> Tuple[int, int]:
    """
    将输入的timeStr提取时间数据，并以'%Y-%m-%dT%H:%M:%S'格式返回，这是一个简易的方法

    对于输入的timeStr需要保证以 年、月、日、时、分、秒 的顺序输入，其中必须包含年月日，

    并且年 四位数字， 月、日 各两位数字，可以以任意非数字字符连接，无法识别超过秒的部分

    :param timeStr str 时间序列

    :return '%Y-%m-%dT%H:%M:%S'格式下的时间
    """
    timeStr = append_zeros(timeStr)
    ret = re.match("(?P<Y>\d{4})(?P<m>\d{2})(?P<d>\d{2})(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})", timeStr)
    timeDict = ret.groupdict()
    _t = datetime(int(timeDict['Y']), int(timeDict['m']), int(timeDict['d']),
                  int(timeDict['H']), int(timeDict['M']), int(timeDict['S']))
    _now = datetime.now()
    if _t.timestamp() > _now.timestamp():
        raise ValueError(f"Value {_t} over current time: "
                         f"{_now.strftime('%Y-%m-%dT%H:%M:%S')}")
    return int(_t.strftime('%Y%m%d')), _t.year


def gen_format_time(timeStr: Optional[str] = None) -> Tuple[int, int]:
    """
    根据传入的timeStr生成一个 '%Y-%m-%dT%H:%M:%S' 格式的时间序列，如果输入为空则生成
    """
    if not timeStr:
        now = datetime.now()
        return int(now.strftime('%Y%m%d')), now.year
    else:
        return format_time(timeStr)


# -------------------------------路径处理函数--------------------------------

# 用于生成统一格式的路径
def verify_and_rebuild_path(path_str: str) -> str:
    path_str = rebuilt_path(path_str)

    if not path_str.startswith('/edm_store/'):
        raise ValueError(f"Illegal path {path_str}, all path should start with '/edm_store/'")

    ds = path_str.split('/')[2]

    if ds not in get_config().create_allowed:
        raise ValueError(f"Illegal path {path_str}\n",
                         "Path should start with '/edm_store/{dataset_num}/xxxx\n"
                         "Allows the creation of dataset_num will be like: " + ''.join(get_config().create_allowed))

    if '.' in path_str:
        ext = path_str[path_str.rindex('.') + 1:]
        pathWithoutExt = path_str[:path_str.rindex('.')]
        # 校验ext
        if ext.upper() not in ['BAND', 'IMAGE', 'TIF', 'TIFF']:
            raise ValueError(f"Error type {ext}.\n"
                             f"File type must be BAND|tif|tiff or IMAGE")
        path_str = pathWithoutExt + '.' + ext.upper() if ext.upper() in ['BAND', 'IMAGE'] else pathWithoutExt + '.BAND'
    else:
        raise ValueError(f"Illegal path {path_str}\nPath must end with '.BAND|tif|tiff' or '.IMAGE'")

    if len(re.findall('\.|\?|=| ', pathWithoutExt)) > 0:
        raise ValueError(f"Illegal char  '.|?| |=' in path: '{pathWithoutExt}'")

    return path_str


def rebuilt_path(pathStr: str, sep: Optional[str] = "/") -> str:
    pathStr = os.path.normpath(pathStr)
    if '\\' in pathStr:
        pathStr = pathStr.replace('\\', sep)
    if os.path.sep != sep:
        pathStr = pathStr.replace(os.path.sep, sep)
    return pathStr


def gen_random_name():
    name = random.sample(string.ascii_lowercase + string.ascii_uppercase, 8)
    return ''.join(name)


class ResampleMapper:
    resample_items = ['nearest', 'bilinear', 'cubic', 'cubic_spline', 'lanczos', 'average', 'mode',
                      'gauss', 'max', 'min', 'med', 'q1', 'q3', 'sum', 'rms']

    def get(self, item_name: str) -> int:
        if item_name.lower() in self.resample_items:
            return self.resample_items.index(item_name.lower())
        else:
            raise KeyError(f"The current resampling method named {item_name} does not exist")


def get_resample_method(resample_method: str = None) -> int:
    return ResampleMapper().get(resample_method) if resample_method is not None else ResampleMapper().get('nearest')
