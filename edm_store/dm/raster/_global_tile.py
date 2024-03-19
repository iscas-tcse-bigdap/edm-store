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

from typing import Union, Tuple

import math

from edm_store.dm.vector.core import GeometryGenerator, is_same_crs


def _cal_tile_start_index(l_point: Union[int, float],
                          ori_point:  Union[int, float],
                          tile_size: Union[int, float],
                          scala: Union[int, float]) -> Union[int, float]:
    """
    Calculate the starting index of the current tile under the current sharding condition
    """
    if scala > 0:
        # 升序
        ds = (l_point - ori_point) / abs(float(tile_size * scala))
        tile_start = int(abs(ds))
        if ds < 0:
            tile_start = -1 * int(math.ceil(abs(ds)))
    else:
        # 降序
        ds = (l_point - ori_point) / abs(float(tile_size * scala))
        tile_start = -1 * int(math.ceil(ds))
    return tile_start


def _rounding(num, n=0):
    """
    A new rounding method allows you to set the rounding precision
    """
    if '.' in str(num):
        if len(str(num).split('.')[1]) > n and str(num).split('.')[1][n] == '5':
            num += 1 * 10 ** -(n + 1)
    if n:
        return round(num, n)
    else:
        return round(num)


def _normalized_read_and_fill_info(read_info, fill_info):
    """
    Since the rounding strategy may lead to inconsistencies in the size between the read window and the padded window,
    the following code will unify the size of the two in a way that considers reading more actual data.
    """
    x_size = int(read_info[1] - read_info[0] - fill_info[1] + fill_info[0])
    if x_size != 0:
        if x_size < 0:
            # 读取的范围小，填入的范围大, 扩大读取的范围
            read_info = (read_info[0], int(read_info[1] - x_size), read_info[2], read_info[3])
        if x_size > 0:
            fill_info = (fill_info[0], int(fill_info[1] + x_size), fill_info[2], fill_info[3])

    y_size = int(read_info[3] - read_info[2] - fill_info[3] + fill_info[2])
    if y_size != 0:
        if y_size < 0:
            read_info = (read_info[0], read_info[1], read_info[2], int(read_info[3] - y_size))
        else:
            fill_info = (fill_info[0], fill_info[1], fill_info[2], int(fill_info[3] + y_size))

    return read_info, fill_info


class GlobalTileInfo:
    """
    用于对数据进行分割成统一的网格大小
    """

    def __init__(self, transform: [list, tuple], xSize: int, ySize: int, tileSize: int = 2048,
                 hasPyramid: bool = True,
                 factors: [list, tuple] = None,
                 xFactors: [list, tuple] = None,
                 yFactors: [list, tuple] = None
                 ):
        # 依据输入的坐标系,计算出当前坐标系下 数据投影之后,坐标系的最大范围
        # 当前坐标系的原点
        self.oriX = 0
        self.oriY = 0

        # 设定进行切分的尺寸
        self.tileSize = tileSize

        # 获得左上角的坐标（lx，ly) 获得x，y方向上面的分辨率（scalaX，scalaY)
        self.lx, self.scalaX, _, self.ly, _, self.scalaY = transform

        # 当前数据的尺寸，如果当前数据没有进行切分，记录的是实际的尺寸，
        # 切分后记录的是进行切分后扩充的数据
        self.xSize = xSize
        self.ySize = ySize

        # 当前数据的右下角坐标
        self.rx = self.lx + self.xSize * self.scalaX
        self.ry = self.ly + self.ySize * self.scalaY

        # 计算原点与当前数据按照当前分辨率进行划分时候的误差
        self.deviationX = self.lx % self.scalaX  # x轴上的偏差
        self.deviationY = self.ly % abs(self.scalaY)  # y轴上的偏差

        # 将坐标原点偏移这些误差，保证所有的像素点都在确切的切分上
        self.oriX += self.deviationX
        self.oriY += self.deviationY

        # 以下两端代码为了是计算出全球切分之后，第一个Tile的x，y坐标
        self.tileXStart = _cal_tile_start_index(self.lx, self.oriX, tileSize, self.scalaX)
        self.tileYStart = _cal_tile_start_index(self.ly, self.oriY, tileSize, self.scalaY)

        self.firstTileLeftX = self.oriX + self.tileXStart * float(tileSize * self.scalaX)
        self.firstTileLeftY = self.oriY + self.tileYStart * float(tileSize * self.scalaY)

        # 计算出当前Tile范围
        self.rangeX = int(math.ceil((self.rx - self.firstTileLeftX) / float(tileSize * self.scalaX)))
        self.rangeY = int(math.ceil((self.ry - self.firstTileLeftY) / float(tileSize * self.scalaY)))

        # 获得横向和纵向的切分详情
        self.tileXEnd = self.rangeX - 1
        self.tileYEnd = self.rangeY - 1

        self.tileXStart, self.tileYStart = 0, 0

        self.reSizeXEnd = self.tileXEnd
        self.reSizeYEnd = self.tileYEnd
        self.reSize = self.tileSize

        # 保存当前数据的金字塔层级数据
        if factors is None:
            self._factors, self.scalaXList, self.scalaYList, self.tileSizeList = self.factors(hasPyramid)
        else:
            self._factors = factors
            self.scalaXList = xFactors
            self.scalaYList = yFactors
            self.tileSizeList = [int(tileSize / i) for i in factors]

    def factors(self, has=True):
        """计算出当前数据对象的金字塔的缩放系数,并返回相应的系数
        factors 当前的缩放系数
        scala_x x方向上的分辨率
        scala_y y方向上的分辨率
        tile_size 在不同层级下tile的尺寸
        """
        x, y = self.reSize * self.rangeX, self.reSize * self.rangeY
        base = max(x, y) / 2
        _factors = [1]
        scala_x = [self.scalaX]
        scala_y = [self.scalaY]
        tile_size = [self.tileSize]
        cur = 2
        if has:
            # 最小将当前数据缩小到2048
            while base > 256:
                _factors.append(cur)
                tile_size.append(int(self.tileSize / cur))
                scala_x.append(self.scalaX * cur)
                scala_y.append(self.scalaY * cur)
                cur *= 2
                base = base / 2
        return _factors, scala_x, scala_y, tile_size

    def resize(self, tileSize: int):
        """
        以当前的切分方式，再按照输入的tileSize重新切分，这种切分是逻辑上的切分，不会对是实际的物理存储的数据进行修改

        :param tileSize: int 重新切分的尺寸
        注意 可以输入的尺寸为 256，512，1024，2048
        """
        self.reSize = tileSize
        times = self.tileSize / self.reSize
        if times < 1:
            raise ValueError(f'The current dataset does not support reading at the current tile size: {tileSize}')
        self.reSizeXEnd = (self.tileXEnd + 1) * times - 1
        self.reSizeYEnd = (self.tileYEnd + 1) * times - 1
        self.rangeX = int(self.reSizeXEnd + 1)
        self.rangeY = int(self.reSizeYEnd + 1)

    def get_tile_offset(self):
        return self.rangeX, self.rangeY

    def writeable(self):
        return self.tileSize == self.reSize

    def get_grid_info(self, x: int = None, y: int = None) -> Tuple:
        if x is None and y is None:
            return (self._get_x(0), self.scalaX, 0, self._get_y(0), 0, self.scalaY), \
                (self.reSize * self.rangeX, self.reSize * self.rangeY)
        if x is None or y is None:
            if x is None: x = 0
            if y is None: y = 0
            return (self._get_x(x), self.scalaX, 0, self._get_y(y), 0, self.scalaY), \
                (self.reSize * (self.rangeX - x), self.reSize * (self.rangeY - y))
        return self.get_tile_info(x, y)

    def get_tile_info(self, x: int, y: int) -> tuple:
        # 消除误差并生成对应块的bound
        _nx = self.firstTileLeftX + x * self.reSize * self.scalaX
        _ny = self.firstTileLeftY + y * self.reSize * self.scalaY
        return (_nx, self.scalaX, 0, _ny, 0, self.scalaY), (self.reSize, self.reSize)

    def get_tiles(self) -> list:
        res = []
        for x in range(0, self.rangeX):
            for y in range(0, self.rangeY):
                res.append((x, y))
        return res

    def get_all_tile_infos(self) -> list:
        tiles = self.get_tiles()
        res = []
        for tile in tiles:
            transform, shape = self.get_tile_info(*tile)
            res.append((tile, transform, shape))
        return res

    def get_tile_index_and_offset(self, x, y) -> tuple:
        times = self.tileSize / self.reSize
        ox = int(x / times)
        oy = int(y / times)
        offsetX = int(x % times * self.reSize)
        offsetY = int(y % times * self.reSize)
        return (ox, oy), (offsetX, offsetY, self.reSize, self.reSize)

    def _get_x(self, x: int):
        """
        获得横向第x块的横坐标
        """
        return self.firstTileLeftX + x * self.scalaX * self.tileSize

    def _get_y(self, y: int):
        """
        获得纵向第y块的纵坐标
        """
        return self.firstTileLeftY + y * self.scalaY * self.tileSize

    def calculate_read_window_of_sliced_band(self, transform: Union[list, tuple],
                                             xSize: int,
                                             ySize: int,
                                             zoom: int = 0):
        """
        根据输入的transform和xSize,ySize在特定层级下计算需要读取的tile编号，窗口尺寸，数组填充大小
        """

        # 当前读取数据的范围 x从左向右增加  y从上向下递减 left x < right x   top y > bottom y
        read_lx = transform[0]  # left x
        read_ty = transform[3]  # top y
        read_rx = transform[0] + transform[1] * xSize  # right x
        read_by = transform[3] + transform[5] * ySize  # bottom y

        # 当前层级下的tile尺寸和分辨率
        cur_zoom_tile_size = self.tileSizeList[zoom]
        cur_zoom_scala_x = self.scalaXList[zoom]
        cur_zoom_scala_y = self.scalaYList[zoom]

        # 当前数据的范围
        ori_lx = self.firstTileLeftX
        ori_rx = ori_lx + self.reSize * self.rangeX * self.scalaX
        ori_ty = self.firstTileLeftY
        ori_by = ori_ty + self.reSize * self.rangeY * self.scalaY

        if not max(read_lx, ori_lx) < min(read_rx, ori_rx) and min(read_ty, ori_ty) > max(read_by, ori_by):
            # 如果读取范围和实际数据不存在交集返回空
            return None

        # 计算当前读取范围的起始tile的index(start_index_x, start_index_y) 以及终止tile的index(end_index_x, end_index_y)
        start_index_x = int((read_lx - ori_lx) / (cur_zoom_tile_size * cur_zoom_scala_x))
        start_index_y = int((read_ty - ori_ty) / (cur_zoom_tile_size * cur_zoom_scala_y))

        end_index_x = int(math.ceil((read_rx - ori_lx) / (cur_zoom_tile_size * cur_zoom_scala_x)) - 1)
        end_index_y = int(math.ceil((read_by - ori_ty) / (cur_zoom_tile_size * cur_zoom_scala_y)) - 1)

        # 激进的原则，将实际数据包裹进去

        # 计算当前读取范围在起始tile中x方向与y方向上的偏移(偏移多少开始读) 缩小
        start_offset_x = int(abs((read_lx - self._get_x(start_index_x)) / cur_zoom_scala_x))
        start_offset_y = int(abs((read_ty - self._get_y(start_index_y)) / cur_zoom_scala_y))

        # 计算当前读取范围在终止tile中x方向与y方向上的偏移(读取多大的偏移范围) 放大
        end_offset_x = math.ceil(abs((read_rx - self._get_x(end_index_x)) / cur_zoom_scala_x))
        end_offset_y = math.ceil(abs((read_by - self._get_y(end_index_y)) / cur_zoom_scala_y))

        actual_transform = [self._get_x(start_index_x) + start_offset_x * cur_zoom_scala_x,
                            cur_zoom_scala_x,
                            0,
                            self._get_y(start_index_y) + start_offset_y * cur_zoom_scala_y,
                            0,
                            cur_zoom_scala_y]

        actual_shape = [(end_index_y - start_index_y ) * cur_zoom_tile_size - start_offset_y + end_offset_y,
                        (end_index_x - start_index_x ) * cur_zoom_tile_size - start_offset_x + end_offset_x]

        # 用于保存需要读取哪些tile，每个tile中的窗口尺寸，填充到实际数据中的窗口尺寸
        windows_result_list = []

        for x in range(start_index_x, end_index_x + 1):
            # 初始化每块从0开始读，读取cur_zoom_tile_size长度即读取到 cur_zoom_tile_size - 1
            read_x0 = 0
            read_x1 = cur_zoom_tile_size - 1

            # 初始化每读取的块在填入到[fill_x0: fill_x1+1]中
            fill_x0 = 0 + (x - start_index_x) * cur_zoom_tile_size - start_offset_x
            fill_x1 = fill_x0 + cur_zoom_tile_size - 1

            if x == start_index_x:
                # 第一块
                read_x0 = start_offset_x
                fill_x0 = 0

            if x == end_index_x:
                # 最后一块
                read_x1 = end_offset_x - 1
                fill_x1 = fill_x0 + read_x1 - read_x0

            for y in range(start_index_y, end_index_y + 1):
                read_y0 = 0
                read_y1 = cur_zoom_tile_size - 1

                fill_y0 = 0 + (y - start_index_y) * cur_zoom_tile_size - start_offset_y
                fill_y1 = fill_y0 + cur_zoom_tile_size - 1

                if y == start_index_y:
                    read_y0 = start_offset_y
                    fill_y0 = 0

                if y == end_index_y:
                    read_y1 = end_offset_y - 1
                    fill_y1 = fill_y0 + read_y1 - read_y0

                read_info, fill_info = _normalized_read_and_fill_info(
                    (_rounding(read_x0), _rounding(read_x1), _rounding(read_y0), _rounding(read_y1)),
                    (_rounding(fill_x0), _rounding(fill_x1), _rounding(fill_y0), _rounding(fill_y1)))

                windows_result_list.append(
                    ((x, y), read_info, fill_info))

        return windows_result_list, actual_transform, actual_shape

    def rebuild_transform_to_target_crs(self, transform, shape, s_crs, t_crs) -> Tuple[Tuple, Tuple, bool, int]:
        """
        根据输入的transform和shape，计算出从当前坐标系下哪一个层级读取，并依据当前层级生成新的transform和shape
        """
        # 返回匹配的层级
        min_x, step_x, _, max_y, _, step_y = transform

        useless = step_x == self.scalaX and step_y == self.scalaY and is_same_crs(s_crs, t_crs)

        if useless:
            return transform, shape, False, 0

        # 传入数据的范围
        max_x = min_x + shape[1] * step_x
        min_y = max_y + shape[0] * step_y

        gg = GeometryGenerator({"type": "Polygon",
                                "coordinates": [
                                    [[min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y],
                                     [min_x, max_y]]
                                ]
                                }, s_crs).transform(t_crs)

        min_x, max_x, min_y, max_y = gg.export_to_ogr_geometry().GetEnvelope()

        # 转换之后的分辨率
        sx = (max_x - min_x) / shape[1]
        sy = (max_y - min_y) / shape[0]

        # 当前默认的金字塔层级默认为0
        zoom = 0

        for i, v in enumerate(self.scalaXList):
            if sx >= v:
                zoom = i
            else:
                break

        zoom = int(zoom)

        if max_y >= self.ly:
            # 说明y最高点在实际数据之上
            max_y = self.ly
        elif max_y < self.ly:
            # 说明y最高点在实际数据之下，则需要舍弃小数点
            max_y = int((max_y - self.ly) / self.scalaYList[zoom]) * self.scalaYList[zoom] + self.ly

        if min_x <= self.lx:
            # 说明x左端超过实际数据
            min_x = self.lx
        elif min_x > self.lx:
            min_x = int((min_x - self.lx) / self.scalaXList[zoom]) * self.scalaXList[zoom] + self.lx

        if max_x >= self.rx:
            max_x = self.rx
        else:
            max_x = math.ceil((max_x - self.lx) / self.scalaXList[zoom]) * self.scalaXList[zoom] + self.lx

        if min_y <= self.ry:
            min_y = self.ry
        else:
            min_y = math.ceil((min_y - self.ly)/ self.scalaYList[zoom]) * self.scalaYList[zoom] + self.ly

        # min_y = max(min_y, self.ry)
        # max_x = min(max_x, self.rx)

        h = _rounding(abs((min_y - max_y) / self.scalaYList[zoom]))
        w = _rounding(abs((max_x - min_x) / self.scalaXList[zoom]))

        return (min_x, self.scalaXList[zoom], 0, max_y, 0, self.scalaYList[zoom]), (h, w), True, zoom

    def calculate_read_window_of_unsliced_band(self, transform: Union[list, tuple],
                                               xSize: int,
                                               ySize: int,
                                               zoom: int = 0):
        """
        Calculate the window size of the uncut band you want to read.
        In addition to returning window, it also returns where it padded the array built inside which you can ignore
        if you don't need it.
        If the return is None, it means that the actual data is not read, which is generally used for testing.

        :param transform: list or tuple format in gdal version
        :param xSize: size of the x dimension
        :param ySize: size of the y dimension
        :param zoom: zoom level

        :return (start_x, start_x, offset_x, offset_y), (fill_x, fill_offset_x, fill_y, fill_offset_y)
        """
        regionLX = transform[0]
        regionLY = transform[3]
        regionRX = transform[0] + transform[1] * xSize
        regionRY = transform[3] + transform[5] * ySize
        _x0 = 0
        _y0 = 0
        _x1 = 0
        _y1 = 0
        _offsetX = 0
        _offsetY = 0
        _endX = 0
        _endY = 0

        # 当前层级下的tile尺寸和分辨率
        scalaX = self.scalaXList[zoom]
        scalaY = self.scalaYList[zoom]
        o_ySize = int(self.ySize / 2 ** zoom)
        o_xSize = int(self.xSize / 2 ** zoom)

        if regionLX >= self.rx or regionRX <= self.lx or regionLY <= self.ry or regionRY >= self.ly:
            return None, None

        disLX = (regionLX - self.lx) / scalaX
        if disLX < 0:
            _x0 = 0
            _offsetX = abs(disLX)
        else:
            _x0 = disLX
            _offsetX = 0

        disRX = (regionRX - self.rx) / scalaX
        if disRX >= 0:
            _x1 = o_xSize - 1
            _endX = xSize - 1 - disRX
        else:
            _x1 = o_xSize - 1 + disRX
            _endX = xSize - 1

        disLY = (regionLY - self.ly) / scalaY
        if disLY < 0:
            _y0 = 0
            _offsetY = abs(disLY)
        else:
            _y0 = disLY
            _offsetY = 0

        disRY = (regionRY - self.ry) / scalaY
        if disRY >= 0:
            _y1 = o_ySize - 1
            _endY = ySize - 1 - disRY
        else:
            _y1 = o_ySize + disRY - 1
            _endY = ySize - 1

        read_info, fill_info = _normalized_read_and_fill_info(
            (_rounding(_x0), _rounding(_x1), _rounding(_y0), _rounding(_y1)),
            (_rounding(_offsetX), _rounding(_endX), _rounding(_offsetY), _rounding(_endY)))

        return read_info, fill_info
