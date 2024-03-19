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

import json as _json

from osgeo import ogr as _ogr, osr, gdal
from typing import Union, Optional


class GeometryGenerator:
    """
    Generate ogr geometry
    """

    def __init__(self, geom: Union[dict, bytes, str], s_crs: Optional[str] = None):
        """
        Crate EDMGeometryGenerator by geom and crs

        :param geom: Describe Geometry :geojson, gml, ogr.Geometry
        :param s_crs: The crs of Geometry: Usually in proj, wkt, epsg code

        :raise Throw InPutException when can not parse geom to Geometry Object
        """
        if isinstance(geom, dict):
            geom = _json.dumps(geom, ensure_ascii=False)
        if isinstance(geom, (bytes,)):
            geom = geom.decode("utf8")
        if isinstance(geom, (str,)):
            if geom.find('{') >= 0 and geom.find("}") > 0:  # geojson
                self.geom = _ogr.CreateGeometryFromJson(geom)
                self.geojson = geom

            elif geom.find("<") >= 0 and geom.find(">") > 0:  # gml
                self.geom = _ogr.CreateGeometryFromGML(geom)

            elif geom.find("(") > 0 and geom.find(")") > 0:
                self.geom = _ogr.CreateGeometryFromWkt(geom)

            else:
                raise ValueError(f"Can't parse {geom}")
        self._crs = None
        if isinstance(s_crs, str):
            self._crs = osr.SpatialReference()
            self._crs.SetFromUserInput(s_crs)

            if str(gdal.__version__) >= '3.0.0':
                from osgeo.osr import OAMS_TRADITIONAL_GIS_ORDER

                self._crs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER)

    def transform(self, t_crs: str):
        """
        Transform  self.Geometry from s_crs to t_crs

        :param t_crs: target crs

        :return self
        """
        out_sr = osr.SpatialReference()
        out_sr.SetFromUserInput(t_crs)

        if str(gdal.__version__) >= '3.0.0':
            from osgeo.osr import OAMS_TRADITIONAL_GIS_ORDER

            out_sr.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER)

        if out_sr.IsSame(self._crs) == 0:
            tf = osr.CoordinateTransformation(self._crs, out_sr)
            _t = self.geom
            _s = self._crs
            self.geom.Transform(tf)
            self._crs = out_sr
            if self.geom is None:
                # 如果当前坐标系无法转换
                self.geom = _t
                self._crs = _s

        return self

    def export_to_ogr_geometry(self) -> _ogr.Geometry:
        """
        Parse and Export geometry to ogr.Geometry

        :return: object ogr.Geometry
        """
        return self.geom

    def export_to_geojson(self):
        return self.geom.ExportToJson()

    @property
    def crs(self):
        return self._crs

    def set_crs(self, t_crs: str):
        """
        Set crs from user input, Success only when self._crs is None

        :param t_crs: (CRS, str)

        :return: self
        """
        if self._crs is None:
            self._crs = osr.SpatialReference()
            self._crs.SetFromUserInput(t_crs)
            return self
        else:
            return self.transform(t_crs)


def gen_geobox(
        transform: Union[tuple, list],
        shape: Union[tuple, list],
        s_crs: Optional[str] = None,
):
    min_x, step_x, _, max_y, _, step_y = transform
    max_x = min_x + shape[1] * step_x
    min_y = max_y + shape[0] * step_y
    gg = GeometryGenerator({"type": "Polygon",
                            "coordinates": [
                                [[min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y],
                                 [min_x, max_y]]
                            ]
                            }, s_crs)
    return gg


def geobox_info(transform: Union[tuple, list], shape: Union[tuple, list],
                s_crs: Optional[str] = None, t_crs: Optional[str] = None) -> str:
    """
    生成一个矩形，并以Geojson格式输出，如果需要输出该矩形在其他坐标系的矩形则需要输入sCRS与tCRS

    :param transform [tuple, list] Gdal格式的矩阵 （x，scala_x，0,y,0, scala_y)
    :param shape [tuple, list] 矩阵的形状 （height， wight）
    :param s_crs str 源坐标系
    :param t_crs str 目标坐标系

    """
    min_x, step_x, _, max_y, _, step_y = transform
    max_x = min_x + shape[1] * step_x
    min_y = max_y + shape[0] * step_y
    gg = GeometryGenerator({"type": "Polygon",
                            "coordinates": [
                                [[min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y],
                                 [min_x, max_y]]
                            ]
                            }, s_crs)
    if not t_crs:
        return _json.loads(gg.export_to_geojson())
    if not s_crs:
        raise ValueError(f"sCrs is None!\nIf you want get geobox in Crs:{t_crs}, sCrs can't be None")
    return _json.loads(gg.transform(t_crs).export_to_geojson())


def is_same_crs(s_crs, t_crs):
    out_sr = osr.SpatialReference()
    out_sr.SetFromUserInput(t_crs)
    in_sr = osr.SpatialReference()
    in_sr.SetFromUserInput(s_crs)
    return out_sr.IsSame(in_sr) == 1


def calculate_tiles_info_by_input(transform, shape, base_size=2048 * 5):
    y_size, x_size = shape
    min_x, step_x, _, max_y, _, step_y = transform
    ix = 0
    res = []
    while x_size - base_size * ix > 0:
        _tx = x_size - base_size * ix
        if _tx > base_size:
            _tx = base_size
        iy = 0
        while y_size - base_size * iy > 0:
            _ty = y_size - base_size * iy
            if _ty > base_size:
                _ty = base_size
            res_shape = (_ty, _tx)
            res_transform = (min_x + step_x * base_size * ix, step_x, 0,
                             max_y + step_y * base_size * iy, 0, step_y)
            res.append([res_shape, res_transform])
            iy += 1
        ix += 1
    return res


