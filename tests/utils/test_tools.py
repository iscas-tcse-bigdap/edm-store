import datetime

from edm_store.utils.tools import (
    append_zeros,
    gen_format_time,
    verify_and_rebuild_path,
    rebuilt_path,
    ResampleMapper,
    get_resample_method,
)
from edm_store import config
from edm_store.utils import constant


def test_append_zero():
    src_num = "20240308"
    target_num = append_zeros(src_num)
    assert target_num == "20240308000000"

    src_num = "20240308000000"
    target_num = append_zeros(src_num)
    assert target_num == "20240308000000"

    src_num = "20240308000"
    target_num = append_zeros(src_num)
    assert target_num == "20240308000000"


def test_format_time():
    src_time_str = "20240308"
    target_time, year = gen_format_time(src_time_str)

    assert target_time == 20240308
    assert year == 2024

    src_time_str = "2023-01-01 02:03"
    target_time, year = gen_format_time(src_time_str)

    assert target_time == 20230101
    assert year == 2023

    src_time_str = "2023/01/01 02:03"
    target_time, year = gen_format_time(src_time_str)

    assert target_time == 20230101
    assert year == 2023

    target_time, year = gen_format_time()
    assert target_time == int(datetime.datetime.now().strftime("%Y%m%d"))
    assert year == datetime.datetime.now().year


def test_verify_and_rebuild_path():
    config.create_allowed = ["test"]

    edm_store_path = "/edm_store/test/1.TIF"
    target = verify_and_rebuild_path(edm_store_path)
    assert target == "/edm_store/test/1.BAND"

    # Doesn't start with '/edm_store'
    edm_store_path = "/edm/test/1.TIF"
    try:
        verify_and_rebuild_path(edm_store_path)
    except ValueError as e:
        assert "Illegal path" in str(e)
    edm_store_path = "/edm_store/test/1.TIF"
    assert verify_and_rebuild_path(edm_store_path) == "/edm_store/test/1.BAND"

    # 'dataset' not in create_allowed
    edm_store_path = "/edm_store/dataset/1.tif"
    try:
        verify_and_rebuild_path(edm_store_path)
    except ValueError as e:
        assert "Illegal path" in str(e)

    config.create_allowed.append("dataset")
    assert verify_and_rebuild_path(edm_store_path) == "/edm_store/dataset/1.BAND"


def test_rebuild_path():
    src_path = "/edm_store\\test/1.TIF"
    assert rebuilt_path(src_path) == "/edm_store/test/1.TIF"


def test_get_resample_method():
    # use ResampleMapper
    assert ResampleMapper().get("nearest") == constant.nearest
    assert ResampleMapper().get("bilinear") == constant.bilinear
    assert ResampleMapper().get("cubic") == constant.cubic
    assert ResampleMapper().get("q1") == constant.q1
    assert ResampleMapper().get("q3") == constant.q3
    assert ResampleMapper().get("min") == constant.min
    assert ResampleMapper().get("max") == constant.max
    assert ResampleMapper().get("gauss") == constant.gauss
    assert ResampleMapper().get("average") == constant.average
    assert ResampleMapper().get("cubic_spline") == constant.cubic_spline

    # use function get_resample_method
    assert get_resample_method("lanczos") == constant.lanczos
    assert get_resample_method("mode") == constant.mode
