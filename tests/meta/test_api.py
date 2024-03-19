from edm_store.dm.meta._api_impl import (
    create_band,
    create_image,
    exist,
    unlink,
    get_metadata,
    query_by_filter,
    load_dataset_from_file,
    get_image,
    get_band,
)

from edm_store import config

_transform = [12834619, 30, 0, 5011732, 0, -30]
_shape = [2000, 2000]
_proj = "EPSG:3857"
_band_name1 = "/edm_store/test/api_test.BAND"
_band_name2 = "/edm_store/test/api_test.tif"
_band_name3 = "/edm_store/test/api_test.tiff"

_image_name1 = "/edm_store/test/api_test.IMAGE"
_image_name2 = "/edm_store/test/api_test.image"


def setup_module():
    config.datasource_mapper_list["test"] = {"name": "default", "auth": "rw"}
    config.create_allowed.append("test")
    config.delete_allowed.append("test")
    config.storage_config["test_fs"] = {
        "BASE_DIRECTORY": "/opt/test/data/",
        "CONSTRUCT": "fs",
    }
    config.base_store_type = "test_fs"


def test_band_api():
    assert create_band(_proj, _shape, _transform, _band_name1, 0, "int8") == True

    assert exist(_band_name1) == True
    assert exist(_band_name2) == True
    assert exist(_band_name3) == True

    metadata = get_metadata(_band_name1)
    assert metadata is not None
    band_metadata = get_band(_band_name1)
    assert band_metadata is not None

    try:
        get_band(_band_name2)
    except ValueError as e:
        assert "No such band or image" in str(e)

    assert get_metadata(_band_name1) is not None

    assert unlink(_band_name1) == True
    assert exist(_band_name1) == False
    assert exist(_band_name2) == False
    assert exist(_band_name3) == False


def test_image_api():
    try:
        create_image(_proj, _shape, _transform, _image_name1, {})
    except ValueError as e:
        assert "Empty bands" in str(e)

    assert create_band(_proj, _shape, _transform, _band_name1, 0, "int8") == True
    bands = {"B1": _band_name1}

    assert create_image(_proj, _shape, _transform, _image_name1, bands) == True
    assert exist(_image_name1) == True
    assert exist(_image_name2) == True

    metadata = get_metadata(_image_name1)
    assert metadata is not None
    image_metadata = get_image(_image_name1)
    assert image_metadata is not None
    try:
        get_image(_image_name2)
    except ValueError as e:
        assert "No such band or image" in str(e)

    assert get_metadata(_image_name2) is not None

    assert unlink(_image_name1) == True
    assert exist(_image_name1) == False
    assert exist(_image_name2) == False

    # will delete band
    assert exist(_band_name1) == False
