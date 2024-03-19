from edm_store.dm.raster import GlobalTileInfo
from edm_store.dm.vector.core import gen_geobox

_transform = [12834619, 30, 0, 5011732, 0, -30]
_shape = [2000, 2000]
_proj = "EPSG:3857"


def test_factors():
    tile_size = 2000
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)

    # 生成缩放因子与每个层级对应的分辨率和tile_size
    factors, sx, sy, tile_size_list = gti.factors()

    assert factors == [1, 2, 4, 8]
    assert sx == [_transform[1] * i for i in factors]
    assert sy == [_transform[-1] * i for i in factors]
    assert tile_size_list == [tile_size / i for i in factors]


def test_resize():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)
    ori_x_size = gti.rangeX
    ori_y_size = gti.rangeY

    # Get tile info (transform, shape)
    ori_tile_info = gti.get_tile_info(0, 0)

    # Shape in tile equals [2048, 2048]
    assert list(ori_tile_info[1]) == [tile_size, tile_size]

    new_tile_size = 1024
    gti.resize(new_tile_size)
    new_x_size = gti.rangeX
    new_y_size = gti.rangeY

    # Get tile info (transform, shape)
    new_tile_info = gti.get_tile_info(0, 0)

    # Shape in every tile will be resized to [1024, 1024]
    assert list(new_tile_info[1]) == [new_tile_size, new_tile_size]

    # 2048/1024 = 2 range in x and range in y will be enlarged twice
    assert ori_y_size * 2 == new_y_size
    assert ori_x_size * 2 == new_x_size

    # New tile_size should be less than ori tile_size
    new_tile_size = 2049
    try:
        gti.resize(new_tile_size)
    except ValueError as e:
        assert "not support" in str(e)


def test_writeable():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)
    assert gti.writeable() == True

    new_tile_size = 1024
    gti.resize(new_tile_size)
    assert gti.writeable() == False

    gti.resize(tile_size)
    assert gti.writeable() == True


def test_tile_info():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)

    # Get the new transform and shape after globally tiled.
    new_transform, new_shape = gti.get_grid_info()
    tile_0_0_info = gti.get_tile_info(0, 0)

    # The ranges represented by the new transform and the new shape contain the original data ranges.
    assert new_transform[0] <= _transform[0]
    assert new_transform[2] >= _transform[2]
    assert (
        new_transform[0] + new_shape[1] * new_transform[1]
        >= _transform[0] + _shape[1] * _transform[1]
    )
    assert (
        new_transform[2] + new_shape[0] * new_transform[-1]
        <= _transform[2] + _shape[0] * _transform[-1]
    )

    # Use the similar transform and shape will be tilled in the same when the tile_size does not change.
    new_gti_1 = GlobalTileInfo(new_transform, new_shape[0], new_shape[1], tile_size)
    t1, s1 = new_gti_1.get_grid_info()
    new_t1_0_0_info = new_gti_1.get_tile_info(0, 0)

    assert list(t1) == list(new_transform)
    assert list(tile_0_0_info[0]) == list(new_t1_0_0_info[0])
    assert list(tile_0_0_info[1]) == list(new_t1_0_0_info[1])

    # Translate 20 pixel to the right of the x-axis and Translate 20 pixel downwards on the y-axis.
    transform = list(new_transform)
    transform[0] = new_transform[0] + new_transform[1] * 20
    transform[2] = new_transform[2] + new_transform[-1] * 20

    new_gti_2 = GlobalTileInfo(transform, new_shape[0], new_shape[1], tile_size)
    t2, s2 = new_gti_2.get_grid_info()
    new_t2_0_0_info = new_gti_2.get_tile_info(0, 0)

    assert list(t2) == list(new_transform)
    assert list(tile_0_0_info[0]) == list(new_t2_0_0_info[0])
    assert list(tile_0_0_info[1]) == list(new_t2_0_0_info[1])

    # Translate 20 pixel to the left of the x-axis
    transform = list(new_transform)
    transform[0] = new_transform[0] - new_transform[1] * 20
    new_gti_3 = GlobalTileInfo(transform, new_shape[0], new_shape[1], tile_size)
    new_t3_1_0_info = new_gti_3.get_tile_info(1, 0)

    assert list(tile_0_0_info[0]) == list(new_t3_1_0_info[0])
    assert list(tile_0_0_info[1]) == list(new_t3_1_0_info[1])

    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)
    assert len(gti.get_tiles()) == gti.rangeY * gti.rangeX

    for tile_info in gti.get_all_tile_infos():
        tile, transform, shape = tile_info
        t1, s1 = gti.get_tile_info(*tile)

        assert list(transform) == list(t1)
        assert list(shape) == list(s1)


def test_get_index_and_tile_size():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)
    # get (0,0) tile info
    tile, windows = gti.get_tile_index_and_offset(0, 0)
    assert list(tile) == [0, 0]
    assert list(windows) == [0, 0, tile_size, tile_size]

    # After resize, tile((0,0),(1,0),(0,1),(1,1)) belong to the original (0,0) tile
    new_tile_size = 1024
    gti.resize(new_tile_size)
    tile, windows = gti.get_tile_index_and_offset(0, 0)
    assert list(tile) == [0, 0]
    assert list(windows) == [0, 0, new_tile_size, new_tile_size]

    tile, windows = gti.get_tile_index_and_offset(0, 1)
    assert list(tile) == [0, 0]
    assert list(windows) == [0, new_tile_size, new_tile_size, new_tile_size]

    tile, windows = gti.get_tile_index_and_offset(1, 0)
    assert list(tile) == [0, 0]
    assert list(windows) == [new_tile_size, 0, new_tile_size, new_tile_size]

    tile, windows = gti.get_tile_index_and_offset(1, 1)
    assert list(tile) == [0, 0]
    assert list(windows) == [new_tile_size, new_tile_size, new_tile_size, new_tile_size]


def test_calculate_read_window_of_sliced_band():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, _shape[0], _shape[1], tile_size)
    transform, shape = gti.get_tile_info(0, 0)

    window_infos = gti.calculate_read_window_of_sliced_band(transform, 50, 50)
    # Does not intersect with actual data
    assert window_infos is None

    window_infos = gti.calculate_read_window_of_sliced_band(transform, 2048, 2048)
    assert len(window_infos) == 1
    assert list(window_infos[0][0]) == [0, 0]
    assert list(window_infos[0][1]) == [0, 2047, 0, 2047]
    assert list(window_infos[0][2]) == [0, 2047, 0, 2047]

    transform, shape = gti.get_tile_info(1, 1)
    window_infos = gti.calculate_read_window_of_sliced_band(transform, 80, 50)
    assert len(window_infos) == 1
    assert list(window_infos[0][0]) == [1, 1]
    assert list(window_infos[0][1]) == [0, 49, 0, 79]
    assert list(window_infos[0][2]) == [0, 49, 0, 79]


def test_calculate_read_window_of_unsliced_band():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, 9000, 9000, tile_size)

    transform, shape = gti.get_tile_info(0, 0)
    read_info, fill_info = gti.calculate_read_window_of_unsliced_band(
        transform, 2048, 2048
    )
    offset_x = abs((transform[0] - _transform[0]) / (_transform[1]))
    offset_y = abs((transform[3] - _transform[3]) / (_transform[-1]))
    assert list(read_info) == [0, int(2047 - offset_x), 0, int(2047 - offset_y)]
    assert list(fill_info) == [int(offset_x), 2047, int(offset_y), 2047]

    transform, shape = gti.get_tile_info(1, 1)
    read_info, fill_info = gti.calculate_read_window_of_unsliced_band(
        transform, 2048, 2048
    )
    assert list(read_info) == [
        int(2047 - offset_x) + 1,
        int(2047 - offset_x) + 1 + 2047,
        int(2047 - offset_y) + 1,
        int(2047 - offset_y) + 1 + 2047,
    ]
    assert list(fill_info) == [0, 2047, 0, 2047]


def test_rebuild_transform_to_target_crs():
    tile_size = 2048
    gti = GlobalTileInfo(_transform, 9000, 9000, tile_size)

    transform, shape = gti.get_tile_info(0, 0)
    new_transform, new_shape, need_project, zoom = gti.rebuild_transform_to_target_crs(
        transform, shape, "epsg:3857", "epsg:3857"
    )
    assert list(transform) == list(new_transform)
    assert list(shape) == list(new_shape)
    assert need_project == False
    assert zoom == 0

    new_transform, new_shape, need_project, zoom = gti.rebuild_transform_to_target_crs(
        [transform[0], transform[1] * 3, 0, transform[2], 0, transform[-1] * 3],
        shape,
        "epsg:3857",
        "epsg:3857",
    )
    assert new_transform[1] == transform[1] * (2**1)
    assert need_project == True
    assert zoom == 1

    geo = gen_geobox(
        [transform[0], transform[1] * 3, 0, transform[2], 0, transform[-1] * 3],
        shape,
        s_crs="EPSG:3857",
    )
    geo = geo.transform("EPSG:4326")
    min_x, max_x, min_y, max_y = geo.export_to_ogr_geometry().GetEnvelope()

    another_transform = [
        min_x,
        (max_x - min_x) / 2048,
        0,
        max_y,
        0,
        (max_y - min_y) / 2048,
    ]
    another_shape = [2048, 2048]
    new_transform, new_shape, need_project, zoom = gti.rebuild_transform_to_target_crs(
        another_transform, another_shape, "EPSG:4326", "EPSG:3857"
    )
    assert new_transform[1] == transform[1] * (2**1)
    assert need_project == True
    assert zoom == 1
