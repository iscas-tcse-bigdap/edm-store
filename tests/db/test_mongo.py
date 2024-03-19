"""
Some random test-cases for "edm_store.dm.db._Mongo"
"""

from typing import List

import pytest
from typing_extensions import Final

# noinspection PyProtectedMember
from edm_store.dm.db.mongo._mongo_ops import MongoResource

# -------------------------------- Settings --------------------------------- #

TEST_HOST: Final[str] = "60.245.208.96"
TEST_PORT: Final[int] = 27018
COLLECTION: Final[str] = "test-collection"
MONGO_RESOURCE: Final[MongoResource] = MongoResource(
    {"host": TEST_HOST, "port": TEST_PORT}
)

INITIAL_DOCUMENTS: Final[List[dict]] = [
    {"name": "alice", "gender": 1, "scores": [1, 2, 3, 4, 5, 55]},
    {"name": "bob", "gender": 0, "scores": [19, 55, 36, 80, 59, 100]},
    {"name": "chris", "gender": 0, "scores": [0, 0, 0], "extra": "all-zero"},
]


# -------------------------- Setup & Teardown Stuff ------------------------- #


def setup_module():  # initial data inserted HERE
    for doc in INITIAL_DOCUMENTS:
        MONGO_RESOURCE.insert(COLLECTION, doc)


def teardown_module():  # cleanup and drop collection HERE
    MONGO_RESOURCE.database.drop_collection(COLLECTION)
    MONGO_RESOURCE.close()


# ------------------------------- Test Process ------------------------------ #


@pytest.mark.dependency(depends=[])
def test_step0_find():
    res = list(MONGO_RESOURCE.find(COLLECTION, {"gender": {"$in": [0]}}))
    for x in res:
        x.pop("_id", None)

    assert len(res) == 2
    assert {"name": "bob", "gender": 0, "scores": [19, 55, 36, 80, 59, 100]} in res
    assert {
        "name": "chris",
        "gender": 0,
        "scores": [0, 0, 0],
        "extra": "all-zero",
    } in res


@pytest.mark.dependency(depends=["test_step0_find"])
def test_step1_update():
    MONGO_RESOURCE.update(COLLECTION, {"name": "bob"}, {"$set": {"extra2": "yeah"}})

    res = list(MONGO_RESOURCE.find(COLLECTION, {}))
    for x in res:
        x.pop("_id", None)

    assert len(res) == 3
    assert {
        "name": "bob",
        "gender": 0,
        "scores": [19, 55, 36, 80, 59, 100],
        "extra2": "yeah",
    } in res


@pytest.mark.dependency(depends=["test_step1_update"])
def test_step2_insert():
    MONGO_RESOURCE.insert(COLLECTION, {"name": "leon"})

    res = list(MONGO_RESOURCE.find(COLLECTION, {}))
    for x in res:
        x.pop("_id", None)

    assert {"name": "leon"} in res


@pytest.mark.dependency(depends=["test_step2_insert"])
def test_step3_find():
    res = list(MONGO_RESOURCE.find(COLLECTION, {"name": "leon"}))
    for x in res:
        x.pop("_id", None)

    assert len(res) == 1
    assert res[0] == {"name": "leon"}


@pytest.mark.dependency(depends=["test_step3_find"])
def test_step4_delete():
    del_res = MONGO_RESOURCE.delete(COLLECTION, {"gender": 0})

    assert del_res == True

    res = list(MONGO_RESOURCE.find(COLLECTION, {}))
    for x in res:
        x.pop("_id", None)

    assert len(res) == 2
