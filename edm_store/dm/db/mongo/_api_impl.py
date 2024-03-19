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

import json
from abc import ABC, abstractmethod
from typing import List, Union, Dict, Optional, Literal

from pymongo import MongoClient

from ._mongo_ops import MongoResource, MongoTransaction
from ._validation import Image, Band

# ----------------------- Pydantic Validation Models ------------------------ #

_VALIDATE_MODEL_IMAGE, _VALIDATE_MODEL_BAND = Image, Band


# --------------------------------------------------------------------------- #
# --------------------------- Metadata Operations  -------------------------- #
# --------------------------------------------------------------------------- #


class MetadataOpsMixin(ABC):
    """
    A mix-in trait for common operations that a metadata-resource should have.
    (mixin this to get interface `MetadataOpsInterface` implemented automatically)

    Properties that provided for this trait:
        - def underlying(self) -> MongoResource: ...
    """

    # ------------------------- Abstract Properties  ------------------------ #

    @property
    @abstractmethod
    def underlying(self) -> MongoResource:
        """The underlying `MongoResource` instance."""
        ...

    # ----------------------- Default Implementations  ---------------------- #

    def addImage(self, image_metadata: dict, dataset: str = "default") -> bool:
        collection_name = self.__makeCollectionName(dataset, "image")
        return self.underlying.insert(
            collection_name, image_metadata, _VALIDATE_MODEL_IMAGE
        )

    def updateImage(
        self, image_metadata: dict, dataset: str = "default", upsert: bool = False
    ) -> bool:
        collection_name = self.__makeCollectionName(dataset, "image")
        query = {"image_path": image_metadata["image_path"]}
        return self.underlying.update(
            collection_name,
            query,
            {"$set": image_metadata},
            _VALIDATE_MODEL_IMAGE,
            upsert,
        )

    def deleteImage(
        self, image_path: Union[str, List[str]], dataset: str = "default"
    ) -> bool:
        collection_name = self.__makeCollectionName(dataset, "image")
        if type(image_path) is str:
            return self.underlying.delete(collection_name, {"image_path": image_path})
        elif type(image_path) is List[str]:
            return self.underlying.delete(
                collection_name, {"image_path": {"$in": image_path}}
            )
        else:
            msg = "argument 'image_path' in deleteImage() must be a `str` or `list`, but {} received."
            raise TypeError(msg.format(type(image_path)))

    def findImage(
        self,
        filter_json: Union[str, dict],
        return_props: List[str],
        dataset: str = "default",
        limit: int = 2000,
    ) -> Dict[str, Union[int, List[dict]]]:
        collection_name = self.__makeCollectionName(dataset, "image")
        if type(filter_json) is str:
            loaded_json = json.loads(filter_json)
            retrieved = self.underlying.find(
                collection_name, loaded_json, limit, return_props
            )
            retrieved = list(retrieved)
            return {"Count": len(retrieved), "Images": retrieved}
        elif type(filter_json) is dict:
            retrieved = self.underlying.find(
                collection_name, filter_json, limit, return_props
            )
            retrieved = list(retrieved)
            return {"Count": len(retrieved), "Images": retrieved}
        else:
            msg = "argument 'filter_json' in findImage() must be a `str` or `list`, but {} received."
            raise TypeError(msg.format(type(filter_json)))

    def getImage(self, image_path: str, dataset: str = "default") -> Optional[dict]:
        collection_name = self.__makeCollectionName(dataset, "image")
        retrieved = self.underlying.find(collection_name, {"image_path": image_path})
        retrieved = list(retrieved)
        return None if len(retrieved) == 0 else retrieved[0]

    def addBand(self, band_metadata: dict, dataset: str = "default") -> bool:
        collection_name = self.__makeCollectionName(dataset, "band")
        return self.underlying.insert(
            collection_name, band_metadata, _VALIDATE_MODEL_BAND
        )

    def updateBand(
        self, band_metadata: dict, dataset: str = "default", upsert: bool = False
    ) -> bool:
        collection_name = self.__makeCollectionName(dataset, "band")
        query = {"band_path": band_metadata["band_path"]}
        return self.underlying.update(
            collection_name,
            query,
            {"$set": band_metadata},
            _VALIDATE_MODEL_BAND,
            upsert,
        )

    def deleteBand(
        self, band_path: Union[str, List[str]], dataset: str = "default"
    ) -> bool:
        collection_name = self.__makeCollectionName(dataset, "band")
        if type(band_path) is str:
            return self.underlying.delete(collection_name, {"band_path": band_path})
        elif type(band_path) is List[str]:
            return self.underlying.delete(
                collection_name, {"band_path": {"$in": band_path}}
            )
        else:
            msg = "argument 'image_path' in deleteImage() must be a `str` or `list`, but {} received."
            raise TypeError(msg.format(type(band_path)))

    def getBand(self, band_path: str, dataset: str = "default") -> Optional[dict]:
        collection_name = self.__makeCollectionName(dataset, "band")
        retrieved = self.underlying.find(collection_name, {"band_path": band_path})
        retrieved = list(retrieved)
        return None if len(retrieved) == 0 else retrieved[0]

    # -------------------------- Private Functions -------------------------- #

    @staticmethod
    def __makeCollectionName(dataset: str, collection: Literal["image", "band"]) -> str:
        return f"{collection}_{dataset}"


# --------------------------------------------------------------------------- #
# ----------------------- Metadata API Implementations  --------------------- #
# --------------------------------------------------------------------------- #


class MetadataTransactionImpl(MetadataOpsMixin):
    __slots__ = "__trans"

    def __init__(self, client: MongoClient, db_name: str) -> None:
        self.__trans = MongoTransaction(client, db_name)

    def __enter__(self):
        self.__trans.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> bool:
        # delegate to underlying transaction to handle resource releasing.
        return self.__trans.__exit__(exc_type, exc_value, exc_traceback)

    # ------------------ Properties for `MetadataOpsMixin`  ----------------- #

    @property
    def underlying(self) -> MongoTransaction:
        return self.__trans


class MetadataResourceImpl(MetadataOpsMixin):
    __slots__ = "__mongoResource"

    def __init__(self, mongo_config: dict) -> None:
        self.__mongoResource = MongoResource(mongo_config)

    def __del__(self) -> None:
        self.close()

    # ------------------ Properties for `MetadataOpsMixin`  ----------------- #

    @property
    def underlying(self) -> MongoResource:
        return self.__mongoResource

    # ---------------------------- Other Methods ---------------------------- #

    def transaction(self) -> MetadataTransactionImpl:
        """
        开启一个事务, 返回的`MetadataTransaction` 实现了 Context-manager protocol,
        需要使用`with`语句包裹事务逻辑。

        Example:
            >>> resource: MetadataResourceImpl = MetadataResourceImpl({"host": "localhost", "port": 27017})
            >>> with resource.transaction() as t:
            ...     # do something with `t`
            ...     # transaction will submit if no exception raised
            ...     # transaction will abort if exception raised

        :return: an instance of :class:`MetadataTransaction`
        """
        client = self.__mongoResource.client
        db_name = self.__mongoResource.database.name
        return MetadataTransactionImpl(client, db_name)

    def close(self) -> None:
        """
        释放底层维护的所有资源。
        """
        self.__mongoResource.close()
