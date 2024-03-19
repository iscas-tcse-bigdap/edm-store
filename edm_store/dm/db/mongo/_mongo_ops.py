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

from abc import ABC, abstractmethod
from typing import Type, Iterable, Optional, List

import pymongo
from pydantic import BaseModel
from pymongo import MongoClient, database, client_session


# --------------------------------------------------------------------------- #
# ------------------------ Configuration Preprocess ------------------------- #
# --------------------------------------------------------------------------- #


class _MongoConfig(BaseModel):
    """
    Pydantic model for mongodb configuration.
    """

    host: str
    port: int
    database: str = "edm_store"
    tzAware: bool = True
    connect: bool = True
    maxPoolSize: int = 16


# --------------------------------------------------------------------------- #
# --------------------------- MongoDB Operations  --------------------------- #
# --------------------------------------------------------------------------- #


class MongoOpsMixin(ABC):
    """
    A mix-in trait for common operations that a mongo-resource should have.

    Note:
        Properties that should be provided for mixin this trait:

        - def client(self) -> MongoClient: ...
        - def database(self) -> database.Database: ...
        - def session(self) -> client_session.ClientSession
    """

    # ------------------------- Abstract Properties  ------------------------ #

    @property
    @abstractmethod
    def client(self) -> MongoClient:
        """The underlying `pymongo.MongoClient` instance."""
        ...

    @property
    @abstractmethod
    def database(self) -> database.Database:
        """The underlying `pymongo.database.Database` instance."""
        ...

    @property
    @abstractmethod
    def session(self) -> Optional[client_session.ClientSession]:
        """The underlying `pymongo.client_session.ClientSession` instance."""
        ...

    # ----------------------- Default Implementations  ---------------------- #

    def insert(
            self,
            collection_name: str,
            document: dict,
            validation: Optional[Type[BaseModel]] = None,
    ) -> bool:
        """
        Insert a document into the collection.

        :param collection_name: the name of collection to operate on
        :param document: the document to insert
        :param validation: pydantic model for insert validation, `None` for bypassing
        :return: true when insert operation success, otherwise return false
        """
        document = self.__validateDocument(document, validation)

        if not self.is_collection_exists(collection_name):
            self.creat_index(collection_name)

        collection = self.database[collection_name]
        return collection.insert_one(document, session=self.session).acknowledged

    def is_collection_exists(self, collection_name: str) -> bool:
        if collection_name in self.database.list_collection_names():
            return True
        else:
            return False

    def creat_index(self, collection_name: str):
        collection = self.database[collection_name]
        if "band_" in collection_name:
            # add indexes to the band table
            collection.create_index("band_path")
            collection.create_index([("extent", "2dsphere")])
        if "image_" in collection_name:
            # add indexes to the image table
            collection.create_index("image_path")
            collection.create_index([("wgs_boundary", "2dsphere")])
            collection.create_index([("date", pymongo.DESCENDING)])
            collection.create_index([("year", pymongo.DESCENDING)])


    def update(
        self,
        collection_name: str,
        query_filter: dict,
        document: dict,
        validation: Optional[Type[BaseModel]] = None,
        upsert: bool = False,
    ) -> bool:
        """
        Update documents that satisfies the 'query_filter'.

        :param collection_name: the name of collection to operate on
        :param query_filter: the query filter for selecting documents to update
        :param document: the target document for update
        :param validation: pydantic model for insert validation, `None` for bypassing
        :param upsert: whether to insert if no documents match the query filter
        :return: true when update operation success, otherwise return false
        """
        document = self.__validateDocument(document["$set"], validation)
        collection = self.database[collection_name]
        return collection.update_one(
            query_filter, {"$set": document}, upsert, session=self.session
        ).acknowledged

    def delete(self, collection_name: str, query_filter: dict) -> bool:
        """
        Delete documents that satisfies the 'query_filter'.

        :param collection_name: the name of collection to operate on
        :param query_filter: the query filter for selecting documents to delete
        :return: true when delete operation success, otherwise return false
        """
        collection = self.database[collection_name]
        return collection.delete_many(query_filter, session=self.session).acknowledged

    def find(
        self,
        collection_name: str,
        query_filter: dict,
        limit: int = 0,
        projection: Optional[List[str]] = None,
    ) -> Iterable[dict]:
        """
        Find multiple documents that satisfies the 'query_filter'.

        :param collection_name: the name of collection to operate on
        :param query_filter: the query filter for selecting documents
        :param limit: the maximum number of results to return (0 means no limit)
        :param projection: a group of fields to retain in the query result, `None` for retaining all.
        :return: a `Iterable` of `dict` as selected documents
        """
        collection = self.database[collection_name]
        projection = (
            None
            if projection is None
            else ["_id" if x == "objectId" else x for x in projection]
        )

        result_iterable = collection.find(
            filter=query_filter,
            projection=projection,
            limit=limit,
            session=self.session,
        )

        return [self.__replaceIdKeyName(x) for x in result_iterable]

    # -------------------------- Private Functions -------------------------- #

    @staticmethod
    def __validateDocument(
        raw_document: dict, model: Optional[Type[BaseModel]]
    ) -> dict:
        if model is None:
            return raw_document
        return model.model_validate(raw_document).model_dump()

    @staticmethod
    def __replaceIdKeyName(document: dict) -> dict:
        if "objectId" in document.keys():
            document["objectId"] = document.pop("_id")
        return document


# --------------------------------------------------------------------------- #
# ------------------------------ MongoDB Index  ----------------------------- #
# --------------------------------------------------------------------------- #


# TODO: finish this and mix-in to compose the functionality


class MongoIndexMixin(ABC):
    pass  # TODO: not implemented


# --------------------------------------------------------------------------- #
# --------------------------- MongoDB API Classes  -------------------------- #
# --------------------------------------------------------------------------- #


class MongoTransaction(MongoOpsMixin):
    """
    An abstraction for transaction in `MongoResource`.

    This class implements the `ContextManager` protocol. (use it in `with` statement)

    DO NOT instantiate this class directly, you should call `MongoResource.startTransaction()`
    to get an instance of this class, and then use it with `with` block.

    Example:

        >>> resource = MongoResource({"host": "localhost", "port": 27017}) # this is just an example
        >>> with resource.transaction() as t:
        ...     # do whatever you need with `t`, it has implemented almost all methods in `MongoResource`
        ...     # e.g. call `t.insert()`
    """

    __slots__ = "__client", "__session", "__database", "__dbName"

    def __init__(self, client: MongoClient, db_name: str) -> None:
        self.__client = client
        self.__dbName = db_name

    def __enter__(self):
        # create logic session and start transaction on it
        self.__session = self.__client.start_session()
        self.__database = self.__session.client[self.__dbName]
        self.__session.start_transaction()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> bool:

        # if no exception raised, commit the transaction
        if exc_type is None:
            self.__session.commit_transaction()
            self.__session.end_session()
            return True

        # if exception raised, abort transaction and re-raise
        else:
            self.__session.abort_transaction()
            self.__session.end_session()
            return False

    # -------------------- Properties for `MongoOpsMixin`  ------------------ #

    @property
    def client(self) -> MongoClient:
        return self.__client

    @property
    def database(self) -> database.Database:
        return self.__database

    @property
    def session(self) -> Optional[client_session.ClientSession]:
        return self.__session


class MongoResource(MongoOpsMixin):
    """
    A group of operations for mongodb manipulation.

    The instance of this class maintains a `pymongo.MongoClient` and encapsulates operations based on it.
    (You can use the `client` property to get underlying resource if extra control are required)

    Use the CRUD methods directly when transaction is not considered. if you need to run transaction,
    call `transaction()` and do transaction in the `with` block like following example:

    >>> resource = MongoResource({"host": "localhost", "port": 27017}) # this is just an example
    >>> with resource.transaction() as t:
    ...     # do whatever you need with `t`, it has implemented almost all methods in `MongoResource`
    ...     # e.g. call `t.insert()`
    """

    __slots__ = "__client", "__database"

    def __init__(self, config: dict) -> None:
        """
        Get an instance of `MongoResource`.

        :param config: the `MongoClient` configuration dict
        :return: a `MongoResource` instance
        """
        c = _MongoConfig.model_validate(config)
        self.__client = MongoClient(
            c.host, c.port, dict, c.tzAware, c.connect, maxPoolSize=c.maxPoolSize
        )
        self.__database = self.__client[c.database]

    # -------------------- Properties for `MongoOpsMixin`  ------------------ #

    @property
    def client(self) -> MongoClient:
        return self.__client

    @property
    def database(self) -> database.Database:
        return self.__database

    @property
    def session(self) -> Optional[client_session.ClientSession]:
        return None

    # ---------------------------- Other Methods ---------------------------- #

    def transaction(self) -> MongoTransaction:
        """
        Get an instance of `MongoTransaction`.

        `MongoTransaction` has implemented the `Context-Manager` protocol.
        (Use `with` statement to ensure the underlying resource are released in safe)

        :return: the `MongoTransaction` instance
        """
        return MongoTransaction(self.__client, self.database.name)

    def close(self) -> None:
        """Close the underlying `pymongo.MongoClient` and release resources."""
        self.__client.close()
