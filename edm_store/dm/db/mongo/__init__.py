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

from edm_store.dm.db.mongo._api_impl import MetadataResourceImpl
from edm_store.dm.db.mongo._api_interface import MetadataResource


def get_metadata_resource_instance(mongoConfig: dict) -> MetadataResource:
    """
    Get an instance of :class:`MetadataResource`.

    The schema of `mongoConfig` :class:`dict` are as follows:

    >>> class _MongoConfig:
    ...     host: str
    ...     port: int
    ...     database: str = "edm_store"
    ...     tzAware: bool = True
    ...     connect: bool = True
    ...     maxPoolSize: int = 16
    ...     username: str = None
    ...     password: str = None

    :param mongoConfig: a :class:`dict` for configurations of mongodb
    :return: an instance of :class:`MetadataResource`
    """
    return MetadataResourceImpl(mongoConfig)
