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

from edm_store.config import get_config
from edm_store.storage.AbsClient import AbsBackendClient
from edm_store.storage.fs import FSClient
from edm_store.storage.obs import ObsClient
from edm_store.storage.ceph_rgw import CephRGWClient


# ================== Add items for new type of client HERE ==================== #

_CLIENT_CONSTRUCTORS = {
    "obs": ObsClient,
    "ceph_rgw": CephRGWClient,
    "s3": CephRGWClient,
    "fs": FSClient
    # Add items for new type of client HERE
    # ...
}

# ============================================================================= #


class ClientGenerator:
    CACHE_STORE = {}

    def storeClient(self, storeType: str) -> AbsBackendClient:
        client = self.CACHE_STORE.get(storeType)
        if client is None:
            _CLIENT_CONSTRUCTORS_CONFIG_ITEMS = get_config().storage_config
            constructor = _CLIENT_CONSTRUCTORS[_CLIENT_CONSTRUCTORS_CONFIG_ITEMS[storeType]["type"]]
            parameters = _CLIENT_CONSTRUCTORS_CONFIG_ITEMS[storeType]['configure_params']
            self.CACHE_STORE[storeType] = constructor(parameters)
        return self.CACHE_STORE[storeType]

    def __getattr__(self, storeType: str):
        return self.storeClient(storeType)

    def __getitem__(self, item):
        return self.storeClient(item)

    def __setattr__(self, storeType: str, client: AbsBackendClient):
        self.CACHE_STORE[storeType] = client

    def get(self, storeType: str) -> AbsBackendClient:
        return self.storeClient(storeType)

    def set(self, storeType: str, client: AbsBackendClient):
        self.CACHE_STORE[storeType] = client


storage_client_mapper = ClientGenerator()

__all__ = ["storage_client_mapper"]
