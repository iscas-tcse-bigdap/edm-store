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
import os
import yaml
from typing import List, Union

from edm_store.utils.cache import global_cache

ENVIRONMENT_VARNAME = 'EDM_STORE_CONFIG_PATH'

DEFAULT_CONF_PATHS = tuple(p for p in ['/etc/edm_store_config.json',
                                       '/etc/edm_store_config.yaml',
                                       os.environ.get(ENVIRONMENT_VARNAME, ''),
                                       str(os.path.expanduser("~/.edm_store_config.json")),
                                       str(os.path.expanduser("~/.edm_store_config.yaml")),
                                       'edm_store_config.yaml',
                                       'edm_store_config.json'] if len(p) > 0)


def _load_config(config_path: str):
    if config_path is not None:
        config_path_list = [config_path] + list(DEFAULT_CONF_PATHS)
    else:
        config_path_list = list(DEFAULT_CONF_PATHS)

    for config_path in config_path_list:
        if os.path.exists(config_path):
            with open(config_path, 'r') as file:
                ctx = file.read()
            if config_path.endswith('.json'):
                return json.loads(ctx)
            elif config_path.endswith('.yaml'):
                return yaml.load(ctx, yaml.FullLoader)
            else:
                raise TypeError("Invalid config file format")
    raise FileNotFoundError("No config file, please specify a valid config file by using function "
                            "`edm_store.init_config({config_path})`.\n"
                            "Although you can configure it in `/etc/edm_store_config.json`, or configure the "
                            "environment variable named `EDM_STORE_CONFIG_PATH`")


class LocalConfig:

    def __init__(self, config: Union[str, dict] = None):
        if isinstance(config, str):
            config = _load_config(config)
        if not isinstance(config, dict):
            raise TypeError("Config is not a dictionary or a dict containing config values")
        try:
            self.storage_config = config['storage_client_config']
            if len(self.storage_config.keys()) > 0:
                self.base_store_type = list(self.storage_config.keys())[0]
            else:
                raise ValueError('At least one storage client in config file is required')

            metadata_config = config['metadata_config']
            self.db_config = metadata_config['db_config']
            self.datasource_config = metadata_config['datasource_config']

            if len(self.datasource_config.keys()) > 0:
                self.base_datasource = list(self.datasource_config.keys())[0]
            else:
                raise ValueError('At least one datasource in config file is required')

            self.max_cache_size = 1024 ** 3
            if 'cache_config' in metadata_config.keys() and 'max_cache_size' in metadata_config['cache_config']:
                self.max_cache_size = int(metadata_config['cache_config']['max_cache_size'])

            self.create_allowed = []
            self.delete_allowed = []
            self.datasource_mapper = {}

            for key in self.datasource_config.keys():
                datasource = self.datasource_config[key]

                if 'create' in datasource['authority']:
                    self.create_allowed.append(datasource['alias'])
                if 'delete' in datasource['authority']:
                    self.delete_allowed.append(datasource['alias'])

                self.datasource_mapper[datasource['alias']] = key

        except KeyError as e:
            raise KeyError("Invalid config file, please check the config file") from e
        except Exception as e:
            raise e

    @property
    def datasource(self) -> List[str]:
        return list(self.datasource_config.keys())


def auto_config():
    globals()['global_edm_store_config'] = LocalConfig()


def init_config(config: Union[str, dict]):
    config = LocalConfig(config)
    globals()['global_edm_store_config'] = config
    global_cache.clear()
    global_cache.configure(config.max_cache_size)


def get_config() -> LocalConfig:
    if globals().get('global_edm_store_config') is None:
        globals()['global_edm_store_config'] = LocalConfig()
        return globals()['global_edm_store_config']
    else:
        return globals()['global_edm_store_config']
