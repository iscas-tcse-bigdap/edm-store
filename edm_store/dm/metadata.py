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

class Metadata(object):
    """
    Converting the dict dictionary to an object.
    You can use 'CLASS_NAME.property_name' to get the element
    """
    def __init__(self, dict_doc=None):
        self._metadata_docs = dict_doc

    def __getattr__(self, name):
        val = self._metadata_docs.get(name)
        if isinstance(val, dict):
            return Metadata(val)
        return val

    def to_dict(self):
        return self._metadata_docs
