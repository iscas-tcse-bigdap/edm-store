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

import logging

import boto.s3.connection
import requests
from typing import Tuple, Union

from ._path import Path
from .awsauth import S3Auth
from .. import AbsBackendClient


class CephRGWClient(AbsBackendClient):
    __slots__ = "_session", "_url_prefix", "_auth", "_conn", "_bucket"

    # noinspection HttpUrlsUsage
    def __init__(self, config: dict):
        self._session = requests.session()

        self._url_prefix = "http://{}:{}/".format(config['host'], config['port'])
        self._auth = S3Auth(config["access_key"], config["secret_key"],
                            str(config['host']) + ':' + str(config['port']))

        self._conn = boto.connect_s3(
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            host=config['host'],
            port=config['port'],
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
        self._bucket = config['bucket']
        self._url_prefix = self._url_prefix + self._bucket

    def __del__(self):
        self._session.close()

    def size_of(self,
                object_path: str
                ) -> Union[int, None]:
        return None

    def mk_dirs(self, directory: str) -> str:
        return str(Path("", directory))

    def upload_by_bytes(self, object_name: str, stream: bytes, directory: str = '') -> Tuple[bool, str]:
        path = Path(object_name, directory)
        try:
            encodedPath: str = str(path)
            self._session.put(url=f"{self._url_prefix}/{encodedPath}", auth=self._auth, data=stream)
        except Exception:
            return False, str(path)
        else:
            return True, str(path)

    def upload_by_file(self, file_name, object_name, directory: str = ''):
        path = Path(object_name, '')
        try:
            encodedPath = str(path)
            response = self._session.post(url=f"{self._url_prefix}/{encodedPath}?uploads", auth=self._auth,
                                          headers={'Accept': 'application/json'})
            upload_id = response.json()['UploadId']
            # 上传分片
            part_number = 1
            part_tags = []
            with open(file_name, 'rb') as file:
                while True:
                    data = file.read(5 * 1024 * 1024)  # 假设分片大小为5MB
                    if not data:
                        break
                    response = self._session.put(
                        url=f"{self._url_prefix}/{encodedPath}?partNumber={part_number}&uploadId={upload_id}",
                        data=data,
                        auth=self._auth)
                    part_tags.append({'PartNumber': part_number, 'ETag': response.headers['ETag']})
                    part_number += 1

            # 完成上传
            parts = [f"<Part><PartNumber>{i['PartNumber']}</PartNumber><ETag>{i['ETag']}</ETag></Part>" for i in part_tags]
            payload = '<CompleteMultipartUpload>' + ''.join(parts) + '</CompleteMultipartUpload>'
            headers = {'Content-Type': 'application/json'}
            response = self._session.post(url=f"{self._url_prefix}/{encodedPath}?uploadId={upload_id}", headers=headers,
                                          data=payload, auth=self._auth)
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            return False

    def is_accessible(self, object_path: str) -> bool:
        path = Path(object_path)
        try:
            encodedPath: str = str(path)
            resp = self._session.head(url=f"{self._url_prefix}/{encodedPath}", auth=self._auth)
            # 200 => object found
            if resp.status_code == 200:
                return True
            else:
                return False
        except Exception:
            return False

    def get_access_path(self,
                        object_path: str
                        ) -> Union[None, str]:

        path = Path(object_path)
        try:
            encodedPath: str = str(path)
            _bucket = self._conn.get_bucket(self._bucket)
            pathKey = _bucket.get_key(encodedPath)
            pathKey.set_canned_acl('public-read')
            url = pathKey.generate_url(3600, query_auth=False,
                                       force_http=False)
        except Exception as e:
            return None
        else:
            return str(url)

    def exist(self, object_path: str) -> bool:
        return self.is_accessible(object_path)

    def delete(self, object_path: str) -> bool:
        path = Path(object_path)
        try:
            encodedPath: str = str(path)
            self._session.delete(url=f"{self._url_prefix}/{encodedPath}", auth=self._auth)
        except Exception:
            logging.error(f"failed to delete object in '{path}'.")
            return False
        else:
            return True
