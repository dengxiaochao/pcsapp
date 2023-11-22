import datetime
from io import BytesIO
import logging
import os
import re
import hashlib
from typing import Dict, List

import requests

from apiauth import apiauth
from config.config import SyncType
from third_party.pcssdk.openapi_client import api_client, exceptions
from third_party.pcssdk.openapi_client.api import fileupload_api, fileinfo_api, multimediafile_api

BLOCKSIZE=4*104*1024

class NameBytesIO(BytesIO):
    def __init__(self, file: str, block: bytes):
        super().__init__(block)
        self.name = file


class SyncException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Sync(object):
    def __init__(self, local: str, remote: str, auth: apiauth.Auth, type=SyncType.DOWNLOAD, excludes: List[str] = [], blocksize=BLOCKSIZE):
        self.local = local
        self.remote = remote
        self.type = type
        self.auth = auth
        self.api_client = api_client.ApiClient()
        self.blocksize = blocksize
        self.excludes = []
        for exclude in excludes:
            logging.info(f"exclude {exclude}")
            self.excludes.append(re.compile(exclude))

    def excluded(self, file: str) -> bool:
        for exclude in self.excludes:
            if exclude.match(file):
                return True
        return False

    def sync(self):
        if self.type == SyncType.UPLOAD or self.type == SyncType.UPDOWNLOAD:
            self.sync_up_dir(self.local, self.remote)
        if self.type == SyncType.DOWNLOAD or self.type == SyncType.UPDOWNLOAD:
            self.sync_down_dir(self.remote, self.local)

    def sync_up_dir(self, src_dir: str, dst_dir: str):
        if self.excluded(src_dir):
            logging.info(f"skip excluded dir {src_dir}")
            return
        remote_files = self.remote_ls(dst_dir)
        logging.info(f"remote_ls on {dst_dir} got: {remote_files}")
        for file in os.listdir(src_dir):
            if file.startswith('.'):
                continue
            src = os.path.join(src_dir, file)
            dst = os.path.join(dst_dir, file)
            if os.path.isdir(src):
                if file not in remote_files:
                    self.remote_mkdir(dst)
                self.sync_up_dir(src, dst)
            elif file not in remote_files or os.path.getmtime(src) > remote_files[file].get('server_mtime', 0):
                self.sync_up_file(src, dst)
        pass

    def sync_up_file(self, src: str, dst: str):
        if self.excluded(src):
            logging.info(f"skip excluded file {src}")
            return
        logging.info(f'uploading file {src}')
        self.upload_file(self.auth.get_access_token(), src, dst)
        logging.info(f'sync file {src} success')

    def sync_down_dir(self, src_dir: str, dst_dir: str):
        remote_files = self.remote_ls(src_dir)
        logging.info(f"remote_ls on {src_dir} got: {remote_files}")
        for remote_file in remote_files:
            ts_now = datetime.datetime.now().timestamp()
            if self.excluded(remote_file):
                logging.info(f"skip excluded file {remote_file}")
                continue
            local_file = os.path.join(dst_dir, remote_file)
            if remote_files[remote_file].get('isdir', 0) == 1:
                if os.path.isfile(local_file):
                    logging.info(
                        f"rename {local_file} to {local_file}.{ts_now}")
                    os.rename(local_file, f'{local_file}.{ts_now}')
                if not os.path.exists(local_file):
                    os.mkdir(local_file)
                self.sync_down_dir(os.path.join(
                    src_dir, remote_file), local_file)
                continue
            if os.path.exists(local_file) and not os.path.isfile(local_file):
                logging.info(
                    f"rename {local_file} to {local_file}.{ts_now}")
                os.rename(local_file, f'{local_file}.{ts_now}')
            if os.path.exists(local_file) and os.path.getmtime(local_file) > remote_files[remote_file].get('server_mtime', 0):
                logging.info(
                    f"skip download {remote_file} as local file is newer")
                continue
            self.download_file(self.auth.get_access_token(
            ), remote_files[remote_file].get('fs_id', 0), local_file)

    def remote_ls(self, path: str) -> Dict[str, Dict]:
        ret = {}
        api_instance = fileinfo_api.FileinfoApi(self.api_client)
        try:
            api_response = api_instance.xpanfilelist(
                self.auth.get_access_token(), dir=path, showempty=1)
            if api_response.get('errno', -1) != 0:
                raise SyncException(
                    f"xpanfilelist {path} error: {api_response}")
            for file in api_response.get('list', []):
                ret[file.get('server_filename')] = file
        except exceptions.ApiException as e:
            raise SyncException(
                f"remote_ls {path} raised pcssdk Exception: {e}")
        return ret

    def remote_mkdir(self, dst: str):
        api_instance = fileupload_api.FileuploadApi(self.api_client)
        try:
            api_response = api_instance.xpanfilecreate(
                self.auth.get_access_token(), dst, 1, 0, None, [])
            if api_response.get('errno', -1) != 0:
                raise SyncException(f"xpanmkdir {dst} error: {api_response}")
        except exceptions.ApiException as e:
            raise SyncException(f"mkdir {dst} raised pcssdk Exception: {e}")

    def upload_file(self, token: str, src: str, dst: str):
        api_instance = fileupload_api.FileuploadApi(self.api_client)
        stat = os.stat(src)
        size = stat.st_size
        block_list = self.md5_blocks(src)
        logging.info(f'start sync file {src}, size {size}')
        try:
            block_list_str = '[' + \
                ','.join('"' + b + '"' for b in block_list) + ']'
            api_response = api_instance.xpanfileprecreate(
                token, dst, 0, size, 1, block_list_str, rtype=3)
            if api_response.get('errno', -1) != 0:
                raise SyncException(
                    f"xpanfileprecreate {src} error: {api_response}")
            uploadid = api_response.get('uploadid')

            with open(src, 'rb') as f:
                for i in range(len(block_list)):
                    block = f.read(self.blocksize)
                    if not block:
                        break
                    api_response = api_instance.pcssuperfile2(token, str(
                        i), dst, uploadid, "tmpfile", file=NameBytesIO(src, block))
                    if api_response.get('md5', "") != block_list[i]:
                        raise SyncException(
                            f"pcssuperfile2 {src} with wrong md5: {api_response}, expect {block_list[i]}")
                    logging.info(
                        f"sync {src} upload block {i}/{len(block_list)} done")
            api_response = api_instance.xpanfilecreate(
                token, dst, 0, size, uploadid, block_list_str, rtype=3)
        except exceptions.ApiException as e:
            raise SyncException(
                f"upload file {src} raised pcssdk Exception: {e}")
        except Exception as e:
            raise SyncException(f"Exception when upload file {src}: {e}")

    def download_file(self, token: str, fsid: int, dst: str):
        api_instance = multimediafile_api.MultimediafileApi(self.api_client)
        fsidstr = f'[{fsid}]'
        try:
            api_response = api_instance.xpanmultimediafilemetas(
                token, fsidstr, dlink="1")
            if api_response.get('errno', -1) != 0 or not api_response.get('list', []):
                raise SyncException(
                    f"xpanmultimediafileget {fsid} error: {api_response}")
            dlink = api_response.get('list', [])[0].get('dlink')
            if not dlink:
                raise SyncException(
                    f"xpanmultimediafileget {fsid} error: {api_response}")
            logging.info(f"download {fsid} to {dst}")
            req = requests.get(f'{dlink}&access_token={token}', headers={
                               "User-Agent": "pan.baidu.com"}, stream=True)
            req.raise_for_status()
            tmp = dst + '.tmp'
            with open(tmp, 'wb') as f:
                for chunk in req.iter_content(chunk_size=BLOCKSIZE):
                    if chunk:
                        f.write(chunk)
            os.rename(tmp, dst)
            req.close()
        except exceptions.ApiException as e:
            raise SyncException(
                f"download_file file {fsid} raised pcssdk Exception: {e}")
        except Exception as e:
            raise SyncException(f"Exception when download file {fsid}: {e}")

    def md5(self, data: bytes) -> str:
        m = hashlib.md5()
        m.update(data)
        return m.hexdigest()

    def md5_blocks(self, src: str) -> List[str]:
        with open(src, 'rb') as f:
            blocks = []
            while True:
                block = f.read(self.blocksize)
                if not block:
                    break
                blocks.append(self.md5(block))
            return blocks
