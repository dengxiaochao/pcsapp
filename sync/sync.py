from io import BytesIO
import logging
import os
import hashlib
from typing import Dict, List

from apiauth import apiauth
from third_party.pcssdk.openapi_client import api_client, exceptions
from third_party.pcssdk.openapi_client.api import fileupload_api, fileinfo_api

class NameBytesIO(BytesIO):
    def __init__(self, file:str, block: bytes):
        super().__init__(block)
        self.name = file

class SyncException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class Sync(object):
    def __init__(self, src: str, dst: str, auth: apiauth.Auth, blocksize=4*1024*1024):
        self.src = src
        self.dst = dst
        self.auth = auth
        self.api_client = api_client.ApiClient()
        self.blocksize = blocksize

    def sync(self):
        self.sync_dir(self.src, self.dst)

    def sync_dir(self, src_dir: str, dst_dir: str):
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
                self.sync_dir(src, dst)
            elif file not in remote_files or os.path.getmtime(src) > remote_files[file].get('server_mtime', 0):
                self.sync_file(src, dst)
                logging.info(f'sync file {src} success')
        pass

    def sync_file(self, src: str, dst: str):
        self.upload_file(self.auth.get_access_token(), src, dst)

    def remote_ls(self, path: str) -> Dict[str, Dict]:
        ret = {}
        api_instance = fileinfo_api.FileinfoApi(self.api_client)
        try:
            api_response = api_instance.xpanfilelist(self.auth.get_access_token(), dir=path, showempty=1)
            if api_response.get('errno', -1) != 0:
                raise SyncException(f"xpanfilelist {path} error: {api_response}")
            for file in api_response.get('list', []):
                ret[file.get('server_filename')] = file
        except exceptions.ApiException as e:
            raise SyncException(f"remote_ls {path} raised pcssdk Exception: {e}")
        return ret

    def remote_mkdir(self, dst: str):
        api_instance = fileupload_api.FileuploadApi(self.api_client)
        try:
            api_response = api_instance.xpanfilecreate(self.auth.get_access_token(), dst, 1, 0, None, [])
            if api_response.get('errno', -1) != 0:
                raise SyncException(f"xpanmkdir {dst} error: {api_response}")
        except exceptions.ApiException as e:
            raise SyncException(f"mkdir {dst} raised pcssdk Exception: {e}")


    def upload_file(self, token: str, src: str, dst: str):
        api_instance = fileupload_api.FileuploadApi(self.api_client)
        stat = os.stat(src)
        size = stat.st_size
        block_list = self.md5_blocks(src)
        print(f'block_list is {block_list}')
        try:
            block_list_str = '[' + ','.join('"' + b + '"' for b in block_list) + ']'
            api_response = api_instance.xpanfileprecreate(
                token, dst, 0, size, 1, block_list_str, rtype=3)
            if api_response.get('errno', -1) != 0:
                raise SyncException(f"xpanfileprecreate {src} error: {api_response}")
            uploadid = api_response.get('uploadid')
    
            with open(src, 'rb') as f:
                for i in range(len(block_list)):
                    block = f.read(self.blocksize)
                    if not block:
                        break
                    api_response = api_instance.pcssuperfile2(token, str(i), dst, uploadid, "tmpfile", file=NameBytesIO(src, block))
                    if api_response.get('md5', "") != block_list[i]:
                        raise SyncException(f"pcssuperfile2 {src} with wrong md5: {api_response}, expect {block_list[i]}")
            api_response = api_instance.xpanfilecreate(token, dst, 0, size, uploadid, block_list_str, rtype=3)
        except exceptions.ApiException as e:
            raise SyncException(f"upload file {src} raised pcssdk Exception: {e}")
        except Exception as e:
            raise SyncException(f"Exception when upload file {src}: {e}")

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
            


