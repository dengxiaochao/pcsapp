from enum import Enum
import yaml

def check_field(conf:dict[str, str], key:str):
    value = conf.get(key, None)
    if not value:
        raise Exception(f'config error: key {key} should not be empty')
    return value

class PCSConfig(object):
    def __init__(self, conf: dict[str, str]):
        self.key = check_field(conf, 'key')
        self.secret = check_field(conf, 'secret')
        self.endpoint = check_field(conf, 'endpoint')


class SyncType(Enum):
    DOWNLOAD = 'download'
    UPLOAD = 'upload'
    UPDOWNLOAD = 'updownload'

class SyncDir(object):
    local: str
    remote: str
    type: SyncType
    excludes: list[str]
    def __init__(self, conf: dict[str, str]):
        self.local = check_field(conf, 'local_dir')
        self.remote = check_field(conf, 'remote_dir')
        self.type = SyncType(conf.get('type', 'download'))
        self.excludes = conf.get('excludes', [])

class Config(object):
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        self.pcs = PCSConfig(self.config.get('pcs', {}))
        self.sync = [SyncDir(d) for d in self.config.get('sync', [])]

    def load_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.load(f, yaml.FullLoader)
        return config
