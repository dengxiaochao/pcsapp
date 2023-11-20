import logging
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'third_party', 'pcssdk'))
from pprint import pprint
from apiauth import apiauth
from config import config
from sync import sync

def setup_logging():
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("sync.log")
    fileHandler.setFormatter(formatter)
    rootLogger.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    rootLogger.addHandler(consoleHandler)

if __name__ == '__main__':
    setup_logging()
    conf = config.Config("pcs.yml")
    pprint(vars(conf))

    auth = apiauth.Auth(conf.pcs.endpoint, conf.pcs.key, conf.pcs.secret)
    if not auth.try_recover_access_token():
        print("open following url in browser and copy authorization_code:")
        print(auth.authorize_url())
        print("once done, press any key to continue...")
        input()
        print("now input your authorization_code:")
        code = input()
        auth.setup_access_token(code)
    print("success. got access_token:", auth.get_access_token())

    logging.getLogger().info("start sync...")
    for s in conf.sync:
        s = sync.Sync(s.local, s.remote, auth, type=s.type, excludes=s.excludes, blocksize=20*1024*1024)
        s.sync()

