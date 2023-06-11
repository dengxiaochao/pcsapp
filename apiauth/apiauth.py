from datetime import datetime, timedelta
import requests

class Auth(object):
    def __init__(self, endpoint: str="http://openapi.baidu.com/oauth/2.0", key: str="", secret: str=""):
        self.endpoint:str = endpoint
        self.key:str = key
        self.secret:str = secret
        self.access_token:dict = {}
        self.refresh_at:float = 0.0
        self.refresh_token_file = ".refresh_token"

    def headers(self) -> dict:
        return {'User-Agent': 'pan.baidu.com'}

    def _set_access_token(self, access_token: dict):
        self.access_token = access_token
        refresh_at = datetime.now() + timedelta(seconds=access_token['expires_in'])
        self.refresh_at = refresh_at.timestamp()
        self._dump_refresh_token(access_token['refresh_token'])

    def authorize_url(self) -> str:
        return f'{self.endpoint}/authorize?response_type=code&client_id={self.key}&redirect_uri=oob&scope=basic,netdisk'
    
    def setup_access_token(self, code: str) -> None:
        url = f'{self.endpoint}/token?grant_type=authorization_code&code={code}&client_id={self.key}&client_secret={self.secret}&redirect_uri=oob'
        resp = requests.get(url, headers=self.headers())
        if resp.status_code == 200:
            return self._set_access_token(resp.json())
        raise Exception(f'setup_access_token failed, response: {resp.text}')

    def refresh_access_token(self, refresh_token: str) -> None:
        url = f'{self.endpoint}/token?grant_type=refresh_token&refresh_token={refresh_token}&client_id={self.key}&client_secret={self.secret}'
        resp = requests.get(url, headers=self.headers())
        if resp.status_code == 200:
            return self._set_access_token(resp.json())
        raise Exception(f'refresh_access_token failed, response: {resp.text}')

    def get_access_token(self) -> str:
        if self.refresh_at == 0.0:
            raise Exception('access_token not set')
        if datetime.now().timestamp() >= self.refresh_at:
            self.refresh_access_token(self.access_token['refresh_token'])
        return self.access_token['access_token']

    def _dump_refresh_token(self, refresh_token:str):
        if len(refresh_token) == 0:
            return
        with open(self.refresh_token_file, "w") as f:
            f.write(refresh_token)

    def try_recover_access_token(self) -> bool:
        try:
            with open(self.refresh_token_file, "r") as f:
                refresh_token = f.read()
                self.refresh_access_token(refresh_token.strip())
                return True
        except Exception:
            return False

