import unittest
from apiauth import apiauth

class ApiAuthTest(unittest.TestCase):
    def setUp(self):
        self.key = "key"
        self.secret = "secret"
        self.auth = apiauth.Auth(key=self.key, secret=self.secret)

    def test_authorize_url(self):
        url = self.auth.authorize_url()
        print(url)
        self.assertEqual(url, f"http://openapi.baidu.com/oauth/2.0/authorize?response_type=code&client_id={self.key}&redirect_uri=oob&scope=basic,netdisk")

if __name__ == '__main__':
    unittest.main()
