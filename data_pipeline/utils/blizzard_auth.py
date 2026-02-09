import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

class BlizzardAuth:
    def __init__(self):
        self.client_id = os.getenv("BLIZZARD_CLIENT_ID")
        self.client_secret = os.getenv("BLIZZARD_CLIENT_SECRET")
        self.access_token = None
        self.expiration_time = None

    def get_token(self):
        now = datetime.now()
        
        if self.access_token and self.expiration_time and now < (self.expiration_time - timedelta(seconds=30)):
            return self.access_token
        
        return self._request_new_token()

    def _request_new_token(self):
        url = "https://oauth.battle.net/token"
        
        payload = {'grant_type': 'client_credentials'}
        auth_tuple = (self.client_id, self.client_secret)
        
        try:
            response = requests.post(url, data=payload, auth=auth_tuple)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data['access_token']
            seconds_to_expire = data.get('expires_in', 86399)
            self.expiration_time = datetime.now() + timedelta(seconds=seconds_to_expire)
            
            print("Token Blizzard renovado com sucesso.")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao autenticar na Blizzard: {e}")

_auth_instance = BlizzardAuth()

def get_blizzard_token():
    return _auth_instance.get_token()