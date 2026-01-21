import os
import requests
from datetime import datetime, timedelta

class BlizzardAuth:
    def __init__(self):
        self.client_id = os.getenv("BLIZZARD_CLIENT_ID")
        self.client_secret = os.getenv("BLIZZARD_CLIENT_SECRET")
        
        self.access_token = None
        self.expiration_time = None

    def get_token(self):
        now = datetime.now()
        
        if self.access_token and self.expiration_time and now < self.expiration_time:
            return self.access_token
        
        return self._request_new_token()

    def _request_new_token(self):
        url = "https://oauth.battle.net/token"
        
        payload = {'grant_type': 'client_credentials'}
        auth_tuple = (self.client_id, self.client_secret)
        
        response = requests.post(url, data=payload, auth=auth_tuple)
        
        if response.status_code == 200:
            data = response.json()
            
            self.access_token = data['access_token']
            
            seconds_to_expire = data['expires_in']
            self.expiration_time = datetime.now() + timedelta(seconds=seconds_to_expire)
            
            return self.access_token
        else:
            raise Exception(f"Erro ao pegar token: {response.status_code} - {response.text}")