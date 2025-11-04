import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

def is_retryable_exception(exc):
    return isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None and exc.response.status_code in RETRY_STATUS_CODES

class DummyJsonAPI:

    def __init__(self, base_url):
        self.base_url = base_url
        self.access_token = None
        self.refresh_token = None
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_retryable_exception)
    )
    def post (self, endpoint, json):
        url = f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}
        response = requests.post(url, json=json, headers=headers)
        if response.status_code in (429, 500, 502, 503, 504):
            response.raise_for_status()
        elif not response.ok:
            raise Exception(f"Non-retryable error occurred: {response.status_code}")
        return response.json()
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_retryable_exception)
    )
    def get(self, endpoint):
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url)
        if response.status_code in (429, 500, 502, 503, 504):
            response.raise_for_status()
        elif not response.ok:
            raise Exception(f"Non-retryable error occurred: {response.status_code}")
        return response.json()
    
    def get_paginated(self, endpoint, limit=30):
        all_items = []
        skip = 0
    
        while True:
            paged_endpoint = f"{endpoint}?limit={limit}&skip={skip}"
            response = self.get(paged_endpoint)
            items = response.get('users') or response.get('posts') or response.get('items') 
            if not items:
                break
            all_items.extend(items)
            skip += limit    
        return all_items
    
    def create_user(self, user_data):
        return self.post("/users/add", user_data)
    
    def login (self, credentials):
        data = self.post("/auth/login", credentials)
        self.access_token = data.get("accessToken")
        self.refresh_token = data.get("refreshToken")

    def get_filtered_users(self, key, value):
        return self.get_paginated(f"/users/filter?key={key}&value={value}")
    
    def get_posts_by_users (self, filtered_users):
        all_posts = []

        for user in filtered_users:
            user_id = user['id']
            posts = self.get_paginated(f"/users/{user_id}/posts")
            all_posts.extend(posts)
        return all_posts


