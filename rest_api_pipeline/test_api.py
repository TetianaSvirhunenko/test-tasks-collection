import unittest
from unittest.mock import patch, Mock
from src.api_client import DummyJsonAPI

class TestDummyJsonAPI(unittest.TestCase):

    def setUp(self):
        self.api = DummyJsonAPI(base_url="https://dummyjson.com")

    @patch("src.api_client.requests.post")
    def test_post_success(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"result": "ok"}
        mock_post.return_value = mock_response

        result = self.api.post("/some-endpoint", {"key": "value"})
        self.assertEqual(result, {"result": "ok"})
        mock_post.assert_called_once()

    @patch("src.api_client.requests.Session.get")
    def test_get_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"data": [1, 2, 3]}
        mock_get.return_value = mock_response

        result = self.api.get("/test-endpoint")
        self.assertEqual(result, {"data": [1, 2, 3]})
        mock_get.assert_called_once()

    @patch("src.api_client.DummyJsonAPI.get")
    def test_get_paginated(self, mock_get):
        mock_get.side_effect = [
            {"users": [{"id": 1}, {"id": 2}]},
            {"users": [{"id": 3}]},
            {"users": []}
        ]

        result = self.api.get_paginated("/users")
        self.assertEqual(result, [{"id": 1}, {"id": 2}, {"id": 3}])
        self.assertEqual(mock_get.call_count, 3)

    @patch("src.api_client.DummyJsonAPI.post")
    def test_login_stores_tokens(self, mock_post):
        mock_post.return_value = {
            "accessToken": "abc123",
            "refreshToken": "refresh456"
        }

        self.api.login({"username": "test", "password": "pass"})
        self.assertEqual(self.api.access_token, "abc123")
        self.assertEqual(self.api.refresh_token, "refresh456")
