import requests

url = "https://www.freelancer.com/api/users/0.1/self/"

headers = {'freelancer-oauth-v1': '<oauth_access_token>'}

response = requests.request("GET", url, headers=headers)

print(response.text)