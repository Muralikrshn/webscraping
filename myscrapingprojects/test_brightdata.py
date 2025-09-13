import requests

# Proxy details from your curl command
proxy_host = "brd.superproxy.io"
proxy_port = "33335"
proxy_user = "brd-customer-hl_5bcfb25a-zone-datacenter_proxy1"
proxy_pass = "e69r493xfrf2"

# URL to test (Bright Data test endpoint)
test_url = "https://geo.brdtest.com/welcome.txt?product=dc&method=native"

# Build proxy dictionary
proxies = {
    "http": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}",
    "https": f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}",
}

try:
    response = requests.get(test_url, proxies=proxies, timeout=20)
    print("Status Code:", response.status_code)
    print("Response:\n", response.text)
except requests.exceptions.RequestException as e:
    print("Error:", e)
