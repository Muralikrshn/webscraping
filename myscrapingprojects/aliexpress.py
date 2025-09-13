


# import requests
# from lxml import html

# r = requests.get("https://www.aliexpress.com/w/wholesale-Digital-Cameras.html")
# tree = html.fromstring(r.text)


import requests
from lxml import html
import re
import json5   # safer than json since AliExpress uses JS object notation

url = "https://www.aliexpress.com/w/wholesale-Digital-Cameras.html"
headers = {
    
    "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-length": "3022",
    "origin": "https://www.aliexpress.com",
    "referer": "https://www.aliexpress.com/",
    "user-agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 CrKey/1.54.250320",
    
}


r = requests.get(url)
tree = html.fromstring(r.text)
print(f"Status Code: {r.status_code}")
print(tree)

# Get all script tags
scripts = tree.xpath("//script[contains(text(),'window._dida_config_._init_data_')]/text()")
print(scripts)

# Look for the one containing "window.runParams" or "__data"
pattern = r"window\._dida_config_\._init_data_\s*=\s*(\{.*?\});"
data_obj = None

for script in scripts:
    match = re.search(pattern, script, re.S)
    if match:
        raw_data = match.group(1)
        try:
            # Parse with json5 (tolerates JS-like JSON)
            data_obj = json5.loads(raw_data)
            break
        except Exception as e:
            print("Parsing error:", e)

if data_obj:
    # Example: extract product items (AliExpress uses "items" or "mods" keys)
    print(data_obj)
else:
    print("No matching data found")
