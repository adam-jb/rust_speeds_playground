# rust_speeds_playground

Testing IO and data processing with Rust and GCP




## download walking network from GCS: if running Rust locally
```
import requests

url = 'https://storage.googleapis.com/hack-bucket-8204707942/walk_network_full.json'
txt = requests.get(url).text

with open('walk.txt', 'w') as w:
    w.write(txt)
```
