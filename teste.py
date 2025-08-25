import requests, json
r = requests.post("http://localhost:11434/api/generate",
                  json={"model":"llama3.2:3b","prompt":"hi","stream":False},
                  timeout=60)
print(r.status_code); print(r.text)
