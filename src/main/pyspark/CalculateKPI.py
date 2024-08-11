import json

with open("..\\resources\\config.json","r") as f:
    config = json.load((f))

print(config)

with open("table.sql","r") as f:
    content = f.read()
print(content)

from code import AppConfig
t = AppConfig("None","None","None")
print(t)

