import json
import os

cmd_create_topic = 'confluent kafka topic create movielog --if-not-exists --cluster lkc-nvvpxd'
cmd_create_connector = 'confluent connect create --config mongodb-sink.json --cluster lkc-nvvpxd'
cmd_produce = 'confluent kafka topic produce movielog --cluster lkc-nvvpxd'

os.system(cmd_create_topic)
os.system(cmd_create_connector)

with open('rating.json', 'r', encoding= 'utf-8') as f:
    data = json.load(f)

for d in data:
    os.system(f'{cmd_produce} {d}')