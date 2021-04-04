import csv
import codecs
import json
import pandas as pd


with open('/Users/nikilkatturi/PycharmProjects/Travelers/venv/part-r-00000-ace4d84a-d8cb-4eaa-a377-9d3e8bbf73b2', 'r',encoding='utf-8-sig',errors='replace') as f:
    data = f.read()
    json_data = []
    split_data = data.split('\n')

    with open("../data.json", 'w') as file:
        for data_row in split_data:
            if data_row:
             print(json.dumps(data_row), file=file)

with open('../data.json', encoding="utf8") as f:
    data = f.readlines()
    data = [json.loads(line) for line in data] #convert string to dict format
df = pd.read_json(data,encoding='latin-1')
















