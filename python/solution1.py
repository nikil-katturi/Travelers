import csv
import json
import pandas as pd


with open('/Users/nikilkatturi/PycharmProjects/Travelers/python/part-r-00000-ace4d84a-d8cb-4eaa-a377-9d3e8bbf73b2', 'r',encoding='utf-8-sig',errors='replace') as f:

    data = f.read()
    json_data = []
    split_data = data.split('\n')

    for data_row in split_data:
        if data_row:
            json_data.append(json.loads(data_row))

    print(json_data)
    data_file = open('data_file.csv', 'w')

    csv_writer = csv.writer(data_file)


    flag = True

    for data in json_data:
        # to get the key and run it only once
        if flag == True:
            # writting the key - > header to the csv file
            header = data.keys()
            csv_writer.writerow(header)
            flag = False

        # mapping the value to the key(header) in the csv file
        csv_writer.writerow(data.values())

    data_file.close()
































