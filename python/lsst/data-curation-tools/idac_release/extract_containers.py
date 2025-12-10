#!/usr/bin/env python3
'''
extract_containers.py extracts the DIDs from the IDAC spreadsheet
'''

import csv
import json

data = []
with open('./IDAC DP1 Rucio Transfers - Container selections.csv', 'r', encoding='utf-8') as f:
    csv_reader = csv.DictReader(f)
    for row in csv_reader:
        formatted = {f"dp1:Container/{row['dp1:Container/']}": int(row['N datasets'])}
        data.append(formatted)

flattened = {k: v for d in data for k, v in d.items()}

with open('./dp1.json', 'w', encoding='utf-8') as f:
    json.dump(flattened, f, indent=4)
