import json


with open('20251101_corrections.json', 'r', encoding='utf-8') as f:
    data = json.load(f)


match = 0
update = 0
for did in data:
    old = did['old']

    different = False
    diff_keys = []
    for k, v in old.items():
        if did[k] != v:
            diff_keys.append(k)
            different = True
    if different:
        print(f"{did['scope']}:{did['name']} is different")
        print(diff_keys)
        update += 1
    else:
        print(f"{did['scope']}:{did['name']} matches")
        match += 1
print(f"Match: {match}, Update: {update}")
