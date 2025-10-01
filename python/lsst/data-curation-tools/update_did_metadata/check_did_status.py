"""
Check the status of did replicas
"""
from rucio.client import Client


client = Client()


def load_dids():
    with open('adler32.305.txt', 'r') as f:
        lines = f.readlines()

    dids = []
    for line in lines:
        did, _, _ = line.strip().split(' ')
        scope, name = did.split(':')
        dids.append({
            "scope": scope,
            "name": name
        })
    return dids


def main():
    dids = load_dids()
    total_rules = 0
    total_ok = 0
    total_stuck = 0
    total_suspended = 0
    total_replicating = 0
    for did in dids:
        rules = client.list_associated_rules_for_file(
            scope=did['scope'],
            name=did['name']
        )
        stuck = 0
        suspended = 0
        replicating = 0
        ok = 0
        for rule in rules:
            total_rules += 1
            if rule['state'] in ['STUCK']:
                print(rule)
                stuck += 1
                total_stuck += 1
                client.update_replication_rule(
                    rule_id=rule['id'],
                    options={
                        'boost_rule': True
                    }
                )
            if rule['state'] in ['REPLICATING']:
                replicating += 1
                total_replicating += 1
            if rule['state'] in ['SUSPENDED']:
                suspended += 1
                total_suspended += 1
            if rule['state'] in ['OK']:
                ok += 1
                total_ok += 1
        if stuck > 0 or suspended > 0:
            print(f"{did['scope']}:{did['name']}")
            msg = (
                f"Ok: {ok}, Stuck: {stuck}, "
                f"Suspended: {suspended}, Replicating: {replicating}"
            )
            print(msg)
    print(f"Total rules: {total_rules}")
    msg = (
        f"Ok: {total_ok}, Stuck: {total_stuck}, "
        f"Suspended: {total_suspended}, Replicating: {total_replicating}"
    )
    print(msg)


if __name__ == "__main__":
    main()
