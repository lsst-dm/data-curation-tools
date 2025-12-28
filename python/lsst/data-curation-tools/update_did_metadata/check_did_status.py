"""
check_did_status.py

Check the status of did replicas

Copyright 2025 Fermi National Accelerator Laboratory (FNAL)
"""
import argparse

from rucio.client import Client


client = Client()


def load_dids(file):
    with open(file, 'r') as f:
        lines = f.readlines()

    dids = []
    for line in lines:
        did = line.strip()
        scope, name = did.split(':')
        dids.append({
            "scope": scope,
            "name": name
        })
    return dids


def parse_args():
    '''
    parse arguments
    '''
    description = "Checks the status of a given list of DIDs"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        '--boost',
        action='store_true',
        help='Boost the rules. Default False.'
    )

    parser.add_argument(
        'dids_file',
        type=str,
        help='File with list of DIDs'
    )

    args = parser.parse_args()

    return args


def main(args):
    try:
        dids = load_dids(args.dids_file)

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
                            'boost_rule': args.boost
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
    except FileNotFoundError as e:
        print(f"Error: File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
