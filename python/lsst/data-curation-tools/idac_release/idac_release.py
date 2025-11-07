#!/usr/bin/env python3
'''
Script to create rules for IDAC release using a config file
'''

import argparse
import json
import textwrap

from rucio.client import Client
from rucio.common.exception import DuplicateRule


client = Client(account="release_service")


def parse_arguments():
    """
    Parses command-line arguments for a dry run and a configuration file.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
    """

    examples = textwrap.dedent('''\
            examples:
                python idac_release.py --did_file ./dp1.json --idac_file ./IDAC_US-UW.json IDAC_US-UW
            ''')
    # Create the top-level parser
    parser = argparse.ArgumentParser(
        description="A script that can perform a dry run and use a JSON config file.",
        epilog=examples
    )

    parser.add_argument(
        '--dry_run',
        action='store_true',
        help='Perform a dry run without making any replication rules.'
    )

    parser.add_argument(
        '--idac_file',
        type=str,
        help='Path to an IDAC product selection JSON configuration file.'
    )

    parser.add_argument(
        '--did_file',
        type=str,
        help='Path to the data release JSON configuration file. Contains number of files in a container.'
    )

    parser.add_argument(
        'rse',
        type=str,
        help='RSE name'
    )

    args = parser.parse_args()

    return args


def load_configuration(containers_file: str):
    with open(containers_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def get_container_datasets(did):
    dids = client.list_content(scope=did['scope'],
                               name=did['name'])

    to_add = [{'scope': d['scope'], 'name': d['name']} for d in dids if d['type'] == 'DATASET']

    return to_add


def check_rule_exists(rse, scope, name):
    replicas = client.list_did_rules(scope=scope, name=name)
    result = filter(lambda x: x['rse_expression'] == rse, replicas)

    return result


def main():
    """
    Creates IDAC release rules from a config file
    """
    parsed_args = parse_arguments()

    print(f"Dry run enabled: {parsed_args.dry_run}")
    print(f"RSE: {parsed_args.rse}")
    print(f"Release file: {parsed_args.did_file}")
    print(f"Config file: {parsed_args.idac_file}")

    # open IDAC release json file
    with open(parsed_args.idac_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # open data release dataset sizes
    with open(parsed_args.did_file, 'r', encoding='utf-8') as f:
        release_sizes = json.load(f)

    me = client.whoami()
    print(f"Creating rules for account {me['account']}")

    rse = data['rse']
    if rse != parsed_args.rse:
        raise Exception("RSE doesn't match config RSE")
    containers = data['containers']

    dids_to_transfer = []
    skipped = 0
    for container, enabled in containers.items():
        if enabled:
            scope, name = container.split(':')
            result = check_rule_exists(rse, scope, name)
            if list(result):
                print(f"replica exists for {scope}:{name} at {rse}, skipping")
                skipped += 1
            else:
                print(f"no rules exist at {rse} for {scope}:{name}, will create rule")
                dids_to_transfer.append({"scope": scope, "name": name})
    print(f"Rules to create: {len(dids_to_transfer)}, "
          f"Rules skipped: {skipped}, "
          f"Total: {len(dids_to_transfer) + skipped}")
    for did in dids_to_transfer:
        if release_sizes[f"{did['scope']}:{did['name']}"] > 10000:
            print("Large container; using datasets")
            dids = get_container_datasets(did)
        else:
            dids = [did]
        if not parsed_args.dry_run:
            try:
                rules_created = client.add_replication_rule(dids=dids,
                                                            copies=1,
                                                            rse_expression=rse,
                                                            activity="IDAC Release",
                                                            asynchronous=True)
                print(f"Rule created: {rules_created}")
            except DuplicateRule:
                print(f"replica exists for {did['scope']}:{did['name']} at {rse}, skipping")


if __name__ == "__main__":
    main()
