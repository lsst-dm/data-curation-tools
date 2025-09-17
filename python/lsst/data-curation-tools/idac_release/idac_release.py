#!/usr/bin/env python3
'''
Script to create rules for IDAC release using a config file
'''

import argparse
import json
from pprint import pprint

from rucio.client import Client


client = Client(account="release_service")


def parse_arguments():
    """
    Parses command-line arguments for a dry run and a configuration file.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
    """
    # Create the top-level parser
    parser = argparse.ArgumentParser(
        description="A script that can perform a dry run and use a JSON config file."
    )

    parser.add_argument(
        '--dry_run',
        action='store_true',
        help='Perform a dry run without making any replication rules.'
    )

    parser.add_argument(
        'rse',
        type=str,
        help='RSE name'
    )

    parser.add_argument(
        'file',
        type=str,
        help='Path to an IDAC release JSON configuration file.'
    )

    args = parser.parse_args()

    return args


def load_configuration(containers_file: str):
    with open(containers_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def main():
    """
    Creates IDAC release rules from a config file
    """
    parsed_args = parse_arguments()

    print(f"Dry run enabled: {parsed_args.dry_run}")
    print(f"RSE: {parsed_args.rse}")
    print(f"Config file: {parsed_args.file}")

    # open IDAC release json file
    with open(parsed_args.file, "r", encoding="utf-8") as f:
        data = json.load(f)

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
            replicas = client.list_did_rules(scope=scope, name=name)
            result = filter(lambda x: x['rse_expression'] == rse, replicas)
            if list(result):
                print(f"replica exists for {scope}:{name} at {rse}, skipping")
                skipped += 1
            else:
                print(f"no rules exist at {rse} for {scope}:{name}, will create rule")
                dids_to_transfer.append({"scope": scope, "name": name})
    print(f"Rules to create: {len(dids_to_transfer)}, "
          f"Rules skipped: {skipped}, "
          f"Total: {len(dids_to_transfer) + skipped}")
    if not parsed_args.dry_run:
        for did in dids_to_transfer:
            rules_created = client.add_replication_rule(dids=[did], copies=1,
                                                        rse_expression=rse,
                                                        asynchronous=True)
            print(f"Rule created: {rules_created}")


if __name__ == "__main__":
    main()
