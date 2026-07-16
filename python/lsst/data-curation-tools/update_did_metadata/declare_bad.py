#!/usr/bin/env python

import argparse
import json
import logging

from rucio.client.replicaclient import ReplicaClient


logging.basicConfig(level=logging.DEBUG)

client = ReplicaClient()


def parse_args():
    '''
    parse arguments
    '''
    description = "Updates DID metadata from a corrections file"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        'rse',
        type=str,
        help='Target RSE'
    )

    parser.add_argument(
        'corrections_file',
        type=str,
        help='JSON file with DID corrections'
    )

    args = parser.parse_args()

    return args


def main(args):
    with open(args.corrections_file, 'r') as f:
        dids = json.load(f)
    logging.info("Number of DIDs to declare bad: %s", len(dids))

    bad_dids = [{'scope': did['scope'], 'name': did['name']} for did in dids]
    logging.info("Bad DIDs: %s", bad_dids)

    logging.info("Declaring DIDs bad for %s", args.rse)

    declared = client.declare_bad_did_replicas(rse=args.rse,
                                               dids=bad_dids,
                                               reason="recreated")
    logging.info("Declared bad %s", declared)


if __name__ == '__main__':
    args = parse_args()
    try:
        main(args)
    except FileNotFoundError as e:
        print(f"Error: File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
