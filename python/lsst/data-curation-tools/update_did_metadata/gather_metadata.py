#!/usr/local/env python3
'''
gather_metadata.py

Script that gathers DID metadata, provided a file with a list of DIDs.
The script gathers metadata and PFNs from Rucio,
then uses the gfal client to get metadata from the PFNs.

Copyright 2025 Fermi National Accelerator Laboratory (FNAL)
'''
import argparse
import json
import pprint

import gfal2
from rucio.client import Client

client = Client()
ctx = gfal2.creat_context()


def read_did_file(file: str):
    """
    Reads a list of dids from a file that need updating.
    """
    dids = []
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            if 'raw' in line:
                did = line.split(' ')[0].strip()
                print(did)
                dids.append(did)
    return dids


def gather_metadata(dids):
    """
    Gather metadata from Rucio and gfal2
    """
    metadata = []
    for did in dids:
        scope, name = did.split(':')
        did_meta = client.get_metadata(scope=scope, name=name)

        replicas = client.list_replicas(dids=[{'scope': scope, 'name': name}],
                                        rse_expression='SLAC_RAW_DISK')
        replica = list(replicas)[0]
        pfns = replica['pfns']
        for pfn, info in pfns.items():
            if info['rse'] == 'SLAC_RAW_DISK':
                gfal_stat = ctx.stat(pfn)
                gfal_size = gfal_stat.st_size
                gfal_md5 = ctx.checksum(pfn, 'md5')
                gfal_adler32 = ctx.checksum(pfn, 'adler32')

                meta = {'name': name,
                        'scope': scope,
                        'adler32': gfal_adler32,
                        'md5': gfal_md5,
                        'bytes': gfal_size,
                        'old': {'adler32': did_meta['adler32'],
                                'md5': did_meta['md5'],
                                'bytes': did_meta['bytes']}}
                pprint.pprint(meta)
                metadata.append(meta)
    return metadata


def main():
    parser = argparse.ArgumentParser(
        description=("A utility that fetches DID metadata from a list of DIDs"
                     "and saves updated metadata corrections."))

    parser.add_argument(
        'dids_file',
        type=str,
        help='File containing the list of DIDs on each line'
    )

    parser.add_argument(
        'corrections_file',
        type=str,
        help='File path to save the generated corrections'
    )

    args = parser.parse_args()

    try:
        dids = read_did_file(args.dids_file)
        metadata = gather_metadata(dids)

        with open(args.corrections_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4)
    except FileNotFoundError as e:
        print(f"Error: File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()
