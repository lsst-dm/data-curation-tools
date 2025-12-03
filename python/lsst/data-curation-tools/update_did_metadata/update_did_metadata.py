#!/usr/local/env python3
"""
update_did_metadata.py

An example on updating a DID's adler32, bytes, and md5 using Rucio's internal
core API. The rucio client is *NOT* used by this script.

To use this, we need a rucio.cfg from a server or daemon that contains the
rucio database url. One way is to copy this script to a running rucio server
instance.

The adler32, bytes, and md5 lists should also have the same line count
and are sorted by the files.

An example dids_template.json shows how the metadata could be loaded into
the script.

Copyright 2025 Fermi National Accelerator Laboratory (FNAL)
"""
import argparse
import json
import logging

from rucio.core import did
from rucio.common.types import InternalScope


logging.basicConfig(level=logging.DEBUG)


def update_did(scope: InternalScope,
               name: str,
               adler32: str,
               md5: str,
               filesize):
    """
    Updates a did's adler32, bytes, and md5 given the scope and name
    """
    meta = {
        'adler32': adler32,
        'bytes': filesize,
        'md5': md5
    }
    did.set_metadata_bulk(scope=scope, name=name, meta=meta)


def get_metadata(scope, name):
    """
    Fetches the metadata of a did from the name and scope
    """
    current = did.get_metadata(scope=scope, name=name)
    return current


def load_metadata():
    """
    Legacy. Loads metadata from files
    """
    metadata = ['adler32', 'filesize', 'md5']
    file_data = {}
    for m in metadata:
        with open(f'{m}.73.txt', 'r') as f:
            file_data[m] = f.readlines()
    metadata_val = []
    for adler32, md5, filesize in zip(file_data['adler32'],
                                      file_data['md5'],
                                      file_data['filesize']):
        adler_scope, rucio_adler, butler_adler = adler32.strip().split(' ')
        md5_scope, rucio_md5, butler_md5 = md5.strip().split(' ')
        (filesize_scope,
         rucio_filesize,
         butler_filesize) = filesize.strip().split(' ')

        if adler_scope == md5_scope and adler_scope == filesize_scope:
            scope, name = adler_scope.split(':')
            meta = {
                "name": name,
                "scope": scope,
                "adler32": butler_adler,
                "md5": butler_md5,
                "bytes": int(butler_filesize),
                "old": {
                    "adler32": rucio_adler,
                    "md5": rucio_md5,
                    "bytes": int(rucio_filesize),
                }
            }
            metadata_val.append(meta)

    return metadata_val


def verify_metadata(a: dict, b: dict) -> bool:
    """
    Verify if metadata matches between dicts
    """
    for k, v in a.items():
        if v != b[k]:
            return False
    return True


def load_from_json(file):
    '''
    Loads corrections file from JSON
    '''
    with open(file, 'r') as f:
        dids = json.load(f)
    return dids


def parse_args():
    '''
    parse arguments
    '''
    description = "Updates DID metadata from a corrections file"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        'corrections_file',
        type=str,
        help='JSON file with DID corrections'
    )

    args = parser.parse_args()

    return args


def main(args):
    dids = load_from_json(args.corrections_file)
    logging.info("Number of DIDs to update: %s", len(dids))

    to_update = 0
    updated = 0
    failed = []
    for file in dids:
        logging.info("Updating %s:%s with metadata: %s",
                     file['scope'], file['name'], file)
        scope = InternalScope(scope=file['scope'])
        current_metadata = get_metadata(scope, file['name'])
        logging.debug("Current metadata for %s:%s: %s", file['scope'],
                      file['name'], file)
        current_vals = {k: v for k, v in current_metadata.items()
                        if k in ['adler32', 'md5', 'bytes']}
        if verify_metadata(current_vals, file['old']):
            try:
                to_update += 1
                update_did(scope, file['name'], file['adler32'],
                           file['md5'], file['bytes'])
            except Exception as e:
                logging.error("DID %s:%s update failed due to %s",
                              file['scope'], file['name'], e)
                failed.append(file)
                continue
            updated_metadata = get_metadata(scope, file['name'])
            updated_vals = {k: v for k, v in updated_metadata.items()
                            if k in ['adler32', 'md5', 'bytes']}
            if verify_metadata(updated_vals, file):
                updated += 1
                logging.info("Updated metadata: ", updated_metadata)
                logging.info("DID %s:%s updated", file['scope'], file['name'])
            else:
                logging.info("Failed to update or dry run")
        else:
            if verify_metadata(current_vals, file):
                logging.info("DID %s:%s already updated, skipping",
                             file['scope'], file['name'])
            else:
                logging.info(("DID %s:%s metadata doesn't match provided "
                              "old metadata, skipping"),
                             file['scope'], file['name'])

    logging.info(("Number of DIDs to update: %s,"
                  "Number of updated DIDs: %s, "
                  "Number of failed updates: %s"),
                 to_update,
                 updated, failed)


if __name__ == '__main__':
    args = parse_args()
    try:
        main(args)
    except FileNotFoundError as e:
        print(f"Error: File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
