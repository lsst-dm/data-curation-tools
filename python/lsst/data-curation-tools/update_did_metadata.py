"""
update_did_metadata.py

An example on updating a DID's adler32, bytes, and md5 using Rucio's internal
core API

To use this, we need a rucio.cfg from a server or daemon that contains the
rucio database url. One way is to copy this script to a running rucio server
instance.

An example dids_template.json shows how the metadata is loaded into the script.

Copyright 2025 Fermi National Accelerator Laboratory (FNAL)
"""
import json

from rucio.core import did
from rucio.common.types import InternalScope


def update_did(scope, name, adler32, md5, filesize):
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


def main():
    with open('dids.json', 'r') as f:
        dids = json.load(f)

    for file in dids:
        scope = InternalScope(scope=file['scope'])
        print("Current metadata: ", get_metadata(scope, file['name']))
        update_did(scope, file['name'], file['adler32'], file['md5'],
                   file['filesize'])
        print("Updated metadata: ", get_metadata(scope, file['name']))


if __name__ == '__main__':
    main()
