#!/usr/bin/env python

# Ingest all DID in a Rucio dataset to a butler
#
# make sure those DID have metadata "rubin_butler" and "rubin_sidecar" defined.
#
# The following envars control execution
#
# "INGEST_BUTLER"
#       Path to the Butler repository that should be used.

import sys
import os
from rucio.client.didclient import DIDClient
from lsst.daf.butler import Butler, DatasetRef, FileDataset, registry

repo = os.getenv("INGEST_BUTLER", None)
if repo is None:
    raise ValueError("Please point unix envorinment INGEST_BUTLER to a Butler")


def ingest_to_butler(dids: list[dict]):

    scopenames = []
    for did in dids:
        scopenames.append({"scope": did["scope"], "name": did["name"]})

    entries = []
    for data in DIDclient.get_metadata_bulk(scopenames, inherit=True, plugin='JSON'):

        if 'rubin_butler' not in data.keys() or 'rubin_sidecar' not in data.keys():
            raise KeyError("rubin_butler or rubin_sidecar not found")

        if data['rubin_butler'] != 'data_product':
            raise TypeError(f"rubin_butler is not data_product ({data['rubin_butler']})")

        path = data['name']
        sidecar = data['rubin_sidecar']
        ref = DatasetRef.from_json(sidecar, registry=butler.registry)
        entries.append(FileDataset(path, [ref]))

    try:
        butler.ingest(*entries, transfer=None, record_validation_info=False)
    except registry._exceptions.ConflictingDefinitionError:
        # if some of them are already in bulter, try one-by-one
        for entry in entries:
            try:
                butler.ingest(entry, transfer=None, record_validation_info=False)
            except registry._exceptions.ConflictingDefinitionError as e:
                print(f"\nfail to ingest {entry.path}")
                print(e)
                pass


def main():

    global butler, butler_root, DIDclient, maxItems
    butler = Butler(repo, writeable=True)
    butler_root = butler._datastore.root.ospath
    DIDclient = DIDClient()
    maxItems = 500

    dids = []
    while did := sys.stdin.readline().rstrip('\n'):
        scope, name = did.split(":")
        dids.append({"scope": scope, "name": name})
        if len(dids) >= maxItems:
            ingest_to_butler(dids)
            dids = []
    ingest_to_butler(dids)


if __name__ == "__main__":
    main()
