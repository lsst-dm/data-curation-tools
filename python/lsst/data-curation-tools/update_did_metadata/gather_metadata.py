#!/usr/local/env python3

import json
import pprint

import gfal2
from rucio.client import Client

c = Client()
ctx = gfal2.creat_context()


def read_did_file():
    dids = []
    with open('data/20251006_missing.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            if 'raw' in line:
                did = line.split(' ')[0]
                dids.append(did)
    return dids


def gather_metadata(dids):
    """
    Gather metadata from Rucio and from using gfal2
    """
    metadata = []
    for did in dids:
        scope, name = did.split(':')
        did_meta = c.get_metadata(scope=scope, name=name)

        replicas = c.list_replicas(dids=[{'scope': scope, 'name': name}],
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
    dids = read_did_file()
    metadata = gather_metadata(dids)

    with open('20251006_corrections.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)


if __name__ == '__main__':
    main()
