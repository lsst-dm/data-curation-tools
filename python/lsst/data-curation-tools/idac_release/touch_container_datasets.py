#!/usr/bin/env python3
'''
Script to touch the datasets in a container
'''

from rucio.client import Client


client = Client(account="dylee")


def get_datasets(scope: str, name: str):
    '''
    Fetches the datasets for a scope and name
    '''
    dids = client.list_dids(scope=scope,
                            filters={'name': name})

    return dids


def touch_dataset(scope: str, name: str):
    '''
    Subscription touch a dataset
    '''
    client.set_metadata(scope, name, 'is_new', True)


def main():
    dids = get_datasets(scope='dp1', name='Dataset/Provenance*')

    for did in dids:
        print(f'dp1:{did}')
        touch_dataset(scope='dp1', name=did)


if __name__ == '__main__':
    main()
