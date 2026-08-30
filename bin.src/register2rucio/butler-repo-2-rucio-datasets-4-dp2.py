#!/usr/bin/env python

'''
This script will split a butler repo's RUN collection(s) into Rucio datasets
It will create a number of refs_XXXXXXXX.txt files at
 <UUIDDIR>/<repoName>/<datasetType>, with each file contains
 1. First line: # <Rucio_dataset_DID_name> <N_files to be attached to the DID>
    DID_name: Dataset/{collection}-{datasetType}-{DFname}-{quarter}-{index:08d}
 2. The rest of lines: butler dateset UUIDs (their LFNs will be attached to the
    DID
'''


import sys
import os
import hashlib
import pandas
from datetime import timedelta,datetime,date
from lsst.daf.butler import Butler, CollectionType, _exceptions

# Adjust the following

startDate = date(2026, 7, 1)    # 1st day month when processing started
cutoffDate = date(2026, 8, 1)  # 1st day of the month after processing completed
repoName = '/sdf/data/rubin/u/yangw/repos/dp2'
rootChain = '*'   # leave it to '*' means processing all RUN collections
DFname = "USDF"
UUIDDIR = "/sdf/data/rubin/u/yangw/uuids/dp2"  # this is the output directory

###########
rucioScope = repoName
maxDIDperDataset = 50000

months = pandas.date_range(startDate, cutoffDate, freq='MS')

def remove_refs_in_rucio(refs: list, scope: str = rucioScope) -> list:
    """ Remove all refs that is already known to Rucio

        NOT IMPLEMENTED

    Parameters
    ----------
    refs : list
        Butler DatasetRef
    scope : str
        Rucio scope. By convention it is also the Butler repo name

    Returns
    -------
    A list of Butler DatasetRef unknown to Rucio
    """

    return refs


workingSetOfRefs = []


def register_refs_to_rucio(
    refsChunk: list,
    repo: str,
    # collection: str,
    datasetType: str,
    period: str,
    index: int,
    maxItems: int
):
    """ Create the a list of butler dataset UUIDs to be added to the Rucio
        The first line contains info about the Rucio dataset DID name

    Parameters
    ----------
    refsChunk : list
        A list of refs to be added to the workingSetOfRefs
    repo : str
        butler repo name
    collection : str
        butler collection name
    datasetType : str
        butler dataset type
    period : str
        periid, e.g. CY quarter the refs chucks was ingested to butler
    index : int
        An index number added to the end of the Rucio dataset name in case
        there are too many dataset from the same collection/datasetType/CYQ
    maxItems : int
        Maximum number of item in workingSetOfRefs after which the current
        working set should be put in a Rucio dataset

    Returns
    -------
    index : int
        The index for the next refsChunk
    """

    global workingSetOfRefs
    workingSetOfRefs.extend(refsChunk)
    while len(workingSetOfRefs) > maxItems:

        # Note: No element in rucioDatasetName contains a dash '-'
        # except {collection}
        rucioDatasetName = f'Dataset/{collection}-{datasetType}-{DFname}-{period}-{index:08d}'
        rucioDatasetName = f'Dataset/{datasetType}-{DFname}-{period}-{index:08d}'

        if maxItems == 0:
            maxItems = len(workingSetOfRefs)
        lines = [f'# {rucioDatasetName} {maxItems}\n']

        for ref in workingSetOfRefs[0:maxItems]:
            lines.append(f'{str(ref.id)}\n')

        uuiddir = f'{UUIDDIR}/{datasetType}'
        os.makedirs(uuiddir, exist_ok=True)
        uuidfile = f'{uuiddir}/refs_{hashlib.md5(rucioDatasetName.encode('utf-8')).hexdigest()}'
        with open(uuidfile, 'w') as f:
            f.writelines(lines)
        print(f'{uuidfile} : {rucioDatasetName} : {maxItems}')
        workingSetOfRefs = workingSetOfRefs[maxItems:]
        index += 1
    return index


# Initialize Butler
butler = Butler(repoName)

run_collections = sorted(butler.collections.query(
    expression=rootChain,
    collection_types={CollectionType.RUN},
    flatten_chains=True
))


dataset_types = []
for x in butler.registry.queryDatasetTypes():
    dataset_types.append(x.name)

#print(f"Debug len(months) = {len(months)}")

for q in range(len(months)-1):
    yearmonth = months[q].strftime("%Y%m")

    day = months[q].strftime("%Y-%m-%d")
    where = f"ingest_date >= T'{day}T00:00:00' and "

    day = months[q+1].strftime("%Y-%m-%d")
    where += f"ingest_date < T'{day}T00:00:00'"

#    print(f'Debug: {where}')
    for datasetType in dataset_types:
        if datasetType == 'raw':
            continue
        # Test if we can include all collections of the datasetType in on shot
        try:
#            print(f"Debug: Query dataset type {datasetType}")
            x = butler.query_datasets(
                dataset_type=datasetType,
                collections='*',
                find_first=False,
                where=where,
                order_by="ingest_date",
                limit=maxDIDperDataset
            )
            #refs = list(set(x))  # remove redundent
            refs = x
        except _exceptions.EmptyQueryResultError:
            refs = []

        if len(refs) < maxDIDperDataset:
            newRefsChunk = remove_refs_in_rucio(refs)

            register_refs_to_rucio(
                refsChunk=newRefsChunk,
                repo=repoName,
                # collection='allCollections',
                datasetType=datasetType,
                period=f'{yearmonth}',
                index=1,
                maxItems=0
            )
            continue

        # has too many files, chop them to smaller group 

#        print("Debug: starting the loop")
        index = 1
        for collection in run_collections:
            try:
                refs = butler.query_datasets(
                    dataset_type=datasetType,
                    collections=collection,
                    find_first=False,
                    where=where,
                    order_by="ingest_date",
                    limit=None
                )
            except _exceptions.EmptyQueryResultError:
                refs = []

            refsChunk = []
            for ref in refs:
                refsChunk.append(ref)
                if len(refsChunk) == 1000:
                    newRefsChunk = remove_refs_in_rucio(refsChunk, scope=rucioScope)
                    index = register_refs_to_rucio(
                        refsChunk=newRefsChunk,
                        repo=repoName,
                        # collection=collection,
                        datasetType=datasetType,
                        period=f'{yearmonth}',
                        index=index,
                        maxItems=maxDIDperDataset
                    )
                    refsChunk = []
            newRefsChunk = remove_refs_in_rucio(refsChunk, scope=rucioScope)
            index = register_refs_to_rucio(
                refsChunk=newRefsChunk,
                repo=repoName,
                # collection=collection,
                datasetType=datasetType,
                period=f'{yearmonth}',
                index=index,
                maxItems=maxDIDperDataset
            )
        register_refs_to_rucio(
            refsChunk=newRefsChunk,
            repo=repoName,
            # collection=collection,
            datasetType=datasetType,
            period=f'{yearmonth}',
            index=index,
            maxItems=0
        )
