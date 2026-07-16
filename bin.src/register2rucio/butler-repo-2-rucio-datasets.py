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


import os
import hashlib
from lsst.daf.butler import Butler, CollectionType, _exceptions

# Adjust the following

startDate = '2025-01-02'   # will only process butler datasets ingested between
cutoffDate = '2026-05-10'  # begin of the quarter (of startDate) and cufoffDate
repoName = 'dp1'
rootChain = '*'   # leave it to '*' means processing all RUN collections
DFname = "USDF"
UUIDDIR = "/sdf/data/rubin/u/yangw/uuids"  # this is the output directory

###########
rucioScope = repoName
maxDIDperDataset = 50000

years = []
years.append(int(startDate.split('-')[0]))
years.append(int(cutoffDate.split('-')[0]))
quarters = []
for year in years:
    for q in ['01-01', '04-01', '07-01', '10-01']:
        quarters.append(f'{year}-{q}')

quarters.append(f'{years[-1]+1}-01-01')
for qSkip in range(len(quarters)-1):
    if quarters[qSkip+1] > startDate:
        break


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
    collection: str,
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
    if len(workingSetOfRefs) >= maxItems:

        # Note: No element in rucioDatasetName contains a dash '-'
        # except {collection}
        rucioDatasetName = f'Dataset/{collection}-{datasetType}-{DFname}-{period}-{index:08d}'

        lines = [f'# {rucioDatasetName} {len(workingSetOfRefs)}\n']
        """
        lines = []
        headers = (
            f"#                    Butler repo : {repo}\n",
            f"#              Butler collection : {collection}\n",
            f"#            Butler dataset type : {datasetType}\n",
            f"#       Butler Ingest_date range : {period}\n",
            f"#             Data facility name : {DFname}\n",
            f"#         Rucio dataset sequence : {index:08d}\n",
            f"# Number of bulter dataset UUIDs : {len(workingSetOfRefs)}\n",
            f"#\n",
            f"# rucio-register dataset-list\\\n",
            f"#     --rucio-register-config <rucio-register-config-file> \\\n",
            f"#     --repo {repo} \\\n",
            f"#     --rucio-dataset {rucioDatasetName} \\\n",
            f"#     --uuidlist <this_file>\n"
        )

        lines.extend(headers)
        """

        for ref in workingSetOfRefs:
            lines.append(f'{str(ref.id)}\n')

        uuiddir = f'{UUIDDIR}/{repo}/{datasetType}'
        os.makedirs(uuiddir, exist_ok=True)
        uuidfile = f'{uuiddir}/refs_{hashlib.md5(rucioDatasetName.encode('utf-8')).hexdigest()}'
        with open(uuidfile, 'w') as f:
            f.writelines(lines)
        print(f'{uuidfile} : {rucioDatasetName} : {len(workingSetOfRefs)}')
        workingSetOfRefs = []
        index += 1
    return index


# Initialize Butler
butler = Butler(repoName)

run_collections = sorted(butler.collections.query(
    expression=rootChain,
    collection_types={CollectionType.RUN},
    flatten_chains=True
))

for collection in run_collections:
    datasetTypes = sorted(butler.collections.get_info(collection,
                                                      include_summary=True).dataset_types)
    if 'raw' not in datasetTypes:
        # first check to see if the collection has enough datasets
        # that worth chopping
        try:
            where = f"ingest_date >= T'{startDate}T00:00:00' and "
            where += f"ingest_date < T'{cutoffDate}T00:00:00'"
            refs = butler.query_all_datasets(
                collections=collection,
                where=where,
                find_first=False,
                limit=maxDIDperDataset+1
            )
        except _exceptions.EmptyQueryResultError:
            continue

        if len(refs) <= maxDIDperDataset:
            newRefsChunk = remove_refs_in_rucio(refs)

            dateRange = f'{"".join(startDate.split("-"))}TO{"".join(cutoffDate.split("-"))}'
            register_refs_to_rucio(
                refsChunk=newRefsChunk,
                repo=repoName,
                collection=collection,
                datasetType="allTypes",
                period=dateRange,
                index=1,
                maxItems=1
            )
            continue

    # this collection has too many files or contains 'raw'.
    # chop them to smaller groups
    for datasetType in datasetTypes:
        if datasetType == 'raw':
            continue
        for q in range(qSkip, len(quarters)-1):
            Q = q % 4 + 1
            year = quarters[q][0:4]
            where = f"ingest_date >= T'{quarters[q]}T00:00:00' and "
            if quarters[q+1] < cutoffDate:
                where += f"ingest_date < T'{quarters[q+1]}T00:00:00'"
            else:
                where += f"ingest_date < T'{cutoffDate}T00:00:00'"

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

            index = 1
            refsChunk = []
            for ref in refs:
                refsChunk.append(ref)
                if len(refsChunk) == 1000:
                    newRefsChunk = remove_refs_in_rucio(refsChunk, scope=rucioScope)
                    index = register_refs_to_rucio(
                        refsChunk=newRefsChunk,
                        repo=repoName,
                        collection=collection,
                        datasetType=datasetType,
                        period=f'{year}Q{Q}',
                        index=index,
                        maxItems=maxDIDperDataset
                    )
                    refsChunk = []
            newRefsChunk = remove_refs_in_rucio(refsChunk, scope=rucioScope)
            register_refs_to_rucio(
                refsChunk=newRefsChunk,
                repo=repoName,
                collection=collection,
                datasetType=datasetType,
                period=f'{year}Q{Q}',
                index=index,
                maxItems=1
            )
