#!/usr/bin/env python

import argparse
from tqdm import tqdm
import pandas as pd
import lsst.daf.butler as daf_butler

parser = argparse.ArgumentParser(
    description="Create a tagged collection of raw files from a dataframe of dataIds"
)
parser.add_argument("repo", type=str, help="Butler repository")
parser.add_argument("dataId_file", type=str, help="Parquet file of dataIds")
parser.add_argument("tagged_collection", type=str, help="Tagged collection name")
parser.add_argument(
    "--input_collection",
    type=str,
    default="LSSTCam/raw/all",
    help="Collection containing datasets to tag",
)
args = parser.parse_args()

collections = [args.input_collection]
butler = daf_butler.Butler(args.repo, collections=collections, writeable=True)

butler.registry.registerCollection(args.tagged_collection)

df0 = pd.read_parquet(args.dataId_file)
exposures = sorted(set(df0["exposure"]))
for exposure in tqdm(exposures):
    dets = ",".join(
        set([str(_) for _ in df0.query(f"exposure=={exposure}")["detector"]])
    )
    where = f"exposure={exposure} and detector in ({dets})"
    refs = butler.query_datasets("raw", where=where, limit=None)
    butler.registry.associate(args.tagged_collection, refs)
