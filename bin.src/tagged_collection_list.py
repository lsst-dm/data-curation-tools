#!/usr/bin/env python

import argparse
from collections import defaultdict
import pandas as pd
import lsst.daf.butler as daf_butler

parser = argparse.ArgumentParser(
    description="Create a dataframe of dataIds for a tagged collection"
)
parser.add_argument("repo", type=str, help="Butler repository")
parser.add_argument("tagged_collection", type=str, help="Tagged collection name")
parser.add_argument(
    "--dataset_type",
    type=str,
    default="raw",
    help="Dataset type to consider.  Default: 'raw'.",
)
parser.add_argument("--output_file", type=str, default=None, help="Output parquet file")
args = parser.parse_args()

collection = args.tagged_collection

butler = daf_butler.Butler(args.repo)

refs = butler.query_datasets(
    args.dataset_type, collections=[args.tagged_collection], limit=None
)

data = defaultdict(list)
for ref in refs:
    dataId = ref.dataId.to_simple().model_dump()["dataId"]
    for k, v in dataId.items():
        data[k].append(v)

df0 = pd.DataFrame(data)
if args.output_file is None:
    outfile = f"{collection.replace('/', '_')}_dataIds.parquet"
else:
    outfile = args.output_file

df0.to_parquet(outfile)
