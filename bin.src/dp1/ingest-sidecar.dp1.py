import argparse
import logging
import random
import time
from typing import Any

import rucio.common.exception
import sqlalchemy
from lsst.daf.butler import Butler, DatasetRef
from lsst.daf.butler.cli.cliLog import CliLog
from rucio.client.didclient import DIDClient

import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Register all datasets in a collection into Rucio."
    )
    parser.add_argument(
        "repo",
        type=str,
        help="Butler repository to query.",
    )
    parser.add_argument(
        "collection",
        type=str,
        help="Collection to query.",
    )
    parser.add_argument(
        "scope",
        type=str,
        help="Rucio scope to register in.",
    )
    parser.add_argument(
        "--dstype",
        type=str,
        default="*",
        help="Dataset type expression (default=*)",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Don't checksum or register any files.",
    )
    parser.add_argument(
        "--log",
        type=str,
        required=False,
        default="INFO",
        help="Log level (default = INFO).",
    )

    ns = parser.parse_args()
    return ns


config = parse_args()
# Initialize the logger and set the level
CliLog.initLog(longlog=True)
CliLog.setLogLevels(logLevels=[(None, config.log)])
logger = logging.getLogger("lsst.rucio.register.release")
logger.info("log level %s", config.log)
logger.info("config: %s", config)
if not config.dry_run:
    did_client = DIDClient()
butler = Butler(config.repo)
root = butler._datastore.root

n_files = dict()
rucio_datasets = dict()

dataset_type_list = sorted(butler.registry.queryDatasetTypes(config.dstype))

nfiles_total = 0
nfiles_added = 0
for i, dstype in enumerate(dataset_type_list):
    # Known dataset types ingested with -t direct.
    if (
        dstype.name == "raw"
        or dstype.name == "guider_raw"
        or dstype.name.startswith("the_monster_")
    ):
        continue

    # logger.info(f"Handling dataset type {dstype}: {i}/{len(dataset_type_list)}")

    files = []
    ref_list = sorted(butler.query_datasets(dstype,
                                            collections=config.collection,
                                            find_first=False,
                                            limit=None,
                                            explain=False
                                           ))
 
    for j, ref in enumerate(ref_list):
        nfiles_total += 1
        path = butler.getURI(ref)
        rel_path = path.relative_to(root)
        if not rel_path:
            # rel_path = path.relative_to(rse_root)
            # if not rel_path:
            #     logger.info(f"Skipping {path}")
            #     continue
            logger.info(f"Skipping {path}")
            continue
        path = path.ospath

        # Define the Rucio DID for the file.
        metadata = {"rubin_butler": "data_product", "rubin_sidecar": ref.to_json()}
        did = dict(
            scope=config.scope,
            name=rel_path,
            meta=metadata
        )

        # Do batched addition of replicas.
        files.append(did)

        if len(files) >= 500 or j == len(ref_list)-1:
            nfiles_added += len(files) 
            did_client.set_dids_metadata_bulk(files)
            # logger.info(f"add did metadata: files: {len(files)} dstype: {dstype.name}")
            files = []

        if nfiles_total % 1000 == 0:
            logger.info(f"PerfMark: {nfiles_total} processed, {nfiles_added} added to rucio")

    logger.info(f"Summary: {nfiles_total} processed, {nfiles_added} added to rucio")
    # if nfiles_total > 2000:
    #     sys.exit(1)
