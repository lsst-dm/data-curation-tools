#!/usr/bin/env python
"""
Final job script to zip and rucio-register selected dataset types.
"""

import os
import glob
import argparse
import logging
import warnings
import tempfile
import subprocess
import yaml
import lsst.daf.butler as daf_butler
from lsst.pipe.base import QuantumGraph
from lsst.utils.logging import getLogger


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("qgraph_file", type=str, help="Quantum Graph file")
    parser.add_argument("repo", type=str, help="Butler repo")
    args = parser.parse_args()
    return args


def get_dstypes(qgraph):
    """
    Read in dataset types from the Quantum Graph and from the zip
    config file and determine which dstpyes to zip and not to zip
    """
    # Read in dataset types to exclude from zipping.
    zip_config_file = os.environ["ZIP_DSTYPE_CONFIG"]
    with open(zip_config_file) as fobj:
        zip_exclude = set(yaml.safe_load(fobj)["zip_exclude"])

    # Get the dataset types of the QG output refs.
    dstypes = set()
    output_refs, _ = qgraph.get_refs(
        include_outputs=True, include_init_outputs=True, conform_outputs=True
    )
    for ref in output_refs:
        dstypes.add(ref.datasetType.name)

    # Make lists of dstypes to zip and not to zip.
    to_zip = sorted(dstypes.difference(zip_exclude))
    not_to_zip = sorted(dstypes.intersection(zip_exclude))
    return to_zip, not_to_zip


def get_zip_file_locations(repo, qgraph, dstypes):
    """
    Query the butler to get a dict of on-disk locations for ingested
    zip files, keyed by dataset type.
    """
    butler = daf_butler.Butler(repo)
    run_collection = qgraph.metadata["output_run"]
    zip_file_locations = {}
    try:
        butler.registry.queryCollections(run_collection)
    except daf_butler.registry.MissingCollectionError:
        # No zip files for the run collection have been ingested, so
        # return the empty dict.
        return zip_file_locations
    for dstype in dstypes:
        ref = butler.query_datasets(dstype, limit=1, collections=[run_collection])[0]
        zip_file_locations[dstype] = butler.getURI(ref).path
    return zip_file_locations


def process_datasets(to_zip, qgraph, qgraph_file, butler_config, logger):
    """
    Process and register all datasets:
    * In case of a retry, check for existing zip files to skip.
    * Create the zip files for the selected dataset types.
    * Register the newly zipped datasets with the butler.
    * Register any unzipped datasets.
    """
    # For retries, skip any dataset types that have already been
    # zipped and ingested.
    existing_zip_files = get_zip_file_locations(butler_config, qgraph, to_zip)

    butler_exe = os.environ["DAF_BUTLER_DIR"] + "/bin/butler"
    with tempfile.TemporaryDirectory() as zip_tmp_dir:
        # Zip the files for each dataset type individually.
        for dstype_to_zip in to_zip:
            if dstype_to_zip in existing_zip_files:
                logger.info(
                    f"Files for dataset type {dstype_to_zip} "
                    "have already been zipped, so skipping."
                )
                continue
            logger.info(f"Zipping dataset type: {dstype_to_zip}")
            zip_from_graph = [
                butler_exe,
                "--long-log",
                "--log-level=VERBOSE",
                "zip-from-graph",
                "--dataset-type",
                dstype_to_zip,
                qgraph_file,
                butler_config,
                zip_tmp_dir,
            ]
            subprocess.check_call(zip_from_graph)

        # Ingest the zip files.
        zip_files = glob.glob(f"{zip_tmp_dir}/*.zip")
        for zip_file in zip_files:
            logger.info(f"Ingesting zip file: {zip_file}")
            ingest_zip = [
                butler_exe,
                "--long-log",
                "--log-level=VERBOSE",
                "ingest-zip",
                "--transfer",
                "move",
                butler_config,
                zip_file,
            ]
            subprocess.check_call(ingest_zip)

        # Transfer any remaining dataset types directly to the
        # destination repo.  This should also pick up any dataset
        # types produced by a bps restart, including those that were
        # previously zipped.
        logger.info("Running `butler transfer-from-graph`")
        transfer_from_graph = [
            butler_exe,
            "--long-log",
            "--log-level=VERBOSE",
            "transfer-from-graph",
            qgraph_file,
            butler_config,
            "--register-dataset-types",
            "--update-output-chain",
        ]
        subprocess.check_call(transfer_from_graph)


def run_rucio_register(to_zip, not_to_zip, qgraph, butler_config, logger):
    """
    Run rucio-register for zipped and unzipped datasets.
    """
    # TODO: This function will not handle files for dataset types
    # intended for zipping that were generated from a bps restart.
    rucio_register_config = os.environ.get("RUCIO_REGISTER_CONFIG", None)

    if rucio_register_config is None:
        warnings.warn("RUCIO_REGISTER_CONFIG not set, skipping rucio-register")
        return

    # Rucio register each zip file.
    run_collection = qgraph.metadata["output_run"]
    parent_collection = os.path.dirname(run_collection)
    rucio_dataset = os.environ.get(
        "RUCIO_DATASET", f"Dataset/{parent_collection}/zip_test"
    )
    chunk_size = "30"

    zip_file_locations = get_zip_file_locations(butler_config, qgraph, to_zip)
    for dstype, zip_file in zip_file_locations.items():
        if not os.path.isfile(zip_file):
            continue
        logger.info(f"Running `rucio-register zips` for {dstype}, {zip_file}")
        rucio_register = [
            "rucio-register",
            "zips",
            "--log-level=VERBOSE",
            "--rucio-dataset",
            rucio_dataset,
            "--zip-file",
            zip_file,
            "--chunk-size",
            chunk_size,
            "--rucio-register-config",
            rucio_register_config,
        ]
        subprocess.check_call(rucio_register)

    # Rucio register the non-zip files.
    for dstype in not_to_zip:
        logger.info(f"Running `rucio-register data-products` for {dstype}")
        rucio_register = [
            "rucio-register",
            "data-products",
            "--rucio-dataset",
            rucio_dataset,
            "--dataset-type",
            dstype,
            "--collections",
            qgraph.metadata["output_run"],
            "--repo",
            butler_config,
            "--chunk-size",
            chunk_size,
            "--rucio-register-config",
            rucio_register_config,
        ]
        subprocess.check_call(rucio_register)


def main():
    """Main function for final_job_zip."""
    logger = getLogger("final_job_zip")
    logging.basicConfig(
        level=logger.VERBOSE,
        format="%(asctime)s   %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = parse_args()
    qgraph_file = args.qgraph_file
    butler_config = args.repo

    qgraph = QuantumGraph.loadUri(qgraph_file)
    to_zip, not_to_zip = get_dstypes(qgraph)

    process_datasets(to_zip, qgraph, qgraph_file, butler_config, logger)
    run_rucio_register(to_zip, not_to_zip, qgraph, butler_config, logger)


if __name__ == "__main__":
    main()
