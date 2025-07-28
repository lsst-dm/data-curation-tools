#!/usr/bin/env python

import argparse
import fnmatch
from lsst.daf.butler import Butler
from lsst.daf.butler.cli.cliLog import CliLog
import logging
import time
import yaml


def parse_args():
    parser = argparse.ArgumentParser(prog='run_pruning',
                                     description='Prunes datasets')

    parser.add_argument('--collection',
                        type=str,
                        help="Collection name to use when querying"
                        " datasets. Ignored if --config is passed.")
    parser.add_argument('--config', type=str,
                        help="Optional configuration yaml file."
                        " Takes precedence over other command-line"
                        " arguments regarding configuration such as"
                        " --collection, --dataset-types, or --where.")
    parser.add_argument('--dry_run', action="store_true")
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--chunk_size', type=int)
    parser.add_argument('--where', type=str,
                        help="Argument to pass to the where"
                        " option in query_datasets."
                        " Ignored if --config is passed.")
    parser.add_argument('--dataset-types', type=str,
                        help="List of comma-separated dataset types to prune."
                        " Ignored if --config is passed.")
    parser.add_argument('--repo', type=str,
                        help="Butler repo name/path",
                        default="embargo")
    args = parser.parse_args()
    if args.dataset_types:
        args.dataset_types = args.dataset_types.split(",")
    return args


def main():
    CliLog.initLog(longlog=True)
    logger = logging.getLogger("lsst.run.pruning")
    CliLog.setLogLevels(logLevels=[(None, "INFO")])
    logger.setLevel(logging.INFO)
    repo = "embargo"
    dry_run = False
    # Some reasonable default
    chunk_size = 5000
    args = parse_args()
    if args.dry_run:
        dry_run = True
    if args.debug:
        debug = args.debug
        CliLog.setLogLevels(logLevels=[(None, "DEBUG")])
        logger.setLevel(logging.DEBUG)
    else:
        debug = False
    if args.chunk_size:
        chunk_size = args.chunk_size

    # Instantiate the Butler as writeable or not
    writeable = True
    if dry_run:
        writeable = False
    if args.repo:
        repo = args.repo

    butler = Butler(repo, writeable=writeable)
    if args.config:
        with open(args.config) as f:
            yamls = yaml.safe_load(f)
        f.close()
        for yml in yamls:
            # do stuff for each iteration
            prune(butler,
                  collection=yml["collection"],
                  where=yml["where"],
                  types_to_prune=yml["dataset_types"],
                  dry_run=dry_run,
                  chunk_size=chunk_size,
                  debug=debug,
                  logger=logger)
    else:
        prune(butler,
              collection=args.collection,
              where=args.where,
              types_to_prune=args.dataset_types,
              dry_run=dry_run,
              chunk_size=chunk_size,
              debug=debug,
              logger=logger)


def prune(butler, collection,
          where=None,
          types_to_prune=None,
          chunk_size=10000,
          dry_run=False,
          debug=False,
          logger=None):
    if collection is None:
        raise ValueError("Empty collection name; cannot proceed.")
    if where is None:
        raise ValueError("You need to provide a valid WHERE query."
                         " It could be as simple as the instrument name.")
    if types_to_prune is None:
        raise ValueError("You did not provide a list of dataset types to prune.")
    # sorted list of dataset types
    dataset_types = sorted(butler.collections.get_info(collection,
                                                       include_summary=True).dataset_types)
    if debug:
        logger.debug(f"Dataset types list: {dataset_types}")

    """
    Final list of dataset types to remove; starts out empty,
    then grows when a member of dataset_types matches one
    of the removal names or patterns in types_to_prune.
    """
    prune_list = []

    for itype in dataset_types:
        if not any(fnmatch.filter(types_to_prune, itype)):
            if debug:
                logger.debug(f"dataset_type {itype} does not match "
                             "any removal pattern; do not remove.")
        else:
            if debug:
                logger.debug(f"found {itype}")
            prune_list.append(itype)
    if debug:
        logger.debug(f"List of types to prune: {prune_list}")

    dataset_refs = []

    # Some reasonable default
    where_query = "instrument='LSSTCam'"
    if where is not None:
        where_query = where
    if debug:
        logger.debug("where = " + where_query)
    for dset in prune_list:
        dataset_refs += butler.query_datasets(dataset_type=dset,
                                              collections=collection,
                                              where=where_query,
                                              find_first=False,
                                              explain=False,
                                              limit=None,
                                              )
    dataset_refs.sort()
    chunked_refs = [dataset_refs[i:i+chunk_size] for i in
                    range(0, len(dataset_refs), chunk_size)]

    if dry_run:
        logger.info("Found " + str(len(dataset_refs)) + " datset refs to prune; stopping here.")
    else:
        if debug:
            logger.debug("Found " + str(len(dataset_refs)) + " datset refs to prune.")
        tstart = time.time()
        for chunk in chunked_refs:
            prune_result = butler.pruneDatasets(chunk,
                                                unstore=True,
                                                purge=True)
            if debug:
                logger.debug("Finished a chunk.", flush=True)
                logger.debug(prune_result, flush=True)
        tend = time.time()
        elapsed = tend - tstart
        logger.info(f"Pruning operation finished in {elapsed} seconds.")


if __name__ == '__main__':
    main()
