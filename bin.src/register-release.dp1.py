# register data in DP1 butler to Rucio
#
import argparse
import hashlib
import logging
import random
import time
import zlib
from typing import Any

import rucio.common.exception
import sqlalchemy
from lsst.daf.butler import Butler, DatasetRef
from lsst.daf.butler.cli.cliLog import CliLog
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient


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
        "rse",
        type=str,
        help="Rucio RSE to register in.",
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
        "--njobs",
        type=int,
        required=False,
        help="Number of parallel jobs.",
    )
    parser.add_argument(
        "--jobnum",
        type=int,
        required=False,
        help="Job number.",
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


def retry(retry_label: str, func: Any, *args, **kwargs) -> Any:
    """Retry a database-dependent function call up to 10 times."""
    global logger

    retries = 0
    max_retries = 10
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except (
            sqlalchemy.exc.InterfaceError,
            sqlalchemy.exc.OperationalError,
            rucio.common.exception.RucioException,
        ) as e:
            retries += 1
            logger.warning(f"{retry_label} retry {retries}: {e}")
            time.sleep(random.uniform(2, 10))
    if retries >= max_retries:
        raise RuntimeError("Unable to communicate with database")


def getchecksum(path):
    size = 0
    md5 = hashlib.md5()
    adler32 = zlib.adler32(b"")
    if not config.dry_run:
        with open(path, "rb") as fd:
            while buf := fd.read(10 * 1024 * 1024):
                size += len(buf)
                md5.update(buf)
                adler32 = zlib.adler32(buf, adler32)
    md5_digest = md5.hexdigest()
    adler32_digest = f"{adler32:08x}"
    return size, adler32_digest, md5_digest


dsmap = dict()


def map_to_rucio(ref: DatasetRef) -> str:
    global dsmap

    dstype = ref.datasetType.name
    dims = ref.datasetType.dimensions
    data_id = ref.dataId
    massive = "tract" in dims and "visit" in dims

    if (dstype, massive) in dsmap:
        base = dsmap[(dstype, massive)]
    elif dstype.endswith("_config") or dstype in ("skyMap"):
        base = dsmap[(dstype, massive)] = "Configuration"
    elif dstype.endswith("_log"):
        if massive:
            base = dsmap[(dstype, massive)] = "Provenance/" + dstype.removesuffix(
                "_log"
            )
        else:
            base = dsmap[(dstype, massive)] = "Provenance"
    elif dstype.endswith("_metadata"):
        if massive:
            base = dsmap[(dstype, massive)] = "Provenance/" + dstype.removesuffix(
                "_metadata"
            )
        else:
            base = dsmap[(dstype, massive)] = "Provenance"
    elif "/calib/" in ref.run:
        base = dsmap[(dstype, massive)] = "Calibration"
    elif "_consolidated_map_" in dstype:
        base = dsmap[(dstype, massive)] = "Map"
    elif "_image" in dstype or "_coadd" in dstype or "_background" in dstype:
        base = dsmap[(dstype, massive)] = "Image/" + dstype
    elif (
        "object" in dstype
        or "source" in dstype
        or "table" in dstype
        or "summary" in dstype
    ):
        base = dsmap[(dstype, massive)] = "Catalog/" + dstype
    elif dstype.startswith("the_monster_"):
        base = dsmap[(dstype, massive)] = "ReferenceCatalog"
    else:
        base = dsmap[(dstype, massive)] = dstype

    if "tract" in dims:
        if dstype in ("dia_object", "dia_source", "object"):
            rucio_dataset = base
        else:
            tract = data_id["tract"]
            rucio_dataset = f"{base}/Tract{tract}"
    elif "visit" in dims:
        day_obs = data_id["day_obs"]
        if "detector" in dims:
            partition = data_id["visit"] // 100 % 1000
            rucio_dataset = f"{base}/{day_obs}/{partition:03d}"
        else:
            rucio_dataset = f"{base}/{day_obs}"
    else:
        rucio_dataset = base
    return "Dataset/" + rucio_dataset


config = parse_args()
# Initialize the logger and set the level
CliLog.initLog(longlog=True)
CliLog.setLogLevels(logLevels=[(None, config.log)])
logger = logging.getLogger("lsst.rucio.register.release")
logger.info("log level %s", config.log)
logger.info("config: %s", config)
if not config.dry_run:
    replica_client = ReplicaClient()
    did_client = DIDClient()
butler = Butler(config.repo)
root = butler._datastore.root

n_files = dict()
n_bytes = dict()
rucio_datasets = dict()

dataset_type_list = sorted(
    retry("DSType query", butler.registry.queryDatasetTypes, config.dstype)
)
for i, dstype in enumerate(dataset_type_list):
    # Known dataset types ingested with -t direct.
    if (
        dstype.name == "raw"
        or dstype.name == "guider_raw"
        or dstype.name.startswith("the_monster_")
    ):
        continue

    logger.info(f"Handling dataset type {dstype}: {i}/{len(dataset_type_list)}")

    files = []
    # pathmap maps rel_path to os path
    pathmap = {}
    ref_list = sorted(
        retry(
            f"DSRef query {dstype.name}",
            butler.query_datasets,
            dstype,
            collections=config.collection,
            find_first=False,
            limit=None,
            explain=False,
        )
    )
    for j, ref in enumerate(ref_list):
        if config.njobs and j % config.njobs != config.jobnum:
            continue

        path = butler.getURI(ref)
        rel_path = path.relative_to(root)
        if not rel_path:
            rel_path = path.relative_to(rse_root)
            if not rel_path:
                logger.info(f"Skipping {path}")
                continue
        path = path.ospath
        pathmap[rel_path] = path

        rucio_dataset = map_to_rucio(ref)

        # If the Dataset is new, register it, and set up dict entries.
        if rucio_dataset not in rucio_datasets:
            try:
                if not config.dry_run:
                    _ = did_client.get_did(config.scope, rucio_dataset)
            except rucio.common.exception.DataIdentifierNotFound:
                try:
                    logger.info(f"Creating dataset {config.scope}:{rucio_dataset}")
                    if not config.dry_run:
                        did_client.add_dataset(
                            config.scope,
                            rucio_dataset,
                            statuses={"monotonic": True},
                            rse=config.rse,
                        )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    pass
            rucio_datasets[rucio_dataset] = []
            n_files[rucio_dataset] = 0
            n_bytes[rucio_dataset] = 0

        # Define the Rucio DID for the file.
        did = dict(
            name=rel_path,
            scope=config.scope,
        )

        # Do batched addition of replicas.
        files.append(did)
        if len(files) >= 500:
            if not config.dry_run:
                present = replica_client.list_replicas(
                    files,
                    rse_expression=config.rse,
                )
                existing_names = {replica["name"] for replica in present}
                files = [did for did in files if did["name"] not in existing_names]
                for did in files:
                    did["bytes"], did["adler32"], did["md5"] = getchecksum(pathmap[did["name"]])
                if len(files) > 0:
                    retry(
                        "add replicas",
                        replica_client.add_replicas,
                        rse=config.rse,
                        files=files,
                    )
            files = []
            pathmap = {}

        # Do batched addition of files to Datasets.
        n_files[rucio_dataset] += 1
        rucio_datasets[rucio_dataset].append(did)
        if len(rucio_datasets[rucio_dataset]) >= 500:
            if not config.dry_run:
                present = did_client.list_content(
                    scope=config.scope, name=rucio_dataset
                )
                existing_names = {did["name"] for did in present}
                needed = [
                    did
                    for did in rucio_datasets[rucio_dataset]
                    if did["name"] not in existing_names
                ]
                if len(needed) > 0:
                    retry(
                        f"add files to {rucio_dataset}",
                        did_client.add_files_to_dataset,
                        scope=config.scope,
                        name=rucio_dataset,
                        files=needed,
                        rse=config.rse,
                    )
            rucio_datasets[rucio_dataset] = []

    # Finish any partial batches.
    if files:
        if not config.dry_run:
            present = replica_client.list_replicas(
                files,
                rse_expression=config.rse,
            )
            existing_names = {replica["name"] for replica in present}
            files = [did for did in files if did["name"] not in existing_names]
            for did in files:
                did["bytes"], did["adler32"], did["md5"] = getchecksum(pathmap[did["name"]])
            if len(files) > 0:
                retry("add replicas", replica_client.add_replicas, rse=config.rse, files=files)
        files = []

    for rucio_dataset_i in rucio_datasets:
        if rucio_datasets[rucio_dataset_i]:
            if not config.dry_run:
                present = did_client.list_content(scope=config.scope, name=rucio_dataset_i)
                existing_names = {did["name"] for did in present}
                needed = [
                    did
                    for did in rucio_datasets[rucio_dataset_i]
                    if did["name"] not in existing_names
                ]
                if len(needed) > 0:
                    retry(
                        f"add files to {rucio_dataset_i}",
                        did_client.add_files_to_dataset,
                        scope=config.scope,
                        name=rucio_dataset_i,
                        files=needed,
                        rse=config.rse,
                    )
            rucio_datasets[rucio_dataset_i] = []

for rucio_dataset in rucio_datasets:
    logger.info(f"{rucio_dataset} has {n_files[rucio_dataset]} files")
#    if config.njobs is not None and config.njobs <= 1:
#        # And close out Datasets, including marking them for tape.
#        did_client.close(config.scope, rucio_dataset)
#        did_client.set_metadata(config.scope, rucio_dataset, "arcBackup", "SLAC_RAW_DISK_BKUP:need")
