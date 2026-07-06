#!/usr/bin/env python

from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from botocore.config import Config
from cryptography.x509 import load_pem_x509_certificate
import os
import time
import datetime
import argparse
import threading
import concurrent.futures
import requests
import zipfile
import dotenv
import hashlib

import boto3
import urllib3

urllib3.disable_warnings()

# print call stack during crash
# import faulthandler
# faulthandler.enable()

# Constants:
RucioAttrTapeBkupKey = "arcBackup"
RucioAttrTapeBkupDone = "SLAC_RAW_DISK_BKUP:done"
RucioAttrSafeCopies = "SafeCopies"

# Global variables
global config, DIDclient, ReplicaClient, taskQueue, taskQueueLock, msgLock, threadExecutor, embargoS3
global totalDatasetChecked
global etagpartsizes
# not sure if rucio operation is thread safe
global rucioOprLock


def get_args() -> dict:
    """
    parse command line args
    """
    parser = argparse.ArgumentParser(
        description="Find all datesets matching their selection criteria. "
        + "Mark the status of the copies with \u2705 or \u274c",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--stdin",
        action="store_true",
        help="read the dataset list from stdin. (--age option is ignored)",
        default=False,
    )

    group.add_argument(
        "--dsprefix",
        type=str,
        help="prefix of a dataset: e.g. raw:Dataset/LSSTCam/raw/Obs/202508",
        default="raw:Dataset/LSSTCam/raw/Obs/NONE",
    )
    parser.add_argument(
        "--age",
        type=int,
        help="age (days) of datasets since last update",
        default=30,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print out detail info of the validation process. This will set the parallelism to 1",
        default=False,
    )
    args = parser.parse_args()
    if args.stdin:
        updated_at = datetime.datetime.now() - datetime.timedelta(20000)
        return {"stdin": True, "updated_at": updated_at, "verbose": args.verbose}
    else:
        scope, nameprefix = args.dsprefix.split(":")
        updated_at = datetime.datetime.now() - datetime.timedelta(days=args.age)
        return {
            "stdin": False,
            "scope": scope,
            "nameprefix": nameprefix,
            "updated_at": updated_at,
            "verbose": args.verbose,
        }


def checkTRSEcopy(m: dict) -> bool:
    """
    check Tape copy status. Currently just check the
    Rucio <RucioAttrTapeBkupKey> attribute
    """

    if (
        RucioAttrTapeBkupKey in m.keys()
        and m[RucioAttrTapeBkupKey] == RucioAttrTapeBkupDone
    ):
        return True
    else:
        return False


def configureAuthenticatedSession():
    """
    Generate a new session object for use with requests to the issuer.

    Configures TLS appropriately to work with a GSI environment.
    """
    euid = os.geteuid()
    if euid == 0:
        cert = "/etc/grid-security/hostcert.pem"
        key = "/etc/grid-security/hostkey.pem"
    else:
        cert = "/tmp/x509up_u%d" % euid
        key = "/tmp/x509up_u%d" % euid

    cert = os.environ.get("X509_USER_PROXY", cert)
    key = os.environ.get("X509_USER_PROXY", key)

    session = requests.Session()

    if os.path.exists(cert):
        session.cert = cert
    if os.path.exists(key):
        session.cert = (cert, key)
    # session.verify = '/etc/grid-security/certificates'
    session.verify = os.environ.get("X509_CERT_DIR", "/etc/grid-security/certificates")

    return session


def queryChecksum(url: str):
    """
    query adler32 checksum
    """
    url = url.replace("davs://", "https://", 1)
    with configureAuthenticatedSession() as session:
        response = session.head(url, headers={"Want-Digest": "adler32"})

    if response.status_code == requests.codes.ok:
        return response.headers["Digest"].split("=")[1]
    else:
        return "xxxxxxxx"


def checkFrDFcopy(m: dict, dids: dict) -> bool:
    """
    deep check the dataset at FrDF: query the adler32 of files in the dataset
    """
    FrDFRSE = "IN2P3_RAW_DISK"
    hasCopy = False
    """
    there are cases when 'rucio replica list dataset <ds>' returns
    DATASET: raw:Dataset/LSSTCam/raw/Obs/20251101/MC_O_20251101_000011
    +----------------+---------+---------+
    | RSE            |   FOUND |   TOTAL |
    |----------------+---------+---------|
    | SLAC_RAW_DISK  |       2 |       2 |
    | IN2P3_RAW_DISK |       0 |       2 |
    +----------------+---------+---------+
    """
    with rucioOprLock:
        dsreplicas = ReplicaClient.list_dataset_replicas(
            scope=m["scope"], name=m["name"], deep=False
        )
        for x in dsreplicas:
            if x["rse"] == FrDFRSE and x["length"] == x["available_length"]:
                hasCopy = True
                break

    if not hasCopy:
        if config["verbose"]:
            with msgLock:
                print(f'  \U0001f7e1 {FrDFRSE} does not have dataset', end="")
                print(f' {m["scope"]}:{m["name"]} (according to Rucio)')
        else:
            return False

    if config["verbose"]:
        with msgLock:
            print(
                f'  \U0001f7e1 Deep check dataset {m["scope"]}:{m["name"]} at {FrDFRSE}'
            )

    # if config["verbose"]:
    #   with msgLock:
    #    print(f'    \U0001F7E1 This dataset contains', end="")
    #    print(f' {len(dids)} logical files:')

    replicas = []
    with rucioOprLock:
        xdids = dids.copy()
        while len(xdids) > 0:
            xReplicas = ReplicaClient.list_replicas(
                dids=xdids[:500], schemes=["davs"], rse_expression=FrDFRSE
            )
            for x in xReplicas:
                replicas.append(x)
            xdids = xdids[500:]

    for x in replicas:
        if config["verbose"]:
            if FrDFRSE in x["rses"].keys():
                with msgLock:
                    print(
                        f'      replica for {x["scope"]}:{x["name"]} exists. ', end=""
                    )
            else:
                with msgLock:
                    print(f'      replica for {x["scope"]}:{x["name"]} do not exist.')
                continue

        pfn = x["rses"][FrDFRSE][0]
        if x["adler32"] != queryChecksum(pfn):
            if config["verbose"]:
                with msgLock:
                    print("Checksum \u274c. Skip the rest!")
            return False
        else:
            if config["verbose"]:
                with msgLock:
                    print("Checksum \u2705")

    if len(dids) != len(replicas):
        return False

    lfnset = set()
    pfnset = set()
    for x in dids:
        lfnset.add(f'{x["scope"]}:{x["name"]}')
    for x in replicas:
        pfnset.add(f'{x["scope"]}:{x["name"]}')

    if lfnset != pfnset:
        if config["verbose"]:
            with msgLock:
                print(
                    f'content of dataset {m["scope"]}:{m["name"]} is not fully replicated to {FrDFRSE}'
                )
        return False
    # else:
    #    print("lfnset vs pfnset")
    #    print(lfnset)
    #    print(pfnset)

    if hasCopy:
        return True
    else:
        return False


def checkUSDFcopy(m: dict, dids: dict) -> bool:
    """
    Deep check the USDF copy, by re-calculate etag of .zip file members
    and compare that with those in embargo
    """
    # global etagpartsizes, embargoS3
    USDFRSE = "SLAC_RAW_DISK"

    # USDFRSE uses lfn2pfn_algorithem = 'identity'
    storageprefixmap = {
        "weka": "/sdf/data/rubin/rses/lsst/rawdisk",
        "embargo": {"bucket": "rubin-summit"},
    }

    if config["verbose"]:
        with msgLock:
            print(
                f'  \U0001f7e1 Deep check dataset {m["scope"]}:{m["name"]} at {USDFRSE}'
            )

    for did in dids:
        name = did["name"]
        scope = did["scope"]
        if not name.endswith(".zip"):
            continue
        s3items = {}
        paginator = embargoS3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=storageprefixmap["embargo"]["bucket"],
            Prefix=name[:-4],
            PaginationConfig={"PageSize": 1000},
        )
        for page in page_iterator:
            if "Contents" in page:
                for x in page["Contents"]:
                    s3items[x["Key"]] = {
                        "Size": x["Size"],
                        "ETag": x["ETag"].replace('"', ""),
                    }
        # A short keys is the last component of
        # LSSTCam/20251231/MC_O_.../MC_O_20251103_000150_R21_S12.json
        s3itemshortkeys = set()
        for i in s3items.keys():
            ii = i.split("/")
            s3itemshortkeys.add(ii[-1])

        wekapfn = f'{storageprefixmap["weka"]}/{scope}/{name}'
        with zipfile.ZipFile(wekapfn, mode="r") as myzip:
            zipitems = myzip.namelist()

            if len(s3itemshortkeys - set(zipitems)) != 0:
                if config["verbose"]:
                    with msgLock:
                        print(
                            "      Please check missing/extra items in the .zip: ",
                            end="",
                        )
                        print(s3itemshortkeys - set(zipitems))
                return False

            for zipitem in zipitems:
                key = name[:-4] + "/" + zipitem
                if key not in s3items.keys():
                    continue
                with myzip.open(zipitem) as f:
                    if s3items[key]["ETag"].find("-") == -1:
                        nparts = 1
                    else:
                        nparts = s3items[key]["ETag"].split("-")[1]

                    agvpartsize = int(s3items[key]["Size"]) / int(nparts)
                    partsize = s3items[key]["Size"]
                    for i in etagpartsizes:
                        if agvpartsize <= i:
                            partsize = i
                            break

                    multipart_md5 = hashlib.md5()
                    parts = 0
                    while buf := f.read(partsize):
                        parts += 1
                        md5 = hashlib.md5()
                        md5.update(buf)
                        multipart_md5.update(md5.digest())
                    if parts == 1:
                        etag = f"{md5.hexdigest()}"
                    elif parts == nparts:
                        etag = f"{multipart_md5.hexdigest()}-{parts}"
                    else:
                        etag = "unknown"

                    if etag == s3items[key]["ETag"]:
                        if config["verbose"]:
                            with msgLock:
                                print(f"      {zipitem} @ {scope}:{name} \u2705")
                    else:
                        if config["verbose"]:
                            with msgLock:
                                print(
                                    f"      {zipitem} @ {scope}:{name} ETag mismatch \u274c. Skip the rest!"
                                )
                        return False
    return True


def checkAll(m: dict):
    """
    check everything
    """
    msg = f'\U0001f539 {m["scope"]}:{m["name"]} '

    state = {"USDF": 0, "TRSE": 0, "FrDF": 0}
    dids = []
    with rucioOprLock:
        # Combine metadata from DIDCOLUMN and JSON. Note:
        # DIDclient.get_metadata_bulk() only returns metadata in DIDCOLUMN.
        jsonMetadata = DIDclient.get_metadata(
            scope=m["scope"], name=m["name"], plugin="JSON"
        )
        m = m | jsonMetadata

        if not config["stdin"] and RucioAttrSafeCopies in m.keys():
            if m[RucioAttrSafeCopies] == ",".join(state.keys()):
                pass
                # return

        files = DIDclient.list_content(scope=m["scope"], name=m["name"])
        for file in files:
            dids.append({"scope": file["scope"], "name": file["name"]})

    if RucioAttrSafeCopies in m.keys():
        for x in m[RucioAttrSafeCopies].split(","):
            state[x] = 1

    # A raw Obs dataset has at least two files: a *.zip and a *_dimensions.yaml
    # it is also possible to have a *_dimension.1.yaml as described in DM-52779
    # if len(dids) == 2 and ((dids[0]['name'].endswith(".zip") and
    #                        dids[1]['name'].endswith("_dimensions.yaml")) or
    #                       (dids[1]['name'].endswith(".zip") and
    #                        dids[0]['name'].endswith("_dimensions.yaml"))):
    if any(".zip" in s["name"] for s in dids) and any(
        "_dimensions.yaml" in s["name"] for s in dids
    ):
        if config["verbose"]:
            msg += "has .zip and .yaml, "

        if state["USDF"] == 1 or checkUSDFcopy(m, dids):
            msg += "USDF \u2705 "
            state["USDF"] = 1
        else:
            state["USDF"] = 0
            msg += "USDF \u274c "
    else:
        if config["verbose"]:
            msg += "missing .zip or .yaml, "
        state["USDF"] = 0
        msg += "USDF \u274c "

    if state["TRSE"] == 1 or checkTRSEcopy(m):
        msg += "TRSE \u2705 "  # green check
        state["TRSE"] = 1
    else:
        msg += "Tape \u274c "  # red cross

    if state["FrDF"] == 1 or checkFrDFcopy(m, dids):
        msg += "FrDF \u2705 "
        state["FrDF"] = 1
    else:
        msg += "FrDF \u274c "

    safeCopyValue = ""
    for x in state.keys():
        if state[x] == 1:
            safeCopyValue += f"{x},"

    with rucioOprLock:
        DIDclient.set_metadata(
            scope=m["scope"],
            name=m["name"],
            key=RucioAttrSafeCopies,
            value=safeCopyValue[:-1],
        )

    with msgLock:
        print(msg)

    return


def embargoS3init():
    """
    initialize the s3 connction to the embargo
    """
    global embargoS3
    o = urllib3.util.parse_url("https://sdfembs3.sdf.slac.stanford.edu")
    endPointUrl = o.scheme + "://" + o.host
    if o.port is not None:
        endPointUrl += ":" + str(o.port)
    if o.scheme == "https":
        useSSL = True
    else:
        useSSL = False

    embargoS3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=endPointUrl,
        use_ssl=useSSL,
        verify=False,
        config=Config(),
    )


def addTask2Queue(mdata: dict):
    """
    Add tasks to the "taskQueue".
    """
    global totalDatasetChecked
    for m in mdata:
        if m["name"] == "TheEnd":
            # print("adding 1 TheEnd task ...")
            with taskQueueLock:
                taskQueue.append(m)
            continue

        if (
            not config["stdin"]
            and m["updated_at"].timestamp() < config["updated_at"].timestamp()
        ):
            continue

        totalDatasetChecked += 1
        with taskQueueLock:
            taskQueue.append(m)


def runTask():
    """
    pick up a task from the taskQueue and process it, by a thread worker
    """
    while 1:
        try:
            with taskQueueLock:
                m = taskQueue.pop(0)
            if m["name"] == "TheEnd":
                # print("end of thread")
                return
            checkAll(m)
        except Exception:
            # print("thread is waiting for tasks...")
            # with msgLock:
            #    print(e)
            time.sleep(1)
    return


def checkX509proxy():
    """
    verify that a valid x509 proxy exist
    """
    euid = os.geteuid()
    cert = "/tmp/x509up_u%d" % euid
    key = "/tmp/x509up_u%d" % euid

    cert = os.environ.get("X509_USER_PROXY", cert)
    key = os.environ.get("X509_USER_PROXY", key)
    try:
        with open(cert, "rb") as cert_file:
            certificate = load_pem_x509_certificate(cert_file.read())
            dt = int(
                certificate.not_valid_after_utc.timestamp()
                - datetime.datetime.now().timestamp()
            )
            if dt < 300:
                print("x509 proxy expired or will expire soon")
                return False
            else:
                return True
    except Exception:
        print("can not open x509 proxy")
        return False


if __name__ == "__main__":

    config = get_args()

    dotenv.load_dotenv()

    if not checkX509proxy():
        exit(1)

    embargoS3init()

    etagpartsizes = []
    for i in range(10, 30):
        etagpartsizes.append(2**i)

    totalDatasetChecked = 0

    taskQueue = []
    taskQueueLock = threading.Lock()

    DIDclient = DIDClient()
    ReplicaClient = ReplicaClient()

    nWorkers = 32
    if config["verbose"]:
        nWorkers = 1
    msgLock = threading.Lock()

    rucioOprLock = threading.Lock()

    maxItems = 500
    with concurrent.futures.ThreadPoolExecutor(max_workers=nWorkers) as executor:
        try:
            futures = []
            # If nWorkers == 1, don't use threads. This makes debugging easier
            if nWorkers > 1:
                for i in range(nWorkers):
                    futures.append(executor.submit(runTask))

            if config["stdin"]:
                config["scope"] = ""
                names = []
                while 1:
                    try:
                        scope, name = input().strip().split(":")
                        if config["scope"] == "":
                            config["scope"] = scope
                        names.append(name)
                    except Exception:
                        break
            else:
                names = DIDclient.list_dids(
                    scope=config["scope"],
                    filters=({"name": config["nameprefix"] + "*"}),
                    long=False,
                    did_type="dataset",
                )
            dsvec = []
            for name in names:
                if config["stdin"]:
                    dsvec.append({"scope": config["scope"], "name": name})
                elif name.startswith(config["nameprefix"]):
                    dsvec.append({"scope": config["scope"], "name": name})
                else:
                    continue

            while len(dsvec) > 0:
                with rucioOprLock:
                    addTask2Queue(DIDclient.get_metadata_bulk(dsvec[:maxItems]))
                dsvec = dsvec[maxItems:]
                while len(taskQueue) > 1000:
                    time.sleep(1)
                # if nWorkers == 1; don't use threads
                if nWorkers == 1:
                    addTask2Queue([{"name": "TheEnd"}])
                    runTask()
            # with rucioOprLock:
            #    addTask2Queue(DIDclient.get_metadata_bulk(dsvec))

            # Tell the workers to quit
            endTasks = []
            for i in range(nWorkers):
                endTasks.append({"name": "TheEnd"})
            addTask2Queue(endTasks)

            concurrent.futures.wait(futures)

        except KeyboardInterrupt:
            print("\nCtrl+C detected. Shutting down executor gracefully...")
            # This will allow currently running tasks to finish
            executor.shutdown(wait=True, cancel_futures=True)
        except Exception as e:
            print(e)

        print(f"Total datasets checked : {totalDatasetChecked}")
