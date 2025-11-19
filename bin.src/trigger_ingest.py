#!/usr/bin/env python

# This script simulates a Ceph S3 webhook notification to the auto-ingest
# enqueue service.
# It is useful for resubmitting missed or failed image notifications.
# Usage: trigger_ingest.py OBS_ID [...]
# where OBS_ID looks like AT_O_20230610_000357
# You must have a USDF Vault token (be logged in using "vault login").

import sys

import requests
import subprocess

from lsst.daf.butler import Butler, EmptyQueryResultError
from lsst.resources import ResourcePath


INSTRUMENTS = dict(AT="LATISS", CC="LSSTComCam", TS="TS8", MC="LSSTCam")
BUCKETS = dict(
    AT="embargoRrubin-summit",
    CC="embargo@rubin-summit",
    TS="rubin-sts",
    MC="embargo@rubin-summit",
)
NAMESPACE = dict(AT="summit-new", CC="summit-new", TS="sts", MC="summit-new")
OPAQUE = {
    instrument: subprocess.run(
        "vault kv get -mount=secret -field=notification"
        f" rubin/usdf-embargo-dmz/{NAMESPACE[instrument]}".split(" "),
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    ).stdout
    for instrument in NAMESPACE
}
# dev
# IP=dict(AT="172.24.5.191", CC="172.24.5.191", TS="172.24.5.180",
# MC="172.24.5.180")
IP = dict(AT="172.24.5.156", CC="172.24.5.156", TS="172.24.5.180", MC="172.24.5.156")

RAFT_LIST = dict(
    AT=["00"],
    CC=["22"],
    TS=["22"],
    MC=[
        ""    "01", "02", "03",
        "10", "11", "12", "13", "14",
        "20", "21", "22", "23", "24",
        "30", "31", "32", "33", "34",
        ""    "41", "42", "43",
    ],
)
SENSOR_LIST = dict(
    AT=["00"],
    CC=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
    TS=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
    MC=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
)
CORNER_LIST = ["00", "04", "40", "44"]
CORNER_SENSORS = ["W0", "W1"]
CORNER_SENSORS.extend(["G0_guider", "G1_guider"])


class Records:
    def __init__(self, bucket, profile=""):
        self._profile = profile
        self._bucket = bucket
        self._records = []

    def records(self):
        return self._records

    def append(self, oid, instr_code):
        record = {
            "s3": {
                "bucket": {"name": self._bucket},
                "object": {"key": oid},
            },
            "opaqueData": OPAQUE[instr_code],
        }
        path = ResourcePath(f"s3://{self._profile}{self._bucket}/{oid}")
        if path.exists():
            self._records.append(record)
            print(oid)


for obs_id in sys.argv[1:]:
    instr_code, controller, obs_day, seq_num = obs_id.split("_", maxsplit=3)
    instrument = INSTRUMENTS[instr_code]
    bucket = BUCKETS[instr_code]
    if "@" in bucket:
        profile, bucket = bucket.split("@")
        profile += "@"
    else:
        profile = ""

    records = Records(bucket, profile)
    butler = Butler(
        "embargo", instrument=instrument, collections=f"{instrument}/raw/all"
    )
    detectors = {d.id: d.full_name for d in butler.query_dimension_records("detector")}
    try:
        refs = butler.query_datasets(
            "raw", where=f"day_obs={obs_day} and exposure.seq_num={seq_num}"
        )
        ingested = {detectors[r.dataId["detector"]] for r in refs}
    except EmptyQueryResultError:
        ingested = {}
    try:
        refs = butler.query_datasets(
            "guider_raw",
            where=f"day_obs={obs_day} and exposure.seq_num={seq_num}",
            collections=f"{instrument}/raw/guider",
        )
        for r in refs:
            ingested.add(detectors[r.dataId["detector"]] + "_guider")
    except EmptyQueryResultError:
        pass

    if "_" in seq_num:
        seq_num, raft, sensor = seq_num.split("_")
        obs_id = f"{instr_code}_{controller}_{obs_day}_{seq_num}"
        oid = f"{instrument}/{obs_day}/{obs_id}/{obs_id}_{raft}_{sensor}.fits"
        if not f"{raft}_{sensor}" in ingested:
            records.append(oid, instr_code)
    else:
        for raft in RAFT_LIST[instr_code]:
            for sensor in SENSOR_LIST[instr_code]:
                oid = f"{instrument}/{obs_day}/{obs_id}/{obs_id}_R{raft}_S{sensor}.fits"
                if not f"R{raft}_S{sensor}" in ingested:
                    records.append(oid, instr_code)
        if instrument == "LSSTCam":
            for raft in CORNER_LIST:
                for sensor in CORNER_SENSORS:
                    oid = f"{instrument}/{obs_day}/{obs_id}/{obs_id}_R{raft}_S{sensor}.fits"
                    if not f"R{raft}_S{sensor}" in ingested:
                        records.append(oid, instr_code)

    json = {"Records": records.records()}
    r = requests.post(f"http://{IP[instr_code]}:8080/notify", json=json)
    print(r.status_code, r)
