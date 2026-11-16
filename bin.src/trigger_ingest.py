#!/usr/bin/env python

# This file is part of data-curation-tools.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Resubmit missed or failed embargo image notifications.

Publishes synthetic S3 ObjectCreated notifications for the given OBS_IDs to the
Kafka topic consumed by the embargo-butler enqueue service. Only objects that
exist in S3 and are not already ingested in the embargo Butler are included.

Usage: trigger_ingest.py [--force] OBS_ID [...]
where OBS_ID looks like MC_O_20260324_000404 (whole exposure) or
MC_O_20260324_000404_R22_S11 (single detector).

Defaults target production; override with KAFKA_CLUSTER, KAFKA_TOPIC and
EMBARGO_K8S_NAMESPACE.
"""

import argparse
import json
import os
import subprocess
import sys

from confluent_kafka import Producer
from lsst.daf.butler import Butler, EmptyQueryResultError
from lsst.resources import ResourcePath

KAFKA_CLUSTER = os.environ.get("KAFKA_CLUSTER", "172.24.10.54:9094")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "rubin-summit-notification-8")
NAMESPACE = os.environ.get("EMBARGO_K8S_NAMESPACE", "summit-new")

INSTRUMENTS = dict(AT="LATISS", CC="LSSTComCam", TS="TS8", MC="LSSTCam")
BUCKETS = dict(
    AT="embargo@rubin-summit",
    CC="embargo@rubin-summit",
    TS="rubin-sts",
    MC="embargo@rubin-summit",
)
RAFT_LIST = dict(
    AT=["00"],
    CC=["22"],
    TS=["22"],
    MC=[
        "01", "02", "03",
        "10", "11", "12", "13", "14",
        "20", "21", "22", "23", "24",
        "30", "31", "32", "33", "34",
        "41", "42", "43",
    ],
)
SENSOR_LIST = dict(
    AT=["00"],
    CC=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
    TS=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
    MC=["00", "01", "02", "10", "11", "12", "20", "21", "22"],
)
CORNER_LIST = ["00", "04", "40", "44"]
CORNER_SENSORS = ["W0", "W1", "G0_guider", "G1_guider"]


def ingested_detectors(instrument, obs_day, seq_num):
    """Return the detector names already ingested for this exposure."""
    butler = Butler("embargo", instrument=instrument, collections=f"{instrument}/raw/all")
    detectors = {d.id: d.full_name for d in butler.query_dimension_records("detector")}
    where = f"day_obs={obs_day} and exposure.seq_num={seq_num}"
    ingested = set()
    try:
        refs = butler.query_datasets("raw", where=where)
        ingested = {detectors[r.dataId["detector"]] for r in refs}
    except EmptyQueryResultError:
        pass
    if instrument == "LSSTCam":
        try:
            refs = butler.query_datasets("guider_raw", where=where, collections=f"{instrument}/raw/guider")
            ingested |= {detectors[r.dataId["detector"]] + "_guider" for r in refs}
        except EmptyQueryResultError:
            pass
    return ingested


def object_keys(obs_id):
    """Return the S3 keys of the not-yet-ingested detectors of an OBS_ID."""
    instr_code, controller, obs_day, seq_num = obs_id.split("_", maxsplit=3)
    instrument = INSTRUMENTS[instr_code]
    ingested = ingested_detectors(instrument, obs_day, seq_num.split("_")[0])
    if "_" in seq_num:  # single detector, e.g. 000404_R22_S11 or 000404_R00_SG0_guider
        seq_num, detector = seq_num.split("_", maxsplit=1)
        detectors = [detector]
    else:
        detectors = [f"R{r}_S{s}" for r in RAFT_LIST[instr_code] for s in SENSOR_LIST[instr_code]]
        if instrument == "LSSTCam":
            detectors += [f"R{r}_S{s}" for r in CORNER_LIST for s in CORNER_SENSORS]
    name = f"{instr_code}_{controller}_{obs_day}_{seq_num}"
    return [f"{instrument}/{obs_day}/{name}/{name}_{d}.fits" for d in detectors if d not in ingested]


def clear_dedupe_keys(paths):
    """Delete enqueue's ENQ:<path> dedupe keys to allow re-enqueueing."""
    # Commands go via stdin: ~200 paths in the exec URI exceed the API limit.
    commands = "".join(f"DEL ENQ:{path}\n" for path in paths)
    subprocess.run(
        ["kubectl", "exec", "-i", "-n", NAMESPACE, "redis-0", "--",
         "sh", "-c", 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning'],
        input=commands, text=True, check=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("obs_ids", nargs="+", metavar="OBS_ID")
    parser.add_argument("--force", action="store_true",
                        help="clear enqueue dedupe keys first, to retry paths enqueued in the last 24h")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": KAFKA_CLUSTER, "acks": "all", "enable.idempotence": True})
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))

    for obs_id in args.obs_ids:
        profile_bucket = BUCKETS[obs_id.split("_")[0]]  # e.g. embargo@rubin-summit
        keys = [k for k in object_keys(obs_id) if ResourcePath(f"s3://{profile_bucket}/{k}").exists()]
        if not keys:
            print(f"{obs_id}: nothing to notify (objects missing in S3 or already ingested)")
            continue
        if args.force:
            clear_dedupe_keys([f"{profile_bucket}/{k}" for k in keys])
        records = [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {"bucket": {"name": profile_bucket.split("@")[-1]}, "object": {"key": k}},
            }
            for k in keys
        ]
        producer.produce(KAFKA_TOPIC, json.dumps({"Records": records}).encode(), on_delivery=on_delivery)
        print(f"{obs_id}: notified {len(keys)} objects")

    producer.flush(30)
    if failures:
        sys.exit(f"Kafka delivery failed: {failures}")


if __name__ == "__main__":
    main()
