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

import click
import collections
import json
import random
import re
import subprocess
import sys

from lsst.daf.butler import Butler
from lsst.resources import ResourcePath

CONFIG = {
    "LSSTCam": {
        "bucket": "embargo@rubin-summit",
        "butler_alias": "embargo",
        "obs_prefix": "MC",
    },
    "LSSTComCam": {
        "bucket": "embargo@rubin-summit",
        "butler_alias": "embargo",
        "obs_prefix": "CC",
    },
    "LATISS": {
        "bucket": "embargo@rubin-summit",
        "butler_alias": "embargo",
        "obs_prefix": "AT",
    },
}


@click.command()
@click.argument("instrument", required=True, type=click.Choice(list(CONFIG.keys())))
@click.argument("dayobs", type=int, required=True)
def run(instrument: str, dayobs: int):
    main(instrument, dayobs)


def diff(expected_set, found_set, ingested_set):
    """Output the differences between the three sets of detectors."""
    for det in sorted(expected_set):
        if det not in found_set:
            print(f"{det} not sent")
        elif det not in ingested_set:
            print(f"{det} not ingested")
    for det in sorted(found_set):
        if det not in expected_set and det not in ingested_set:
            print(f"{det} unexpected, not ingested")
    for det in sorted(ingested_set):
        if det not in found_set:
            print(f"{det} ingested but not found")


def main(instrument, dayobs):
    bucket = CONFIG[instrument]["bucket"]
    butler_alias = CONFIG[instrument]["butler_alias"]
    obs_prefix = CONFIG[instrument]["obs_prefix"]

    # Find the largest sequence number for the observation day.
    day_path = ResourcePath(f"s3://{bucket}/{instrument}/{dayobs}/")
    for dirpath, dirnames, filenames in day_path.walk():
        if len(dirnames) > 0:
            max_seq = int(sorted(dirnames)[-1][-7:-1])
            break
    else:
        print(f"No data on {dayobs}")
        sys.exit(1)

    butler = Butler(
        butler_alias, instrument=instrument, collections=f"{instrument}/raw/all"
    )

    detector_dict = {
        x.id: x.full_name
        for x in butler.query_dimension_records("detector", instrument=instrument)
    }

    # Find the science detector raws and guider raws that have been ingested.

    ingested_detectors = collections.defaultdict(set)
    with butler.query() as q:
        for data_id in (
            q.where(f"day_obs={dayobs}")
            .join_dataset_search("raw")
            .data_ids(["exposure", "detector"])
        ):
            ingested_detectors[data_id["exposure"] % 100000].add(
                detector_dict[data_id["detector"]]
            )

    ingested_guiders = collections.defaultdict(set)
    with butler.query() as q:
        for data_id in (
            q.where(f"day_obs={dayobs}")
            .join_dataset_search("guider_raw", f"{instrument}/raw/guider")
            .data_ids(["exposure", "detector"])
        ):
            ingested_guiders[data_id["exposure"] % 100000].add(
                detector_dict[data_id["detector"]]
            )

    # Check each sequence number to see if it is completely ingested.
    for seqnum in range(1, max_seq + 1):
        print(f"{dayobs=} {seqnum=}", end="")
        expected_detectors = set(detector_dict.values())
        expected_present = False
        expected_guiders = set()
        found_detectors = set()
        found_guiders = set()

        # Need to test for unusual controllers as well as normal "O".
        for controller in ("O", "C", "P", "S"):
            obs_id = f"{obs_prefix}_{controller}_{dayobs}_{seqnum:06d}"
            obs_path = day_path.join(obs_id, forceDirectory=True)
            filenames = []
            for dirpath, dirnames, filenames in obs_path.walk():
                break
            if len(filenames) == 0:
                # Nothing with this controller; try the next.
                continue
            es_name = f"{obs_id}_expectedSensors.json"
            if es_name in filenames:
                expected_present = True
                es_path = dirpath.join(es_name)
                expected_sensors = json.loads(es_path.read())["expectedSensors"]
                expected_detectors = {
                    d for d in expected_sensors if expected_sensors[d] == "SCIENCE"
                }
                expected_guiders = {
                    d for d in expected_sensors if expected_sensors[d] == "GUIDER"
                }

            for f in filenames:
                if f.endswith(".fits"):
                    m = re.search(r"R[0-4][0-4]_S[0-4GW][0-4]", f)
                    detector = m.group(0)
                    if f.endswith("_guider.fits"):
                        found_guiders.add(detector)
                    else:
                        found_detectors.add(detector)

                    # Randomly run fitsverify on the file.
                    if random.uniform(0.0, 1.0) < 2e-5:
                        source_path = dirpath.join(f)
                        with source_path.as_local() as local:
                            print(" - random fitsverify", end="")
                            try:
                                subprocess.run(
                                    ["fitsverify", local.ospath],
                                    capture_output=True,
                                    check=True
                                )
                            except subprocess.CalledProcessError as e:
                                processed = e.stderr.replace(b"OK", b"ok")
                                print(f" ret={e.returncode} stderr={processed}")
                            else:
                                print(" success", end="")

            # If we found files for a controller, there won't be another.
            break

        else:
            # No controllers have images for this sequence number.
            print(" NOT TAKEN?")
            continue

        expect_s = len(expected_detectors)
        expect_g = len(expected_guiders)
        found_s = len(found_detectors)
        found_g = len(found_guiders)
        ingest_s = len(ingested_detectors[seqnum])
        ingest_g = len(ingested_guiders[seqnum])

        print(
            f" {expect_s=} {expect_g=} {found_s=} {found_g=}"
            f" {ingest_s=} {ingest_g=}{'' if expected_present else ' !'}",
            end="",
        )

        # Normal case.
        if (
            found_s == ingest_s
            and found_s == expect_s
            and found_g == ingest_g
            and found_g == expect_g
        ):
            print(" OK")
            continue

        # Cover all possible cases, except the normal one above.
        print(" SCIENCE:", end=" ")
        if found_s < ingest_s:
            print("IMPOSSIBLE", end="")
        elif found_s > ingest_s:
            if found_s < expect_s:
                print("NOT SENT + NOT INGESTED", end="")
            else:
                if ingest_s == 0:
                    print("NONE INGESTED", end="")
                else:
                    print("NOT INGESTED", end="")
        else:  # found_s == ingest_s
            if found_s < expect_s:
                if ingest_s == 0:
                    print("NONE SENT", end="")
                else:
                    print("SOME MISSING", end="")
            elif found_s > expect_s:
                print("?MORE INGESTED", end="")
            else:
                print("INGESTED", end="")
        print(" GUIDER:", end=" ")
        if found_g < ingest_g:
            print("IMPOSSIBLE")
        elif found_g > ingest_g:
            if found_g < expect_g:
                print("NOT SENT + NOT INGESTED")
            else:
                if ingest_g == 0:
                    print("NONE INGESTED")
                else:
                    print("NOT INGESTED")
        else:  # found_g == ingest_g
            if found_g < expect_g:
                if ingest_g == 0:
                    print("NONE SENT")
                else:
                    print("SOME MISSING")
            elif found_g > expect_g:
                print("?MORE INGESTED")
            else:
                print("INGESTED")

        diff(expected_detectors, found_detectors, ingested_detectors[seqnum])
        diff(expected_guiders, found_guiders, ingested_guiders[seqnum])


if __name__ == "__main__":
    run()
