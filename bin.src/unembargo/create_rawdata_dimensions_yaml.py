#!/usr/bin/env python

# Purpose: create dimensions.yaml for raw data. This script mimics
#     the dimensions.yaml creation in transfer_raw_zip.py.
# Usage: python this_script exposure1 exposure2 ...
#     where exposure is like: 2026031500049
# output: Dimensions yaml for each exposure: e.g.
#     MC_O_20260315_000049_dimensions.1.yaml

import sys
from logging import Logger
from lsst.daf.butler import Butler

logger = Logger()

butler = Butler("embargo", skymap="lsst_cells_v1")
instrument = "LSSTCam"
exposures = butler.query_dimension_records(
    "exposure",
    where=f"exposure in ({','.join(sys.argv[1:])})",
    limit=None,
    instrument=instrument,
    order_by="exposure",
    explain=False,
)

for exp in exposures:
    dimensions_file = f"{exp.obs_id}_dimensions.1.yaml"
    with butler.export(filename=dimensions_file) as export:
        export.saveDimensionData("exposure", [exp])
        dims = [
            "day_obs",
            "group",
            "visit",
            "visit_definition",
            "visit_detector_region",
            "visit_system",
            "visit_system_membership",
        ]
        for dim in dims:
            recs = butler.query_dimension_records(
                dim,
                exposure=exp.id,
                limit=None,
                instrument=instrument,
                explain=False,
            )
            if recs:
                export.saveDimensionData(dim, recs)
    logger.info("saving dimension records to  %s", dimensions_file)
