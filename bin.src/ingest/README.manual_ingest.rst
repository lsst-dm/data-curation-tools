##################################################
Manually verify and ingest Rucio dataset to Butler
##################################################

These is a set of tools to verify the result of `ctrl_ingestd` and make up
of any missing file.

Verify a Rucio dataset ingestion
--------------------------------

Given a Rucio dataset, `audit_ingest.py` will verify if all files in that
Rucio dataset have been registered to the Butler. To use it

   .. code:: bash

      export INGEST_BUTLER=<butler path>
      python3 audit_ingest.py ruico <ruico_dataset_did scope:name>

This tool will print to STDOUT all file DIDs that are not known (missing)
by the Butler, one DID per line. This output can be used by the next tool
to ingest the missing file DIDs to Butler.

This tool will also print to STDERR a summary.

Ingest missing file DIDs to butler
----------------------------------

`ingest_from_rucio_filedids.py` takes file DIDs from STDIN (one DID per line)
and ingest to Bulter defined by the INGEST_BUTLER environment variable.

   .. code:: bash

      export INGEST_BUTLER=<butler path>
      cat <list_of_file_dids> | python3 ingest_from_rucio_filedids.py

Improvement
-----------

`audit_ingest.py` can print out the rubin sidecar info. This will save the
`ingest-from_rucio_filedids.py` from obtaining those info from Rucio. This
capability is not quite in place yet.
