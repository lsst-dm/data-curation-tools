###################################
Check Integrity of RAW Obs Datasets
###################################

The main purpose of having the RAW ``Obs`` (Rucio) datasets is maintain three safe copies of the Rubin raw data:
at the USDF (weka) disk store, USDF HPSS (aka TapeRSE) and FrDF (dCache). In Rucio, The ``Obs`` datasets are
name like ``raw:Dataset/LSSTCam/raw/Obs/20260101/MC_O_20260101_000123``. Each dataset contains two file DIDs:

- ``raw:LSSTCam/20260101/MC_O_20260101_000123.zip``
- ``raw:LSSTCam/20260101/MC_O_20260101_000123_dimensions.yaml``

The first .zip file contains all files found in 
Embargo's ``rubin-summit/LSSTCam/20260101/MC_O_20260101_000123``
folder, including 189 detector .fit images, 8 wavefront .fit images and 8 guider .fit images, their 
correcponding .jsons, and a few other .jsons.

The second .yaml contains the bulter dimension related records, to be used to ingest the .zip to a remote site's
butler. In rare cases, there will be a ``_dimensions.1.yaml`` if dimensions records are changed.

The ``checkObs.py`` script checks the following:

#. Check if the USDF copy of the ``raw:LSSTCam/20260101/MC_O_20260101_000123.zip`` contains exactly the same
   files as that in Embargo's ``rubin-summit/LSSTCam/20260101/MC_O_20260101_000123``. It will read each file
   from the zip and compare its e-tag against those in the Embargo s3. Upon successful validation, it adds a value 
   to the Rucio attribute ``SafeCopies`` for the Obs dataset, so that future check will skip these steps. 
#. Query checksum of the .zip and .yaml at the FrDF and compare that with Rucio. Upon success, mark in the
   ``SafeCopies`` attribute.
#. Query Rucio attribute ``arcBackup`` to see if the dataset has been sent to HPSS. Upone success, mark in the
   ``SafeCopies`` attribute.

There are two modes to run ``checkObs.py`` (after setting up `data-curation environment <https://df-ops.lsst.io/usdf-applications/curation/data-curation-environment-setup.html>`__) 

- ``python3 checkObs.py --prefix raw:Dataset/LSSTCam/raw/Obs/202601 --age 1000``. This will check all Rucio
  datasets with prefix ``raw:Dataset/LSSTCam/raw/Obs/202601``. The ``--age 1000`` is used to further limits the
  datasets to be checked(updated in the last 1000 days). It is usually set to a large number.
- ``echo raw:Dataset/LSSTCam/raw/Obs/20260101/MC_O_20260101_000123 | python3 checkObs.py --stdin --verbose``. In
  this case, the script take dataset names from STDIN (one dataset name per line), and print out detail info during
  the checking.

``checkObs.py`` script expects the following environment variables to be set:

- ``RUCIO_ACCOUNT`` and ``RUCIO_CONFIG``. These two are usually part of the data-curation environment setup.
- ``X509_USER_PROXY`` (or the default, ``/tmp/x509up_u$(id -u)``). This is needed to (read-only) access the 
  FrDF dCache.
- ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET``. These are needed to access the Embargo s3.
- ``embargoURL``. It should be set to ``https://sdfembs3.sdf.slac.stanford.edu``.

