#####################################
Split Butler Repo into Rucio Datasets
#####################################

``butler-repo-2-rucio-datasets`` (link to location to be added) is a tool that will split a
Butler repo into multiple rucio datasets. This is useful when you have a butler repo with a
very large number of datasets, and you want to split it into smaller, more manageable rucio
datasets. The script work with all butler RUN collections (or all RUN collection in a root
Chain collection). 

The tools splits butler datasets based on their RUN collection name, dataset types and ingestion 
dates. Ingestion date is used so that we can divide the work into smaller phases.

A few parameters at the begin of the script can be changed to customize the the script. They
are listed below:

* ``repo`` is the path (or alias) to the butler repo that you want to split into rucio datasets.
* ``rootChain`` is the name of the CHAIN collection that you want to split into rucio 
  datasets. If this parameter is '*', the script will split all RUN collections in the butler
  repo into rucio datasets.
* ``UUIDDIR`` is the output directory where lists of butler dataset UUIDs will be stored.
* ``startDate`` and ``cutoffData`` are the boundary of butler ingestion dates for the butler
  datasets to be included in the rucio datasets.

In addition to screen summary output, the script save lists of butler dataset UUIDs for each 
rucio dataset in the ``UUIDDIR`` directory. These lists can be used as input to the 
``rucio-register`` script to create rucio datasets. 

Each of the UUID list file contains a header line (startig with ``#``) with the name of the rucio
dataset and the number of UUIDs in the file, followed by the list of butler dataset UUIDs that
belong to that rucio dataset. The name of the rucio dataset is looks like

   .. code-block:: python

      Dataset/{collection}-{datasetType}-{DFname}-{period}-{index:08d}
 
* For a collection with small amount of datasets (that can all fit into a single rucio dataset),
  the ``period`` will be the ``startData`` to the ``cutoffData``. (in this case, dataset type
  will be ``allTypes``).
* For a collection with large amount of datasets, the ``period`` will be based on the quarter of
  a year. Using quarter of a year as period allows us to align a rucio datasets based on the 
  boundary of quarter, and make the creation of rucio datasets repeatable even when the
  ``startData`` and ``cutoffData`` are not exactly the same.
* The index is used to distinguish multiple rucio datasets that belong to the same collection
  and dataset type, and have the same period.
* No element in rucio dataset name  contains a dash '-' except ``{collection}``.
* The file name of the UUID list is ``refs_{md5sum of the rucio dataset name}``

To register a UUID list file to rucio, use the following command:

   .. code:: bash

      rucio-register dataset-list --rucio-register-config <rucio-register-config-file> \
          --repo {repo} \
          --rucio-dataset {rucioDatasetName} \
          --uuidlist <uuid_file>\n"

(This usage of ``rucio-register`` requests branch `tickets/DM-54927 <https://github.com/lsst/rucio_register/tree/tickets/DM-54927>`__.)
