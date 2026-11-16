##############
trigger_ingest
##############

``trigger_ingest.py`` resubmits missed or failed embargo image notifications.
For each OBS_ID it publishes an S3 ``ObjectCreated`` notification to the Kafka
topic read by the embargo-butler ``enqueue`` service, so the objects go through
the normal enqueue, Redis and ingest path. Only objects that exist in S3 and
are not yet ingested in the embargo Butler are included.

Requirements
============

- LSST Pipelines stack (``lsst.daf.butler``, ``lsst.resources``, ``confluent-kafka``)
- S3 credentials for the ``embargo`` profile, as used by ``Butler("embargo")``
- Network access to the Kafka cluster
- ``kubectl`` access to the embargo-butler namespace (only for ``--force``)

Usage
=====

::

    setup lsst_distrib
    trigger_ingest.py MC_O_20260324_000404                  # whole exposure
    trigger_ingest.py MC_O_20260324_000404_R22_S11          # single detector
    trigger_ingest.py MC_O_20260324_000404_R00_SG0_guider   # corner or guider
    trigger_ingest.py --force MC_O_20260324_000404_R22_S11  # retry within 24h

``enqueue`` ignores paths it has already enqueued in the last 24 hours;
``--force`` deletes those dedupe keys first.

Configuration
=============

Defaults target production. Override with environment variables:

- ``KAFKA_CLUSTER`` (default ``172.24.10.54:9094``)
- ``KAFKA_TOPIC`` (default ``rubin-summit-notification-8``)
- ``EMBARGO_K8S_NAMESPACE`` (default ``summit-new``, used by ``--force``)

The production topic is shared with other consumers, so anything produced is
visible to all of them.
