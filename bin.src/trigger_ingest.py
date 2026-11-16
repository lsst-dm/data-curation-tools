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

Produces synthetic Ceph S3 ObjectCreated notifications onto the Kafka topic
consumed by the embargo-butler ``enqueue`` service, so the objects go through the
normal enqueue -> Redis -> ingest path. This replaces the earlier version of this
script that POSTed to the retired enqueue HTTP ``/notify`` webhook.

The S3 event JSON has the same shape RGW publishes (``Records[].eventName``,
``.s3.bucket.name`` and ``.s3.object.key``), one Kafka message per OBS_ID. A record
is only emitted if the object exists in S3. Detectors already present in the
embargo Butler are skipped (``--skip-butler-check`` to disable).

Usage::

    export VAULT_ADDR=https://vault.slac.stanford.edu
    vault login -method=ldap
    setup lsst_distrib

    trigger_ingest.py MC_O_20260324_000404                 # all detectors
    trigger_ingest.py MC_O_20260324_000404_R22_S11         # single detector
    trigger_ingest.py MC_O_20260324_000404_R00_SG0_guider  # corner / guider

    # Retry a path already enqueued in the last 24h. enqueue keeps an
    # ENQ:<path> dedupe key for ENQUEUE_DEDUPE_TTL; --force DELs those keys in the
    # in-cluster Redis via ``kubectl exec redis-0`` just before producing.
    trigger_ingest.py --force MC_O_20260324_000404_R22_S11

    # Target the dev environment instead of the default (production):
    KAFKA_CLUSTER=172.24.10.50:9094 KAFKA_TOPIC=prompt-processing-dev-20260114 \\
    VAULT_SECRET_MOUNT_PATH=secret/rubin/usdf-embargo-dmz-dev/test \\
    EMBARGO_K8S_NAMESPACE=test SKIP_BUTLER_CHECK=1 \\
        trigger_ingest.py MC_O_20260324_000404

    # Persistent S3 credentials (not deleted on exit); then reuse in your shell:
    trigger_ingest.py --aws-credentials-out ~/.aws/embargo-sdf.credentials
    export AWS_SHARED_CREDENTIALS_FILE=$HOME/.aws/embargo-sdf.credentials
    export AWS_ENDPOINT_URL=https://sdfembs3.sdf.slac.stanford.edu

Defaults target production. The production topic is shared with other consumers
(e.g. Prompt Processing), so anything produced is visible to every consumer group.
Prefer re-notifying only detectors that are missing, which is the default Butler
check.

Dependencies
------------
LSST Pipelines stack (``lsst.resources``, ``lsst.daf.butler`` and
``confluent-kafka``), the ``vault`` CLI, and ``kubectl`` for ``--force``.

Environment variables
---------------------
KAFKA_CLUSTER
    Kafka bootstrap servers (default: ``172.24.10.54:9094``).
KAFKA_TOPIC
    Kafka topic to publish to (default: ``rubin-summit-notification-8``).
EMBARGO_K8S_NAMESPACE
    Kubernetes namespace of the embargo-butler deployment, used by ``--force``
    (default: ``summit-new``).
VAULT_NOTIFICATION_MOUNT_PATH / VAULT_SECRET_MOUNT_PATH
    Vault path holding the S3 ``profile`` field
    (default: ``secret/rubin/usdf-embargo-dmz/summit-new``).
EMBARGO_AWS_CREDENTIALS_FILE
    If set, this script copies it to ``AWS_SHARED_CREDENTIALS_FILE`` before S3 calls
    (skip Vault profile fetch).
SKIP_BUTLER_CHECK
    If ``1``/``true``, same as ``--skip-butler-check``.
"""

import argparse
import atexit
import json
import os
import shlex
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlparse

from confluent_kafka import Producer
from lsst.resources import ResourcePath

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

_DEFAULT_VAULT_SECRET = "secret/rubin/usdf-embargo-dmz/summit-new"
_DEFAULT_KAFKA_CLUSTER = "172.24.10.54:9094"
_DEFAULT_KAFKA_TOPIC = "rubin-summit-notification-8"
_DEFAULT_K8S_NAMESPACE = "summit-new"
_REDIS_POD = "redis-0"

_temp_aws_files = []
_atexit_cleanup_registered = False


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "y")


def _vault_secret_path() -> str:
    return os.environ.get(
        "VAULT_SECRET_MOUNT_PATH",
        os.environ.get("VAULT_NOTIFICATION_MOUNT_PATH", _DEFAULT_VAULT_SECRET),
    )


def _vault_kv_field(path: str, field: str) -> str:
    cmd = ["vault", "kv", "get", "-field=" + field, path]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        sys.stderr.write(p.stderr)
        raise RuntimeError("vault failed (%s): %s" % (p.returncode, " ".join(cmd)))
    return p.stdout


def _cleanup_temp_aws_files() -> None:
    global _temp_aws_files
    for p in list(_temp_aws_files):
        if p and os.path.isfile(p):
            try:
                os.unlink(p)
            except OSError:
                pass
    _temp_aws_files.clear()


def _vault_profile_to_credentials_ini(body: str):
    """Return (credentials_file_contents, endpoint_url_or_none).

    Vault sometimes stores a URL like ``https://ACCESS:SECRET@host`` instead of
    AWS ``[embargo]`` INI; botocore cannot parse the former directly.
    """
    raw = body.strip()
    if not raw:
        return raw, None
    if raw.lstrip().startswith("["):
        return raw, None
    u = urlparse(raw)
    if u.scheme in ("http", "https") and u.username and u.password:
        access_key = unquote(u.username)
        secret_key = unquote(u.password)
        host = u.hostname
        if not host:
            return raw, None
        port = u.port
        endpoint = "%s://%s:%s" % (u.scheme, host, port) if port else "%s://%s" % (u.scheme, host)
        creds = (
            "[embargo]\n"
            "aws_access_key_id = %s\n"
            "aws_secret_access_key = %s\n" % (access_key, secret_key)
        )
        return creds, endpoint
    return raw, None


def _ensure_embargo_s3_credentials(credentials_out: str | None = None) -> None:
    """ResourcePath ``s3://embargo@...`` uses boto profile 'embargo'; materialize from Vault if needed."""
    global _temp_aws_files, _atexit_cleanup_registered
    explicit = os.environ.get("EMBARGO_AWS_CREDENTIALS_FILE", "").strip()
    if explicit:
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = explicit
        return
    if os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "").strip():
        return
    body = _vault_kv_field(_vault_secret_path(), "profile")
    cred_contents, endpoint_url = _vault_profile_to_credentials_ini(body)
    if credentials_out:
        path = os.path.abspath(os.path.expanduser(credentials_out))
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, mode=0o700, exist_ok=True)
        with open(path, "w") as fh:
            fh.write(cred_contents)
        os.chmod(path, 0o600)
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = path
        if endpoint_url and not os.environ.get("AWS_ENDPOINT_URL", "").strip():
            os.environ["AWS_ENDPOINT_URL"] = endpoint_url
            print("# AWS_ENDPOINT_URL=%s (from Vault profile URL)" % endpoint_url, file=sys.stderr)
        print("# wrote persistent AWS_SHARED_CREDENTIALS_FILE=%s" % path, file=sys.stderr)
        return
    fd, path = tempfile.mkstemp(prefix="embargo-trigger-", suffix=".credentials", text=True)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(cred_contents)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    _temp_aws_files.append(path)
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = path
    if endpoint_url and not os.environ.get("AWS_ENDPOINT_URL", "").strip():
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
        print("# AWS_ENDPOINT_URL=%s (from Vault profile URL)" % endpoint_url, file=sys.stderr)
    if not _atexit_cleanup_registered:
        atexit.register(_cleanup_temp_aws_files)
        _atexit_cleanup_registered = True
    print("# AWS_SHARED_CREDENTIALS_FILE=%s (from Vault profile field; deleted on exit)" % path, file=sys.stderr)


class Records:
    def __init__(self, bucket: str, profile: str = ""):
        self._profile = profile
        self._bucket = bucket
        self._records: list[dict] = []
        self._paths: list[str] = []

    def records(self) -> list[dict]:
        return self._records

    def paths(self) -> list[str]:
        """Return ``profile + bucket + "/" + key`` for each appended record.

        Matches the string ``enqueue.py`` uses to build ``ENQ:<path>`` keys.
        """
        return self._paths

    def append(self, oid: str) -> None:
        record = {
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": self._bucket},
                "object": {"key": oid},
            },
        }
        path = ResourcePath(f"s3://{self._profile}{self._bucket}/{oid}")
        if path.exists():
            self._records.append(record)
            self._paths.append(f"{self._profile}{self._bucket}/{oid}")
            print(oid)


def _force_clear_dedupe(paths: list[str], namespace: str, pod: str = _REDIS_POD) -> None:
    if not paths:
        return
    # Pipe DELs via stdin (redis-cli reads commands from stdin) so the kubectl
    # exec request URI stays small — full-focal-plane LSSTCam has ~197 paths and
    # otherwise blows past the API-server/nginx URI limit (HTTP 414).
    stdin_data = "".join(f"DEL ENQ:{p}\n" for p in paths)
    cmd = ["kubectl", "exec", "-i", "-n", namespace, pod, "--",
           "sh", "-c", 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning']
    result = subprocess.run(cmd, input=stdin_data, capture_output=True, text=True)
    if result.returncode != 0:
        print("# --force DEL failed (rc=%s) ns=%s pod=%s: %s"
              % (result.returncode, namespace, pod, result.stderr.strip()), file=sys.stderr)
        return
    print("# --force cleared %d ENQ:<path> key(s) ns=%s pod=%s"
          % (len(paths), namespace, pod), file=sys.stderr)


def _resolve_skip_butler(args: argparse.Namespace) -> bool:
    """Default: query the embargo Butler so already-ingested detectors are skipped."""
    if args.skip_butler_check:
        return True
    return _truthy("SKIP_BUTLER_CHECK")


def _query_ingested(instrument: str, obs_day: str, seq_num: str) -> set[str]:
    """Return the set of detector slot names already ingested for this exposure."""
    from lsst.daf.butler import Butler, EmptyQueryResultError

    butler = Butler("embargo", instrument=instrument, collections=f"{instrument}/raw/all")
    detectors = {d.id: d.full_name for d in butler.query_dimension_records("detector")}
    ingested: set[str] = set()
    try:
        refs = butler.query_datasets(
            "raw", where=f"day_obs={obs_day} and exposure.seq_num={seq_num}"
        )
        ingested = {detectors[r.dataId["detector"]] for r in refs}
    except EmptyQueryResultError:
        pass
    if instrument == "LSSTCam":
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
    return ingested


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "obs_ids",
        nargs="*",
        help="Observation ids, e.g. MC_O_20260125_001234 (omit when using only --aws-credentials-out)",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_CLUSTER", _DEFAULT_KAFKA_CLUSTER),
        help="Kafka bootstrap servers (env: KAFKA_CLUSTER; default %s)" % _DEFAULT_KAFKA_CLUSTER,
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", _DEFAULT_KAFKA_TOPIC),
        help="Kafka topic (env: KAFKA_TOPIC; default %s)" % _DEFAULT_KAFKA_TOPIC,
    )
    parser.add_argument(
        "--skip-butler-check",
        action="store_true",
        help="Do not query Butler('embargo') for already-ingested detectors "
        "(env: SKIP_BUTLER_CHECK=1); only S3 existence gates records",
    )
    parser.add_argument(
        "--host-butler",
        action="store_true",
        help="Deprecated no-op: the Butler check is now the default",
    )
    parser.add_argument(
        "--aws-credentials-out",
        metavar="PATH",
        help=(
            "Write Vault 'profile' field as boto [embargo] INI to PATH (0600); file is kept after exit. "
            "Use alone with no obs_ids to refresh credentials; then export "
            "AWS_SHARED_CREDENTIALS_FILE=PATH (and AWS_ENDPOINT_URL if stderr prints it)."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Before producing each obs_id, DEL the corresponding ENQ:<path> dedupe keys "
            "in the in-cluster Redis (via `kubectl exec redis-0`) so paths enqueued "
            "in the last 24h can be re-enqueued. No-op for paths that aren't deduped."
        ),
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("EMBARGO_K8S_NAMESPACE", _DEFAULT_K8S_NAMESPACE),
        help=(
            "Kubernetes namespace for the embargo-butler deployment, used only by "
            "--force (env: EMBARGO_K8S_NAMESPACE; default %s)" % _DEFAULT_K8S_NAMESPACE
        ),
    )
    args = parser.parse_args()

    if not args.obs_ids:
        if args.aws_credentials_out:
            _ensure_embargo_s3_credentials(credentials_out=args.aws_credentials_out)
            sys.exit(0)
        parser.error(
            "at least one obs_id is required (unless using only --aws-credentials-out PATH to write credentials)"
        )

    _ensure_embargo_s3_credentials(credentials_out=args.aws_credentials_out)
    skip_butler = _resolve_skip_butler(args)

    print(
        f"# kafka={args.bootstrap}  topic={args.topic}  skip_butler_check={skip_butler}"
        f"  force={args.force}  namespace={args.namespace if args.force else '-'}",
        file=sys.stderr,
    )

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "enable.idempotence": True,
    })

    deliveries: list[tuple[str, int | None, int | None, str | None]] = []

    def _on_delivery(obs_id: str):
        def cb(err, msg):
            if err is not None:
                deliveries.append((obs_id, None, None, str(err)))
            else:
                deliveries.append((obs_id, msg.partition(), msg.offset(), None))
        return cb

    try:
        for obs_id in args.obs_ids:
            instr_code, controller, obs_day, seq_num = obs_id.split("_", maxsplit=3)
            instrument = INSTRUMENTS[instr_code]
            bucket = BUCKETS[instr_code]
            if "@" in bucket:
                profile, bucket = bucket.split("@")
                profile += "@"
            else:
                profile = ""

            records = Records(bucket, profile)

            if skip_butler:
                ingested: set[str] = set()
            else:
                ingested = _query_ingested(instrument, obs_day, seq_num.split("_")[0])

            if "_" in seq_num:
                # raft/sensor already carry their R/S prefixes, e.g. R22_S11 or R00_SG0_guider
                seq_num, raft, sensor = seq_num.split("_", maxsplit=2)
                obs_id_short = f"{instr_code}_{controller}_{obs_day}_{seq_num}"
                oid = f"{instrument}/{obs_day}/{obs_id_short}/{obs_id_short}_{raft}_{sensor}.fits"
                if f"{raft}_{sensor}" not in ingested:
                    records.append(oid)
            else:
                for raft in RAFT_LIST[instr_code]:
                    for sensor in SENSOR_LIST[instr_code]:
                        oid = f"{instrument}/{obs_day}/{obs_id}/{obs_id}_R{raft}_S{sensor}.fits"
                        if f"R{raft}_S{sensor}" not in ingested:
                            records.append(oid)
                if instrument == "LSSTCam":
                    for raft in CORNER_LIST:
                        for sensor in CORNER_SENSORS:
                            oid = f"{instrument}/{obs_day}/{obs_id}/{obs_id}_R{raft}_S{sensor}.fits"
                            if f"R{raft}_S{sensor}" not in ingested:
                                records.append(oid)

            if not records.records():
                print(
                    f"# no records for {obs_id} (no S3 paths exist or already ingested)",
                    file=sys.stderr,
                )
                continue

            # DEL dedupe markers BEFORE producing so the consumer's SET NX EX
            # always succeeds for paths we're explicitly re-triggering.
            if args.force:
                _force_clear_dedupe(records.paths(), args.namespace)

            payload = {"Records": records.records()}
            producer.produce(
                args.topic,
                value=json.dumps(payload).encode(),
                on_delivery=_on_delivery(obs_id),
            )
            # Serve any callbacks already ready; non-blocking.
            producer.poll(0)
    finally:
        # Block until queued messages have been delivered (or failed).
        pending = producer.flush(30)
        if pending:
            print(f"# WARNING: {pending} messages still pending after 30s flush", file=sys.stderr)

    errors = []
    for obs_id, partition, offset, err in deliveries:
        if err is not None:
            print(f"# FAILED obs_id={obs_id}: {err}", file=sys.stderr)
            errors.append(obs_id)
        else:
            print(
                f"# produced obs_id={obs_id} partition={partition} offset={offset}",
                file=sys.stderr,
            )

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
