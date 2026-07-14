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

import os
from collections import defaultdict
import click
import pickle
import pandas as pd
import lsst.daf.butler as daf_butler
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.pipe.base.pipeline_graph import TaskImportMode


class CommaSeparatedDictParamType(click.ParamType):
    name = "key=value,key=value"

    def convert(self, value, param, ctx):
        if isinstance(value, dict):
            return value
        parsed_dict = {}
        # Split by comma to get individual pairs
        pairs = value.split(',')
        for pair in pairs:
            if not pair.strip():
                continue  # Skip any trailing or accidental double commas
            if '=' not in pair:
                self.fail(
                    f"'{pair.strip()}' must be in key=value format.",
                    param,
                    ctx
                )
            key, val = pair.split('=', 1)
            key = key.strip()
            # Strip whitespace and accidental quotes from the terminal
            val = val.strip().strip("'\"")
            # Auto-convert numeric strings to integers if applicable
            if val.isdigit():
                val = int(val)
            parsed_dict[key] = val
        return parsed_dict


# Create the type instance
COMMA_DICT = CommaSeparatedDictParamType()


@click.group()
@click.option("--verbose", is_flag=True, help="Enable verbose logging.")
@click.pass_context
def cli(ctx, verbose):
    """Tool for finding and packaging inputs to a pipetask quantum."""
    ctx.ensure_object(dict)
    ctx.obj['VERBOSE'] = verbose


@cli.command(name="extract",
             help=("Extract dataset ref data for the inputs "
                   "to the specified task/dataIds."))
@click.option("--qg-file", required=True, type=click.Path(exists=True),
              help="QuantumGraph (.qg) file.")
@click.option("--task", required=True, help="pipetask name")
@click.option("--data-id", type=COMMA_DICT, required=True,
              help="Data Id (can be partial)")
@click.option("--ref-data-file", default="ref_data.pickle",
              type=click.Path(exists=False),
              help="Output pickle file to contain dataset ref info")
def extract_ref_data(qg_file, task, data_id, ref_data_file):
    extractor = QuantumGraphExtractor(qg_file)
    extractor.get_input_ref_data(task, data_id, outfile=ref_data_file)


@cli.command(name="export",
             help="Create an export dataset for the provided ref-data.")
@click.option("--ref-data-file", required=True, type=click.Path(exists=True),
              help="Pickle file containing dataset ref info")
@click.option("--butler-config", required=True, help="Butler repository")
@click.option("--collection", required=True,
              help="Collection containing the input dataset refs")
@click.option("--output-dir", default=".",
              help="Output directory to contain exported data")
def export_refs(ref_data_file, butler_config, collection, output_dir):
    with open(ref_data_file, "rb") as fobj:
        ref_data = pickle.load(fobj)
    butler = daf_butler.Butler(butler_config, collections=[collection])
    QuantumGraphExtractor.export_refs(butler, ref_data, outdir=output_dir)


class QuantumGraphExtractor:
    def __init__(self, qg_file):
        with PredictedQuantumGraph.open(
                qg_file,
                import_mode=TaskImportMode.DO_NOT_IMPORT) as reader:
            reader.read_thin_graph()
            self.qgraph = reader.finish()
        self._make_mappings()

    def _make_mappings(self):
        # Map uuids to task-dataId combinations
        self._task_instance_map = {}
        self._uuid_map = {}
        self._dfs = {}
        for task in self.qgraph.quanta_by_task.keys():
            data = defaultdict(list)
            quanta = self.qgraph.quanta_by_task[task]
            for dataId, uuid in quanta.items():  # noqa: N806
                self._task_instance_map[uuid] = task, dataId
                my_dataId = dataId.to_simple().model_dump()['dataId']  # noqa: N806, E501
                my_key = tuple(sorted(my_dataId.items()))
                self._uuid_map[(task, my_key)] = uuid
                data['uuid'].append(uuid)
                for k, v in my_dataId.items():
                    data[k].append(v)
            self._dfs[task] = pd.DataFrame(data)

    def get_uuids(self, task, dataId):  # noqa: N803
        conditions = []
        for key, value in dataId.items():
            if isinstance(value, str):
                conditions.append(f"{key}=='{value}'")
            else:
                conditions.append(f"{key}=={value}")
        query = " and ".join(conditions)
        df = self._dfs[task].query(query)
        uuids = []
        for _, row in df.iterrows():
            my_key = tuple((key, row[key]) for key in sorted(df.columns)
                           if key != 'uuid')
            uuids.append(self._uuid_map[(task, my_key)])
        return uuids

    def get_input_ref_data(self, task, dataId, outfile=None):  # noqa: N803
        uuids = self.get_uuids(task, dataId)
        ref_data = set()
        for uuid in uuids:
            task, dataId = self._task_instance_map[uuid]  # noqa: N806
            inputs = self.qgraph.pipeline_graph.inputs_of(task)
            dataId = dataId.to_simple().model_dump()['dataId']  # noqa: N806
            for dstype in inputs.values():
                my_dataId = {key: dataId[key] for key  # noqa: N806
                             in dstype.dimensions.to_simple()
                             if key in dataId}
                ref_data.add(
                    (dstype.name, tuple(sorted(my_dataId.items())))
                )
        if outfile is not None:
            with open(outfile, "wb") as fobj:
                pickle.dump(ref_data, fobj)
        return ref_data

    @staticmethod
    def get_refs(butler, ref_data, ignore=("astrometry_camera",
                                           "the_monster_20250219")):
        refs = []
        for dstype, kv_pairs in ref_data:
            if dstype in ignore:
                continue
            try:
                my_refs = butler.query_datasets(dstype, **dict(kv_pairs))
            except daf_butler.EmptyQueryResultError:
                # No idea why this can't just return an empty list.
                my_refs = []
            refs.extend(my_refs)
        return refs

    @staticmethod
    def export_refs(butler, ref_data, outfile="export.yaml", outdir=".",
                    transfer="copy"):
        os.makedirs(outdir, exist_ok=True)
        refs = QuantumGraphExtractor.get_refs(butler, ref_data)
        with butler.export(directory=outdir,
                           filename=outfile,
                           transfer=transfer) as exporter:
            exporter.saveDatasets(refs)


if __name__ == "__main__":
    cli()
