__all__ = ["make_rucio_ds_map"]


def make_rucio_ds_map(qgraph):
    """
    Map pipeline dataset types to Rucio datasets.

    Parameters
    ----------
    qgraph : `lsst.pipe.base.QuantumGraph`

    Returns
    -------
    dict : Dictionary mapping dstype to Rucio dataset.
    """
    dsmap = {}
    for node in qgraph:
        for datasetType in node.quantum.outputs.keys():
            dstype = datasetType.name
            dims = datasetType.dimensions.to_simple()
            massive = "tract" in dims and "visit" in dims
            if dstype in dsmap:
                continue
            if dstype.endswith("_config") or dstype in ("skyMap"):
                dsmap[dstype] = "Configuration"
            elif dstype.endswith("_log"):
                if massive:
                    dsmap[dstype] = "Provenance/" + dstype.removesuffix("_log")
                else:
                    dsmap[dstype] = "Provenance"
            elif dstype.endswith("_metadata"):
                if massive:
                    dsmap[dstype] = \
                        "Provenance/" + dstype.removesuffix("_metadata")
                else:
                    dsmap[dstype] = "Provenance"
            elif "_consolidated_map_" in dstype:
                dsmap[dstype] = "Map"
            elif ("_image" in dstype
                  or "_coadd" in dstype
                  or "_background" in dstype):
                dsmap[dstype] = "Image/" + dstype
            elif ("object" in dstype
                  or "source" in dstype
                  or "table" in dstype
                  or "summary" in dstype):
                dsmap[dstype] = "Catalog/" + dstype
            else:
                dsmap[dstype] = dstype
    return dsmap
