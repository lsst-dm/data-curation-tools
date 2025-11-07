# IDAC Data Release Script

## Introduction
This script creates rules for dids from a configuration file for IDACs to receive data.

## Usage
```
usage: idac_release.py [-h] [--dry_run] [--idac_file IDAC_FILE] [--did_file DID_FILE] rse

A script that can perform a dry run and use a JSON config file.

positional arguments:
  rse                   RSE name

options:
  -h, --help            show this help message and exit
  --dry_run             Perform a dry run without making any replication rules.
  --idac_file IDAC_FILE
                        Path to an IDAC product selection JSON configuration file.
  --did_file DID_FILE   Path to the data release JSON configuration file. Contains number of files in a container.

examples: python idac_release.py --did_file ./dp1.json --idac_file ./IDAC_US-UW.json IDAC_US-UW
```

## Requirements
* `rucio-clients`

## Example Data Release (`did_file`) Config
```json
{
	"containername": "<# datasets>"
}
```

## Example IDAC Release (`idac_file`) Config
```json
{
	"rse": "<rse name>",
	"containers": {
        "scope:name": "true|false",
	}

}
```

More examples are in the `idac` and `datareleases` directories.
