# IDAC Data Release Script

## Introduction
This script creates rules for dids from a configuration file for IDACs to receive data.

## Usage
```
usage: idac_release.py [-h] [--dry_run] rse file

A script that can perform a dry run and use a JSON config file.

positional arguments:
  rse         RSE name. Should match the name in the file
  file        Path to an IDAC release JSON configuration file.

options:
  -h, --help  show this help message and exit
  --dry_run   Perform a dry run without making any replication rules.
```

## Example Release Config
```json
{
	"rse": "<rse name>",
	"containers": {
        "scope:name": "true|false",
	}

}
```
