# tap-mongodb

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) from a MongoDB source.

**This is a Proof of Concept and may have limited utility.**

The Singer.io core team welcomes proposals regarding how this tap should
work, especially in terms of filling in known limitations, but no promises
are made in terms of timeliness of responses.

# Quickstart

## Install the tap

```bash
git clone git@github.com:singer-io/tap-mongodb.git # Clone this Repo
mkvirtualenv -p python3 tap-mongodb                # Create a virtualenv
source tap-mongodb/bin/activate                    # Activate the virtualenv
pip install -e .
```

## Create a config.json

```
{
  "host": "localhost",
  "port": "27017",
  "user": "user",
  "password": "pass",
  "dbname": "<name of database>"
}
```

## Run the tap in Discovery Mode

```
tap-mongodb --config config.json --discover                # Should dump a Catalog to sdtout
tap-mongodb --config config.json --discover > catalog.json # Capture the Catalog
```

## Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "people"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
          "replication-method": "FULL_TABLE",
          "custom-select-clause": "name,age,birthday,address,city,state,zip"
        }
      }]
    }
  ]
}
```

A stream needs top level (no breadcrumb) metadata that describes the following:

* replication-method
  * LOG_BASED: will use Mongo's Oplog
  * FULL_TABLE: will sync the entire table on every tap run
* custom-select-clause
  * a comma delimited list of columns in the table's data that will be selected and output during the run


## Run the tap in Sync Mode
```
tap-mongodb --config config.json --properties catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.

---

Copyright &copy; 2018 Stitch
