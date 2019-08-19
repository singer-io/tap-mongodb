# tap-mongodb

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) from a MongoDB source.
# tap-mongodb:
## Set up Virtual Environment:
`python3 -m venv ~/.virtualenvs/tap-mongodb`
`source ~/.virtualenvs/tap-mongodb/bin/activate`

## Install tap:
`pip install -U pip setuptools`
`pip install tap-mongodb`
## Set up Config file:
Create json file called `config.json`, with the following contents:
```
{
  "password": "<password>",
  "user": "<username>",
  "host": "<host ip address>",
  "port": "<port>",
  "database": "<database name>",
}
```
All of the above attributes come from local mongodb client setup. If you haven't yet set up a local mongodb client, follow [these instructions](https://github.com/singer-io/tap-mongodb/blob/master/spikes/local_mongo_setup.md)
## Run in discovery mode:
and pipe into catalog file(locate catalog.json in home directory)
`tap-mongodb --config ~/config.json --discover > ~/catalog.json`
Your catalog file should now look like this:
```
{
  "streams": [
    {
      "table_name": "<table name>",
      "tap_stream_id": "<tap_stream_id>",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 100,
            "is-view": false,
            "database-name": "simple_db",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ]
          }
        }
      ],
      "stream": "simple_coll_2",
      "schema": {
        "type": "object"
      }
    }
  ]
}
```
## Edit Catalog file
### Using valid json, edit the config.json file
To select a stream, enter the following to the stream's metadata field:
`"selected": true,
"replication-method": [replication method],`
To add metadata to a stream, add the following to the stream's metadata field:
`"tap-mongodb.projection": [projection]`

For example, if you were to edit the example stream to select the stream as well as add a projection, config.json should look this:
```
{
  "streams": [
    {
      "table_name": "<table name>",
      "tap_stream_id": "<tap_stream_id>",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "row-count": 100,
            "is-view": false,
            "database-name": "simple_db",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ],
            "selected": true,
            "replication-method": "LOG_BASED",
            "tap-mongodb.projection": "{\"string_field\": 1}"
          }
        }
      ],
      "stream": "simple_coll_2",
      "schema": {
        "type": "object"
      }
    }
  ]
}

```
## Run in sync mode:
`tap-mongodb --config ~/config.json --catalog ~/catalog.json`

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.

---

Copyright &copy; 2019 Stitch
