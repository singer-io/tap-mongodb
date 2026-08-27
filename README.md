# tap-mongodb

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) from a MongoDB source.

## Set up Virtual Environment
```
python3 -m venv ~/.virtualenvs/tap-mongodb
source ~/.virtualenvs/tap-mongodb/bin/activate
```

## Install tap
```
pip install -U pip setuptools
pip install tap-mongodb
```

## Set up Config file
Create json file called `config.json`, with the following contents:
```
{
  "password": "<password>",
  "user": "<username>",
  "host": "<host ip address>",
  "port": "<port>",
  "database": "<database name>"
}
```
The folowing parameters are optional for your config file:

| Name | Type | Description |
| -----|------|------------ |
| `replica_set` | string | name of replica set |
|`ssl` | Boolean | can be set to true to connect using ssl |
| `include_schema_in_destination_stream_name` | Boolean | forces the stream names to take the form `<database_name>_<collection_name>` instead of `<collection_name>`|

All of the above attributes are required by the tap to connect to your mongo instance. 

## Run in discovery mode
Run the following command and redirect the output into the catalog file
```
tap-mongodb --config ~/config.json --discover > ~/catalog.json
```

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
            "row-count":<int>,
            "is-view": <bool>,
            "database-name": "<database name>",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ]
          }
        }
      ],
      "stream": "<stream name>",
      "schema": {
        "type": "object"
      }
    }
  ]
}
```

## Edit Catalog file
### Using valid json, edit the config.json file
To select a stream, enter the following to the stream's metadata:
```
"selected": true,
"replication-method": <replication method>,
```

`<replication-method>` must be either `FULL_TABLE` or `LOG_BASED`

To add a projection to a stream, add the following to the stream's metadata field:
```
"tap-mongodb.projection": <projection>
```

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
            "row-count": <int>,
            "is-view": <bool>,
            "database-name": "<database name>",
            "table-key-properties": [
              "_id"
            ],
            "valid-replication-keys": [
              "_id"
            ],
            "selected": true,
            "replication-method": "<replication method>",
            "tap-mongodb.projection": "<projection>"
          }
        }
      ],
      "stream": "<stream name>",
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

## Error messages

A MongoDB source can point at any host, so everything the server sends back is
treated as untrusted input. Stitch reads the tap's stderr one line at a time and
decides what a line is from its leading token, so the tap installs a logging
filter that stops any value containing a line break from forging a `CRITICAL`
error or an `INFO METRIC:` record.

When the tap fails it logs a single `CRITICAL` line built from the exception
class and, for `OperationFailure`, the numeric server error code — the server's
own error text is never echoed, and the exception is not re-raised so that no
traceback reaches stderr. Server-supplied values logged during discovery
(version string, database and collection names, roles) additionally go through
`tap_mongodb.error_messages.scrub`, which strips byte escapes and collapses
whitespace.

Use the numeric error code plus the job's logs to diagnose failures; the tap
deliberately does not surface the raw server response.

## Supplemental MongoDB Info

### Local MongoDB Setup
If you haven't yet set up a local mongodb client, follow [these instructions](https://github.com/singer-io/tap-mongodb/blob/master/spikes/local_mongo_setup.md)

---

Copyright &copy; 2019 Stitch
