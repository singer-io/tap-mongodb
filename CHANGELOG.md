# Changelog

## 2.1.2
  * Update pymongo to v3.12.3 [#81](https://github.com/singer-io/tap-mongodb/pull/81)

## 2.1.1
  * Fix bug in oplog bookmarking where the bookmark would not advance due to fencepost querying finding a single record [#80](https://github.com/singer-io/tap-mongodb/pull/80)

## 2.1.0
  * Optimize oplog extractions to only query for the selected tables [#78](https://github.com/singer-io/tap-mongodb/pull/78)

## 2.0.1
  * Modify `get_databases` function to return a unique list of databases [#58](https://github.com/singer-io/tap-mongodb/pull/58)

## 2.0.0
  * Build and write schema messages [#40](https://github.com/singer-io/tap-mongodb/pull/40) The main changes are:
    1. date-time fields will have a `"type": "string", "format": "date-time"` schema that will cause them to get loaded as date-times instead of strings
    2. decimal fields will have a `"type": "number", "multipleOf": 1e-34` schema written
    3. double fields will have a `"type": "number"` schema written that should prevent them from splitting between doubles/decimals depending on the precision

## 1.1.0
  * Add optional `verify_mode` config value to replace the assumptions in version 1.0.4 [#38](https://github.com/singer-io/tap-mongodb/pull/38)

## 1.0.4
  * Add support for turning off ssl cert validation when using a ssh tunnel [#36](https://github.com/singer-io/tap-mongodb/pull/36)

## 1.0.3
  * Add support for floats as replication keys [#34](https://github.com/singer-io/tap-mongodb/pull/34)

## 1.0.2
  * Add support for DBRefs [#32](https://github.com/singer-io/tap-mongodb/pull/32)

## 1.0.1
  * Discover collections in the `admin` database and add support for `Int64` as a replication key type [#30](https://github.com/singer-io/tap-mongodb/pull/30)

## 1.0.0
  * Release out of Beta [#29](https://github.com/singer-io/tap-mongodb/pull/29)

## 0.3.0
  * Add support for UUID types in replication keys and records [#27](https://github.com/singer-io/tap-mongodb/pull/27)

## 0.2.2
  * Improve invalid datetime handling [#25](https://github.com/singer-io/tap-mongodb/pull/25)

## 0.2.1
  * Clear stream state if replication method changes [#24](https://github.com/singer-io/tap-mongodb/pull/24)

## 0.2.0
  * Improve Oplog query performance by using only a timestamp and the `oplog_replay` arg. [#23](https://github.com/singer-io/tap-mongodb/pull/23)

## 0.1.11
  * Only bookmark latest ts on first sync for oplog [#22](https://github.com/singer-io/tap-mongodb/pull/22)

## 0.1.10
  * Fix for additional empty string projections [#21](https://github.com/singer-io/tap-mongodb/pull/21)

## 0.1.9
  * Make tap robust against projection that is empty string
  * Actually respect `INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME` prop
  * [#20](https://github.com/singer-io/tap-mongodb/pull/20)

## 0.1.8
  * Prefer secondary when connecting to Mongo [#19](https://github.com/singer-io/tap-mongodb/pull/19)

## 0.1.7
  * Full Table syncs can handle empty collections [#18](https://github.com/singer-io/tap-mongodb/pull/18)

## 0.1.6
  * Fix a bug with supporting bookmarks of ObjectId [#17](https://github.com/singer-io/tap-mongodb/pull/17)

## 0.1.5
  * Check for cases when the Oplog may have aged out and execute a full resync [#16](https://github.com/singer-io/tap-mongodb/pull/16)

## 0.1.4
  * Get global oplog timestamp instead of collection-specific [#15](https://github.com/singer-io/tap-mongodb/pull/15)

## 0.1.3
  * Support several new types for the `_id` column aside from ObjectID [#14](https://github.com/singer-io/tap-mongodb/pull/14)

## 0.1.2
  * Encode bytes back to base64 strings as we do not know the encodings [#13](https://github.com/singer-io/tap-mongodb/pull/13)

## 0.1.1
  * During key-based incremental sync, if replication-key changes, wipe state and resync table [#10](https://github.com/singer-io/tap-mongodb/pull/10)
  * Only support replication keys of types `datetime`, `timestamp`, `integer`, `ObjectId` [#10](https://github.com/singer-io/tap-mongodb/pull/10)
  * Only discover databases the user has read access for [#11](https://github.com/singer-io/tap-mongodb/pull/11)

## 0.1.0
 * Added key-based incremental sync [commit](https://github.com/singer-io/tap-mongodb/commit/b618b11d91e111680f70b402c6e94c9bf40c7b8f)

## 0.0.5
 * Fixed bug in oplog projections [commit](https://github.com/singer-io/tap-mongodb/commit/b400836678440499d4a15fb7d5b0a40a13e3342e)

## 0.0.4
 * Fixed bug in oplog projections [commit](https://github.com/singer-io/tap-mongodb/commit/527287e69661e9dbce3f05696b269025d0fc4034)
 * Added metric log printout at end of tap run [commit](https://github.com/singer-io/tap-mongodb/commit/d0403d82028b1dcc9ba306b52b2103ef00188b7d)
