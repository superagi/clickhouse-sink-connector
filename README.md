[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Sink Connector(Kafka version) tests](https://github.com/Altinity/clickhouse-sink-connector/actions/workflows/sink-connector-kafka-tests.yml/badge.svg)](https://github.com/Altinity/clickhouse-sink-connector/actions/workflows/sink-connector-kafka-tests.yml)
[![Sink Connector(Light-weight) Tests](https://github.com/Altinity/clickhouse-sink-connector/actions/workflows/sink-connector-lightweight-tests.yml/badge.svg)](https://github.com/Altinity/clickhouse-sink-connector/actions/workflows/sink-connector-lightweight-tests.yml)
<a href="https://altinity.com/slack">
  <img src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358" alt="AltinityDB Slack" />
</a>
<img alt="Docker Pulls" src="https://img.shields.io/docker/pulls/altinityinfra/clickhouse-sink-connector">
### Latest Releases
https://github.com/Altinity/clickhouse-sink-connector/releases

# Altinity Sink Connector for ClickHouse®

The Altinity Sink Connector moves data automatically from 
transactional database tables in MySQL and PostgreSQL to ClickHouse
for analysis. 

## Features
Refer [Feature Matrix](doc/feature_matrix.md) for detailed features.

* [Initial data dump and load(MySQL)](sink-connector/python/README.md) 
* [Version History(Experimental)](doc/version_history.md)
* Change data capture of new transactions using [Debezium](https://debezium.io/)
* Automatic loading into ClickHouse
* Sources: Support for MySQL, PostgreSQL (other databases experimental)
* Target: Support for ClickHouse ReplacingMergeTree
* Able to recover/restart from failures on source or target
* Handle upstream schema changes automatically
* Checksum-based table comparisons
* Scalable to 1000s of tables
* Multiple deployment models
  * Lightweight: single process that transfers from source to target.
  * Kafka: separate source and target processes using Kafka as transport.
* Distribution as [Docker](https://hub.docker.com/layers/altinityinfra/clickhouse-sink-connector/408-97b1d3d83ef93c1b76a2b1c4d9c544dc67fbbec3-lt/images/sha256-d134bc05e50df7f63025e776ab6e3216c6622cd159eb0f2d459ea2ce8975f396?context=explore)
 container

## Getting Started

[QuickStart Guide: Lightweight (MySQL)](doc/quickstart.md)\
[QuickStart Guide: Lightweight (PostgreSQL)](doc/quickstart_postgres.md)\
[QuickStart Guide: Lightweight (Oracle)(Experimental)](doc/quickstart_oracle.md)\
[QuickStart Guide: Kafka](doc/quickstart_kafka.md)

## Blog Articles

First two are good tutorials on MySQL and PostgreSQL respectively. 

- [Altinity Sink Connector (MySQL)](https://altinity.com/blog/fast-mysql-to-clickhouse-replication-announcing-the-altinity-sink-connector-for-clickhouse)
- [Altinity Sink Connector (PostgreSQL)](https://altinity.com/blog/replicating-data-from-postgresql-to-clickhouse-with-the-altinity-sink-connector)
- [ClickHouse as an analytic extension for MySQL](https://altinity.com/blog/using-clickhouse-as-an-analytic-extension-for-mysql?utm_campaign=Brand&utm_content=224583767&utm_medium=social&utm_source=linkedin&hss_channel=lcp-10955938)

## Reference Documentation

### General 

* [Architecture Overview](doc/architecture.md)
* [Lightweight Sink Connect CLI](doc/sink_connector_cli.md)
* [Connection Pool](doc/connection_pool.md)
* [Mutable Data Handling](doc/mutable_data.md)
* [ClickHouse Table Engine Types](doc/clickhouse_engines.md)
* [Troubleshooting](doc/Troubleshooting.md)
* [TimeZone and DATETIME/TIMESTAMP](doc/timezone.md)
* [Replication Start Position](doc/replication_start_position.md)
* [Logging](doc/logging.md)
* [Production Setup](doc/production_setup.md)
* [Adding new tables(Incremental Snapshot)](doc/incremental_snapshot.md)
* [Multiple Connectors](doc/multiple_connectors.md)
* [Configuration](doc/configuration.md)
* [State Storage](doc/state_storage.md)
* [Data Type Mapping](doc/data_types.md)

### Operations

* [Monitoring](doc/Monitoring.md)
* [Load Testing with Sysbench](doc/Performance.md)

### Development

* [Development](doc/development.md)
* [Testing](doc/TESTING.md)

## Comparison with other technologies
- [Comparison](doc/comparison.md)

## Roadmap 

[2025 Roadmap](https://github.com/Altinity/clickhouse-sink-connector/issues/401)

## Help

File an issue or contact us on the Altinity public Slack workspace. Use 
the link on the Slack badge at the top of this page. 

## Contributing

Contributions to the project are welcome in any form. 

* Submit issues documenting feature requests and bugs
* Submit PRs to make changes
* Talk about the project, write blog articles, or give presentations

We recommend that you file an issue before implementing feature additions 
or major fixes. We are happy to provide guidance and encouragement!

## Commercial Support

Altinity is the primary maintainer of the Sink Connector. It is used
together with Altinity.Cloud as well as self-managed ClickHouse
installations.  Altinity.Cloud and is also used in self-managed
installations. Altinity offers a range of software and services related
to ClickHouse and analytic applications built on ClickHouse. 

- [Official website](https://altinity.com/) - Get a high level overview of Altinity and our offerings.
- [Altinity.Cloud](https://altinity.com/cloud-database/) - Run ClickHouse in our cloud or yours.
- [Altinity Support](https://altinity.com/support/) - Get Enterprise-class support for ClickHouse and Sink Connector.
- [Slack](https://altinity.com/slack) - Talk directly with ClickHouse users and Altinity devs.
- [Contact us](https://hubs.la/Q020sH3Z0) - Contact Altinity with your questions or issues.
- [Free consultation](https://hubs.la/Q020sHkv0) - Get a free consultation with a ClickHouse expert today.

### Credits
YourKit supports the Altinity Sink Connector and other open source projects with its full-featured Java Profiler.  Click the YourKit logo below to learn more.<br/>
[![](https://www.yourkit.com/images/yklogo.png)](http://www.yourkit.com/java/profiler/index.jsp)<br/>


[7 tools called]

Here is a complete reference of all configuration parameters from the Altinity ClickHouse Sink Connector source code:

## Complete Configuration Reference

### ClickHouse Connection Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `clickhouse.server.url` | String | `localhost` | ClickHouse hostname |
| `clickhouse.server.port` | Int | `8123` | ClickHouse HTTP port |
| `clickhouse.server.user` | String | - | ClickHouse username |
| `clickhouse.server.password` | String | - | ClickHouse password |
| `clickhouse.server.database` | String | - | Target database name |

### Buffer and Batching Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `buffer.count` | Long | `100` | Number of records to buffer before flush |
| `buffer.flush.time.ms` | Long | `30000` (30s) | Time interval to flush buffer |
| `buffer.flush.timeout.ms` | Long | `1000` | Timeout for flush operation |
| `buffer.max.records` | Long | `100000` | Max records in buffer before forced flush |
| `buffer.count.records` | - | `5000` | Alternative buffer count setting |

### Thread Pool and Queue Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `thread.pool.size` | Int | `10` | Number of threads for batch processing |
| `sink.connector.max.queue.size` | Int | `500000` | Maximum queue size |
| `single.threaded` | Boolean | `false` | Run in single-threaded mode |

### Connection Pool Settings (HikariCP)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection.pool.max.size` | Int | `500` | Max connections in pool |
| `connection.pool.timeout` | Long | `50000` | Connection timeout (ms) |
| `connection.pool.min.idle` | Int | `10` | Min idle connections |
| `connection.pool.max.lifetime` | Long | `300000` | Max connection lifetime (ms) |
| `connection.pool.disable` | Boolean | `false` | Disable connection pooling |

### Deduplication Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `deduplication.policy` | String | `OFF` | Values: `OFF`, `OLD`, `NEW`. Controls in-memory deduplication |

### Table Creation and Schema

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `auto.create.tables` | Boolean | `false` | Auto-create tables in ClickHouse |
| `auto.create.tables.replicated` | Boolean | `false` | Create ReplicatedReplacingMergeTree tables |
| `schema.evolution` | Boolean | `false` | Enable schema evolution (add columns) |

### Kafka Metadata Storage

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store.kafka.metadata` | Boolean | `false` | Store `_topic`, `_partition`, `_offset` columns |
| `enable.kafka.offset` | Boolean | `false` | Store offsets in ClickHouse (instead of Kafka) |
| `kafka.offset.metadata.table` | String | `topic_offset_metadata` | Table for offset storage |

### Raw Data Storage

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store.raw.data` | Boolean | `false` | Store raw JSON in a column |
| `store.raw.data.column` | String | - | Column name for raw data |

### Topic/Table Mapping

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `clickhouse.topic2table.map` | String | - | Format: `topic1:table1,topic2:table2` |
| `clickhouse.database.override.map` | String | - | Format: `src_db:dest_db` |

### Error Handling

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `errors.max.retries` | Int | `3` | Max retry attempts |
| `error.logging.enable` | Boolean | `false` | Enable error logging to table |
| `default.error.table` | String | `error_table` | Table for error records |
| `ignore_delete` | Boolean | `false` | Ignore DELETE CDC events |

### ReplacingMergeTree Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `replacingmergetree.delete.column` | String | `sign` | Column for delete sign (-1/1) |
| `snowflake.id` | Boolean | `true` | Use snowflake ID for version columns |

### JDBC Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `clickhouse.jdbc.params` | String | - | Format: `socket_timeout=10000,connection_timeout=100` |
| `clickhouse.jdbc.settings` | String | - | ClickHouse settings for JDBC |

### Timezone Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database.connectionTimeZone` | String | - | Source DB timezone |
| `clickhouse.datetime.timezone` | String | - | ClickHouse DateTime timezone |

### Replication History

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `replication.history.enable` | Boolean | `false` | Enable history tracking |
| `replication.history.table.name` | String | `history` | History table name |
| `replication.history.database.name` | String | `binlog_history` | History database |
| `replication.history.ttl` | Int | `30` | TTL in days |

---