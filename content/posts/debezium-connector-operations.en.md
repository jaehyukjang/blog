---
title: "Debezium Connectors Do Not Always Behave as Expected: Four Cases from Production"
date: 2026-11-09T00:00:00+09:00
draft: false
tags: ["cdc", "debezium", "kafka-connect", "data-engineering"]
description: "Four cases from operating Debezium and Kafka Connect: schema mismatches, SQL parsing failures, persistent database connections, and delayed task reassignment—and how we handled them."
cover:
  image: "/images/debezium-connector-operations-cover.png"
  alt: "Debezium connector operational semantics"
  relative: false
---

In the [previous post](https://backpressured.dev/en/posts/debezium-cdc-silent-data-errors/), I described three cases where the pipeline kept running but the data arrived differently than expected. Timestamp units changed, NULL values became defaults, and the same row appeared in both snapshot and streaming events. A running pipeline did not necessarily mean correct data.

This post covers **cases where Debezium connectors and Kafka Connect behaved differently from our operational expectations**.

A connector stopped after a schema change. Collection also stopped when the parser could not handle a privilege statement unrelated to table structure. A connector stayed connected to the old database after a DNS change, and tasks did not immediately move to another worker when a worker went down. The causes and responses differed, but they raised a common question: **how do Debezium and Kafka Connect handle each situation, and how far does a setting or command actually reach?**

I will walk through four operational cases, then briefly discuss duplicate events during restarts and why we are evaluating exactly-once support.

---

## 1. A Connector Suddenly Stops: A Schema History Mismatch

While a column was being added to a production database table, a Debezium connector stopped collecting changes with the following error.

```text
io.debezium.DebeziumException: Error processing row in …,
internal schema size 15, but row size 16, restart connector with schema recovery mode.
```

Debezium's stored schema had 15 columns, while the row read from the binlog had 16. It could not map the values to the columns, so the task entered the FAILED state.

**Debezium maintains table schemas to interpret row events in the binlog.** It initially reads the current table definitions from the database, then updates its in-memory schemas as it encounters DDL in the binlog. It uses those schemas to map row values to columns and construct the events sent to Kafka.

The **schema history topic** stores the initial schemas and subsequent changes. Debezium does not look up this topic for every event. On restart, it reads the stored history to reconstruct the schema appropriate for the position from which collection will resume.

The DBA team was using an online schema change approach. They copied the data into a replacement table that incorporated the new column and its initial values, then swapped it into place using `RENAME TABLE`.

A schema mismatch occurred during this table replacement. The table with the new schema took over the original name, but Debezium still held the old 15-column schema for that name. As a result, it could not interpret the 16-column rows arriving from the replacement table.

We applied the following setting to recover.

```json
{
  "snapshot.mode": "recovery"
}
```

`recovery` reads the current table definitions from the database. The [official documentation](https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-property-snapshot-mode) warns against using this mode if the database schema changed after the connector was shut down.

In this case, the table had already been replaced with the new schema when Debezium stopped while trying to interpret the data using the old schema. Reading the replacement table's structure with `recovery` resolved the mismatch, and collection resumed.

---

## 2. A Privilege Statement Stops Collection Without Changing a Table

In the first case, collection stopped because Debezium's stored schema did not match the row structure. This time, it stopped while processing a `GRANT` statement that assigned privileges to a database user.

Debezium reads row change events from the binlog to capture data, and parses SQL statements to track schema changes. The SQL it parses includes not only `CREATE` and `ALTER`, but also privilege statements such as `GRANT`. Even a statement unrelated to table structure can therefore stop collection if Debezium's parser cannot handle its syntax.

This problem occurred during a MySQL upgrade. In the MySQL 8.4.6 environment, a `GRANT` statement containing the `ALLOW_NONEXISTENT_DEFINER` privilege was recorded in the binlog, but the Debezium parser we were using could not process it. **The SQL was valid in MySQL, yet a parser compatibility issue stopped CDC collection.**

At the time, we applied the following setting to skip statements that could not be parsed.

```json
{
  "schema.history.internal.skip.unparseable.ddl": true
}
```

After applying it, the connector skipped the problematic statement and resumed collection.

The important caveat is that **this option does not selectively skip only statements unrelated to table structure**. If a statement that adds or modifies a column fails to parse, skipping it can cause Debezium to miss a necessary schema change. This is also why the default is `false`. The [official documentation](https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-property-schema-history-internal-skip-unparseable-ddl) warns that this setting can lead to data loss or incorrect interpretation.

The option skips statements that fail to parse; it does not decide whether skipping them is safe. Those caveats need to be reviewed before applying it in production.

While preparing this post, I also found [`schema.history.internal.ddl.filter`](https://github.com/debezium/debezium/blob/v3.0.6.Final/debezium-connector-binlog/src/main/java/io/debezium/connector/binlog/BinlogStreamingChangeEventSource.java#L724), an internal setting that excludes specific SQL statements before parsing. It is another option to evaluate when handling this kind of parsing failure.

---

## 3. DNS Changes, but the Connector Stays Connected to the Old Database

During a database primary switchover, we needed to change the DNS record for the hostname Debezium used so that it pointed to a new database node. Initially, we expected that pausing the connector, changing DNS, and resuming it would connect it to the new node.

But the existing TCP connection to the database remained open while the connector was paused. Changing DNS and resuming the connector did not change the connection target.

**Pause stops message processing without shutting down the tasks, and leaves the resources held by those tasks allocated.** Resuming processing and establishing a new database connection were separate operations.

To establish a new connection, we verified the DNS change and restarted the connector with `includeTasks=true`, including its tasks in the restart.

```http
POST /connectors/{name}/restart?includeTasks=true
```

The key detail was **including the tasks in the restart**. According to the [Kafka Connect documentation](https://kafka.apache.org/35/kafka-connect/user-guide/), `includeTasks` defaults to `false`. Calling the restart API without that option restarts only the connector instance, not its tasks.

In this operation, we confirmed that the connector connected to the new database node and resumed collection after the restart that included its tasks.

---

## 4. A Worker Goes Down, but Its Tasks Do Not Move: The Five-Minute Rebalance Delay

Running Kafka Connect in distributed mode, we expected another worker to take over tasks immediately when a worker node went down. In practice, collection for that node stopped, and the tasks took time to be reassigned.

The reason was a setting that delays reassignment after a worker leaves. In Kafka Connect's incremental cooperative rebalancing, [`scheduled.rebalance.max.delay.ms`](https://kafka.apache.org/36/kafka-connect/administration/) sets the maximum time reassignment can be delayed. Its default is **five minutes (300000 ms)**. We had not overridden it, so we were using the default.

This behavior reduces unnecessary task movement when a worker leaves briefly for a restart or upgrade. If the worker does not return, however, the wait can delay the resumption of collection. Failure detection and task startup also take time, so the setting is not an upper bound on total recovery time.

In production, the delay needs to balance the convenience of rolling restarts against the required recovery time.

---

## Still Under Evaluation: At-Least-Once and Exactly-Once

After restarting a connector, we also had to check for duplicate events, not just connection and collection status. When collection resumes from a stored offset, events that have already been delivered can be sent again.

So far, we have handled duplicates downstream with primary-key-based merges. After learning that [Debezium 3.3 added Kafka Connect-based exactly-once support for its core connectors, including MySQL](https://debezium.io/blog/2025/10/01/debezium-3-3-final-released/), we plan to evaluate it alongside a version upgrade. We will check its scope and prerequisites, then measure the effects on throughput and latency before deciding whether to adopt it.

---

## Closing Thoughts

These were different problems, but each required understanding how Debezium and Kafka Connect work—and the scope of their behavior—to interpret the situation and decide how to respond.

Debezium remembers table schemas and parses SQL to track changes. Kafka Connect manages task execution state and resources, and follows defined procedures to reassign work when a worker leaves. Each behavior has a reason, but operating without understanding it can lead to unexpected failures or delays.

The lesson was that **understanding what a setting or command does not do matters as much as understanding what it does**. Examining that gap helped us understand the behavior we encountered and work out how to respond.
