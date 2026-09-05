---
title: "Debezium Does Not Replicate the Source As-Is: Three Ways Data Can Drift Quietly"
date: 2026-09-05
draft: false
tags: ["cdc", "debezium", "mysql", "data-engineering"]
description: "A CDC pipeline can run without errors while the data it produces no longer has the meaning or shape you expected. These are three quiet data mismatches we ran into while operating Debezium in production."
cover:
  image: "/images/debezium-silent-errors-cover.png"
  alt: "Debezium CDC silent data errors"
  relative: false
---

In the company tech blog, I previously wrote about [redesigning our CDC pipeline with Debezium and Flink](https://blog.afinit.com/cdc-pipeline-debezium-flink), and later about [moving our replication model toward CDC-based incremental processing](https://blog.afinit.com/cdc-incremental-replication). The structure was simple: Debezium captured changes from MySQL, Kafka carried the events, and Flink replicated them into S3 and Iceberg tables.

I will skip the architecture and the reasons behind that design here. This post is about what happened after that.

The issues we ran into while operating Debezium fell roughly into two groups. One was data quietly drifting from what we expected. The other was connector behavior that did not match our operational assumptions. In this post, I will focus on the first group: cases where the data itself ended up different.

At first, I thought about CDC like this.

> A row in MySQL goes through Debezium, and the same change comes out as an event.

In production, it was not that simple. **A CDC pipeline passes through multiple layers of defaults and interpretation, and the meaning of a value or the shape of an event can change along the way.** Some problems had to be corrected in the consumer. Others required us to use a more conservative capture strategy. These are the three cases that made that clear to me: type interpretation, default handling during serialization, and the boundary between snapshot and streaming.

---

## 1. The Same int64 Became 1000x Larger: datetime(6)

The first issue was with timestamps. Some `datetime` columns from the source database arrived in the data lake correctly, while others jumped to a time far in the future.

The Kafka message schema looked like this.

```json
{
  "field": "created_at",
  "type": "int64",
  "name": "io.debezium.time.MicroTimestamp"
}
```

There are two fields that matter here.

- `type` is the **physical type**. It tells you what container the value is stored in. Here, it is `int64`.
- `name` is the **logical type**. It tells you what that `int64` actually means.

The problem was that Debezium emits MySQL temporal types with **different logical types and different units** depending on their precision.

| MySQL type | Debezium logical type (`name`) | Physical type (`type`) | Unit |
|---|---|---|---|
| `DATETIME` to `DATETIME(3)` | `io.debezium.time.Timestamp` | `int64` | milliseconds |
| `DATETIME(4)` to `DATETIME(6)` | `io.debezium.time.MicroTimestamp` | `int64` | microseconds |

With the default `time.precision.mode=adaptive_time_microseconds`, `DATETIME(0)` to `DATETIME(3)` is serialized as `Timestamp` in epoch milliseconds, while `DATETIME(4)` to `DATETIME(6)` is serialized as `MicroTimestamp` in epoch microseconds. Both use the same physical type: `int64`.

In our case, there were many columns coming through Debezium. We saw timestamp columns with physical type `int64` and mapped them all as milliseconds. That assumption worked for most `datetime` columns, but not for `datetime(6)`. Those values were in microseconds, so they became 1000 times larger and ended up as timestamps far in the future.

The fix was not to look at the value itself, but to use the **logical type (`name`) to choose the unit**. In the Flink consumer, we converted only `MicroTimestamp` fields by dividing them by 1000, so all timestamp columns ended up in a single representation.

```scala
// Choose the unit from the logical type.
case Some("io.debezium.time.MicroTimestamp") => value.asLong() / 1000
case _                                       => value.asLong()
```

Later, I learned that [`time.precision.mode`](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types) can change how temporal types are handled. Setting it to `connect` can align values to millisecond precision, but it can also lose microsecond precision. If you keep the default mode, the consumer must read the logical type and convert accordingly.

That was when I stopped trusting `type` alone in CDC schemas. Even when the physical type is the same, the meaning and unit of the value may only be visible in the logical type.

---

## 2. The DB Had NULL, but CDC Had 0

The second issue was more confusing. A column was `NULL` in the source database, but after passing through CDC it appeared as `0`. The column was defined like this.

```sql
`is_ivr_required` tinyint DEFAULT '0',
```

It was a nullable column added later, and existing rows still had `NULL` for that column. At the time we checked it, the column definition had `DEFAULT 0`, but querying the source database directly clearly returned `NULL`. Only the CDC result had `0`.

At first, I suspected the type converter. MySQL `tinyint(1)` can be interpreted as a boolean, and Debezium has a `TinyIntOneToBooleanConverter` for that. I thought either the converter or the `database.tinyInt1isBit` setting was changing the value.

So we tested three combinations.

1. Keep the converter + add `tinyInt1isBit`
2. Remove the converter + add `tinyInt1isBit`
3. Remove the converter

The result was the same in all three cases. The converter was not the cause. The fact that the column was `tinyint` was almost incidental.

The first thing we checked was whether Debezium was receiving that column value in the first place. If `binlog_row_image=MINIMAL` is used, MySQL may write only the columns needed for the change, so it is possible to suspect that the column was missing from the binlog event. But our setting was `FULL`, and row events were expected to contain the full set of column values. At least, this was not a problem caused by the column being omitted from the change row. From there, we narrowed the cause down to how `NULL` and schema defaults were handled in the event schema and serialization layer.

The real issue was this: **when a nullable column has a DEFAULT, a NULL value can be replaced by that DEFAULT during serialization or downstream Connect data handling**.

In the event schema Debezium creates, a nullable column with `DEFAULT 0` becomes a field that is optional and has a default value of 0. Roughly:

```json
{
  "field": "is_ivr_required",
  "type": "int16",
  "optional": true,
  "default": 0
}
```

Even if the actual field value is `NULL`, a Kafka Connect converter or a downstream reader of Connect data can apply the schema default and turn it into `0`. In the source database, `NULL` and `0` mean different things. But both are valid values, so this can pass through the pipeline without any error.

At first, I thought Debezium itself was changing the value. Looking into it, I found that a similar symptom had once been fixed in Debezium core ([DBZ-1064](https://issues.redhat.com/browse/DBZ-1064)). In our environment, however, the more direct layer to check was Kafka Connect's `JsonConverter`. We were using the JSON converter with schemas enabled. Since Kafka 3.5.0, Kafka Connect has had a [`replace.null.with.default`](https://cwiki.apache.org/confluence/display/KAFKA/KIP-581%3A+Value+of+optional+null+field+which+has+default+value) option to control whether a nullable field's `NULL` should be replaced by its schema default. The default remains `true` for backward compatibility.

So the value intended by the source and the value produced after CDC were different. This was not a missing column before Debezium. It was closer to a problem where the serialization layer interpreted `NULL` as the schema default `0`.

Looking back, we could have checked `value.converter.replace.null.with.default=false` first. At the time, we did not know about that option. Our first thought was to remove `DEFAULT` from the source column. Without a schema default, there is less room for `NULL` to be turned into `0` in the same way. But similar columns were spread across many tables, and changing production source schemas all at once carried its own risk. In practice, the immediate priority was to reduce the mismatch between the existing full-load path and the CDC path, so we normalized the two paths in the consumer to use the same representation. If the original meaning of `NULL` must be preserved, though, the right place to handle it is the converter layer, not the consumer.

The lesson here was that the value you query from the source and the value you get after CDC are not always the same. Especially with values like `NULL` and `0`, whose meanings are different, a pipeline can quietly swap one for the other.

---

## 3. The Same Row Appeared as Two Events: Snapshot Boundaries

The first two cases changed the final value while interpreting or processing data. The third was different. The value itself did not change, but the shape of the events was not what we expected. The same row appeared both as a snapshot event created to fill the current state and as a streaming event that came later from the binlog.

The simplest situation would have been collecting CDC from the beginning, when the schema was first created. In real operations, however, we sometimes need to onboard an existing schema into CDC later. By then, older binlogs may already have expired, so we cannot replay every historical change from the beginning. Our approach was to use an initial snapshot to load the current state, then continue CDC from that point onward. While testing different `snapshot.locking.mode` settings, we observed that with `none`, the same PK appeared as two events in raw CDC.

`snapshot.locking.mode` controls how Debezium uses database locks during the initial phase of an initial snapshot. The default, `minimal`, uses a global read lock only while capturing the initial schema, metadata, and binlog position. After that, the row scan is kept consistent through a `REPEATABLE READ` transaction. `none` skips that initial lock.

In the Debezium documentation, the main warning for `none` is about schema changes during a snapshot. But the issue we saw happened even without schema changes. When we ran the initial snapshot with `none`, the same PK appeared both as a snapshot event (`read`) and as a streaming event (`insert`). After switching to `minimal`, we did not observe the same behavior again.

| mode | Snapshot lock | What we observed |
|---|---|---|
| `minimal` | Used only in the initial phase | No duplicate |
| `none` | Not used | Same PK appeared as both `read` and `insert` |

That does not mean `none` always creates this kind of duplicate. But in a setup like ours, where an existing schema is onboarded later and an initial snapshot is stitched together with subsequent CDC streaming, the snapshot boundary can affect how raw CDC should be interpreted.

The important part was not the duplicate itself, but what the downstream system thought those events meant. A Debezium snapshot event (`read`) does not mean an INSERT actually happened in the database. It means "this row existed at the snapshot point." A streaming event (`insert`/`update`), on the other hand, is an actual change event read from the binlog.

If both event types exist for the same PK, a consumer reading raw CDC will see both. Here, raw CDC means the source event log before it is merged into a replicated table. A replication path may be able to absorb this kind of duplication, but a consumer reading raw CDC directly sees a different story. Raw CDC is not one current state per row. It is a change log where multiple events can exist for the same PK.

After that, we used `minimal`, which is also Debezium's default, when onboarding new schemas. In our environment, after moving to `minimal`, we did not observe the earlier `read`/`insert` duplication again. Because the lock is only held during the initial phase, it also carried less operational burden than `extended`, which can block writes for the entire snapshot.

Since then, I have been more careful when treating initial snapshot output as part of a raw change log. A snapshot is not just "reading the current rows once." It is a boundary that needs to be interpreted together with the streaming events that follow it.

---

## Closing

The three issues looked unrelated on the surface: timestamp units, default handling during serialization, and snapshot boundaries. But they had the same shape. The pipeline was running, but the data was not arriving in the form we expected.

We often imagine CDC like this.

> What is in the DB -> binlog -> Debezium -> replicated as-is

In reality, several layers sit in between. How timestamp units are interpreted, how the serialization layer handles `NULL`, how the boundary between snapshot and streaming is represented. At each layer, the meaning or shape of the data can shift a little.

What mattered in operating CDC was not just whether the pipeline was running. It was continuously checking whether the data was being produced with the meaning and shape we expected. Connector and converter defaults, consumer interpretation, and the boundary between snapshot and streaming can all affect the final data.

If this post was about how data can quietly drift even when CDC is flowing, the next one will be about a different kind of lesson: how Debezium and Kafka Connect actually behave in production, including schema history, the difference between pause and restart, worker failover, and rebalance.
