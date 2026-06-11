---
title: "The Hidden Cost of CDC Pipelines: How Small Files Create an S3 Request Bomb"
date: 2026-06-11
draft: false
tags: ["cdc", "s3", "cost-optimization", "debezium", "flink"]
description: "Athena cost isn't just about data scanned. Learn how millions of small files from CDC pipelines silently drain your S3 request budget and degrade query performance — and how we fixed it."
---

In [Why We Redesigned Our CDC Pipeline with Debezium and Flink](https://blog.afinit.com/cdc-pipeline-debezium-flink), we introduced our Debezium + Flink-based CDC pipeline. Debezium captures database changes, routes them through Kafka, and Flink writes Parquet files to S3 every 5 minutes.

![CDC Pipeline Architecture](/blog/images/cdc-pipeline-architecture.png)

The pipeline worked well for over a year. Data consistency was verified, and operations were stable. The problem remained invisible until we dug into our S3 costs as part of a data platform TCO analysis.

---

## S3 Cost Is More Than Just Data Scanned

If you run Athena, you probably think of cost like this:

> "Athena cost = data scanned × $5/TB"

That's not wrong — but it's only the Athena **service charge**. Here's what actually happens when Athena runs a query:

1. Lists target files from S3 (LIST)
2. Opens and reads each file (GET)
3. Writes results back to S3 (PUT)

Among these, GET requests scale proportionally with the number of files. LIST operates at the prefix level, and PUT is once per query for result storage — but GET must open every single file. **$0.0004 per 1,000 GET requests** (ap-south-1) — sounds tiny, but not when you have hundreds of thousands of files.

We broke down our data platform's S3 costs by API operation:

![S3 Monthly Cost: Storage vs GetObject](/blog/images/s3-cost-storage-vs-get.png)

| Month | Storage | GetObject | Total | GET % |
|---|---------|-----------|---------|---------|
| 2025-07 | $28,081 | $4,308 | $36,339 | 12% |
| 2025-08 | $25,986 | $4,149 | $34,475 | 12% |
| 2025-09 | $30,083 | $4,671 | $39,414 | 12% |
| 2025-10 | $29,782 | $9,320 | $43,508 | 21% |
| 2025-11 | $31,068 | $10,267 | $45,503 | 23% |
| 2025-12 | $31,729 | $12,683 | $48,121 | 26% |
| 2026-01 | $33,607 | $20,929 | $58,832 | 36% |
| 2026-02 | $36,960 | $30,292 | $72,107 | 42% |
| 2026-03 | $42,498 | $34,147 | $83,286 | 41% |
| 2026-04 | $46,006 | $33,171 | $86,619 | 38% |
| 2026-05 | $40,788 | $39,853 | $86,726 | **46%** |

Over 10 months, storage grew 45%, but **GetObject cost surged 825%**. Nearly half of our S3 bill was from reading files.

---

## Tracking Down the Culprit

To pinpoint the cost, we needed to know "which path's files are being read how often." The problem is that Cost Explorer only shows costs at the S3 service level — it can't tell you which bucket or prefix is responsible.

We combined three approaches.

### 1. S3 Cost Allocation Tag + Cost Explorer

Tag your S3 buckets (e.g., `purpose: datalake`), then activate the tag in Billing → Cost Allocation Tags. Without activation, tags won't appear in Cost Explorer filters. Once activated, group by tag to see storage vs request cost ratios per bucket.

### 2. S3 Storage Class Analysis

This feature analyzes access patterns per prefix within a bucket. The console provides limited visualization, but by configuring a destination bucket, you can export daily CSV data with GetRequestCount, DataRetrieved_MB, Storage_MB per prefix. Data appends daily, enabling trend analysis.

We set up analysis groups for 29 prefixes. Since multiple optimizations were running simultaneously, we used the stable window just before compaction (6/5~6/8 average) as our baseline:

> **Note:** The GetRequestCount column actually includes both GET and PUT requests despite its name. [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/analytics-storage-class.html) explicitly states it is mislabelled. Keep this in mind for precise cost estimation.

| Prefix | Requests (M/day) | Daily Retrieval | Share |
|--------|------------|-----------|------|
| service_a.cdc | 718 | 117 TB | 92.7% |
| service_a.db | 14 | 12 TB | 1.9% |
| service_b.db | 9 | 7 TB | 1.2% |
| 26 other prefixes | 34 | 32 TB | 4.2% |
| **Total** | **775** | **168 TB** | |

**A single prefix — service_a.cdc — accounted for 93% of all requests.**

### 3. File-Level Analysis

While Storage Class Analysis showed us "where," we needed to look at the actual file structure to understand "why." We analyzed the CDC path on S3:

| Metric | Value |
|------|------|
| Total files (1 week) | 454,572 |
| Total size | 145 GB |
| Average file size | 335 KB |
| Files under 100KB | 79% |

335KB average. Clearly inefficient for Parquet files in production.

---

## Why Small Files Happen

This is a structural problem in CDC pipelines.

Flink commits files at every checkpoint interval. Our setting is 5 minutes. That's 288 times a day, across 300+ tables, each with dt partitions — multiply it out and you get tens of thousands of files per day.

Each file contains "changes that occurred in that table during those 5 minutes." Most tables only see dozens to hundreds of changes per 5-minute window. The result: files ranging from a few KB to a few hundred KB pile up endlessly.

After about 14 months of operation, **31.4 million files** had accumulated in the CDC path.

---

## It's Not Just About Cost

The small file problem isn't limited to cost.

Reading from S3 requires opening an HTTP connection per file. Reading one 128MB file versus 400 files of 335KB each transfers roughly the same data, but the **overhead differs by orders of magnitude**.

- After listing files in a partition/prefix, each object must be opened individually
- Each file incurs GET or range GET, Parquet footer/metadata parsing, schema processing, and task scheduling overhead
- Without connection reuse, TCP handshake + TLS negotiation occurs for every file

Whether it's Athena or Spark, no engine can be fast if it has to open hundreds of thousands of files.

---

## How We Fixed It: Daily Compaction

We evaluated three approaches.

**1. Increase Flink checkpoint interval**
- Pros: Simplest approach
- Cons: Degrades data freshness, impacts all downstream — business monitoring, CDC-based replication, and other pipelines

**2. Write directly to Iceberg from Flink**
- Pros: Table format-level small file management
- Cons: Requires Flink sink redesign, high migration risk

**3. Post-hoc Daily Compaction**
- Pros: No changes to existing ingestion path, incremental rollout possible
- Cons: Additional batch cost, requires safety verification for file replacement

We chose option 3. It delivered the same result with the lowest risk, without touching the existing pipeline:

- Read all small files per partition (dt) and rewrite as 128MB files
- Preserve binlog position ordering
- Triggered daily by Airflow after CDC ingestion completes

Compaction replaces original files directly, so safeguards against data loss were essential. We implemented a Safe Swap pattern:

1. Write compacted output to a temp path (`_tmp_compact/`)
2. Verify files were successfully created in the temp path
3. Delete originals → copy temp files to original location → delete temp

Even if a failure occurs mid-step-3, data remains in `_tmp_compact/` for recovery. We also compare row counts before and after compaction to verify no data loss.

Since S3 doesn't provide atomic rename/replace, we only target completed previous-day partitions — never active ones — and schedule execution during low-query windows.

Single partition test result: **72,885 → 839 files (99% reduction)**

We also backfilled 520 days of historical data. 31.4 million files were reduced to 369,000 (98.8% reduction).

---

## Results

| Metric | Before | After | Change |
|------|--------|-------|------|
| CDC file count | 31,400,000 | 369,000 | -98.8% |
| Average file size | 335 KB | varies by partition | |
| Compaction target size | - | 128 MB | |

Compaction impact (Storage Class Analysis):

| Metric | Before (6/5~6/8 avg) | After (6/10) | Change |
|------|---------------------|-------------|------|
| Request count | 718M/day | 254M/day | -65% |
| Request cost (est.) | ~$8,600/month | ~$3,000/month | **-$5,600/month** |

We compared request trends for the same prefix before and after compaction. Query volume fluctuations may exist, so these figures don't map 1:1 to actual billing.

Daily compaction now runs automatically on the previous day's partitions, preventing small files from accumulating again.

---

## How Much Faster Is It?

To measure the performance impact beyond cost, we ran benchmarks. We restored pre-compaction original files (45,008) from S3 versioning to a separate bucket and compared them against post-compaction files (310) using identical Athena queries. Row counts matched exactly at 59,184,128.

```sql
-- Q1: Full COUNT (155 days)
SELECT count(*)
FROM loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
```
→ Before: 4,542ms / After: 1,403ms (**-69%**)

```sql
-- Q2: Filtered COUNT
SELECT count(*)
FROM loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
  AND status = 'CLOSED'
```
→ Before: 2,138ms / After: 1,041ms (**-51%**)

```sql
-- Q3: GROUP BY aggregation
SELECT status, count(*)
FROM loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
GROUP BY status
ORDER BY 2 DESC
```
→ Before: 2,752ms / After: 1,243ms (**-55%**)

```sql
-- Q4: Single day COUNT
SELECT count(*)
FROM loan
WHERE dt = '2026-03-01'
```
→ Before: 826ms / After: 560ms (**-32%**)

| Query | Before (45,008 files) | After (310 files) | Improvement |
|------|----------------------|-------------------|------|
| Q1. COUNT(*) full | 4,542ms | 1,403ms | **-69%** |
| Q2. COUNT + filter | 2,138ms | 1,041ms | **-51%** |
| Q3. GROUP BY | 2,752ms | 1,243ms | **-55%** |
| Q4. Single day COUNT | 826ms | 560ms | **-32%** |

Across full scans, filtered queries, and aggregations, we observed **32–69% performance improvement**. The total data scanned was identical — the difference comes purely from file-open overhead.

---

## Looking Back

The common wisdom that "Athena cost = data scanned" actually delayed our discovery. Athena service charges looked like just a few hundred dollars per month — hardly alarming. But behind the scenes, hundreds of millions of S3 GET requests were firing daily, and those costs were buried in the S3 bill.

**If you're running a streaming pipeline that writes to S3, it's worth checking:**

1. How much of your S3 cost is requests vs storage
2. Whether requests are concentrated on specific prefixes (via Storage Class Analysis)
3. What the average file size is in your streaming paths

If the answers are "requests exceed 40% of total S3 cost," "requests concentrate on a single prefix," and "average file size under 1MB" — you're probably facing the same problem.

---

*For more details on the CDC pipeline architecture, see the [Afinit team blog](https://blog.afinit.com).*
