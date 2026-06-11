---
title: "CDC 파이프라인의 숨겨진 비용: Small File이 만드는 S3 Request 폭탄"
date: 2026-06-11
draft: true
tags: ["cdc", "s3", "cost-optimization", "debezium", "flink"]
description: "Athena 비용은 스캔량이 전부가 아닙니다. CDC 파이프라인이 만든 수백만 개의 Small File이 S3 Request 비용과 쿼리 성능을 어떻게 잠식하는지, 그리고 어떻게 해결했는지를 공유합니다."
---

[CDC 파이프라인을 Debezium과 Flink로 재설계한 이유](https://blog.afinit.com/cdc-pipeline-debezium-flink)에서 Debezium + Flink 기반 CDC 파이프라인을 소개했습니다. Debezium이 DB 변경분을 캡처하고, Kafka를 거쳐, Flink가 5분마다 S3에 Parquet 파일로 저장하는 구조입니다.

파이프라인은 1년 넘게 잘 동작했습니다. 데이터 정합성도 검증됐고, 운영도 안정적이었습니다. 문제는 데이터 플랫폼 TCO 분석을 하면서 S3 비용을 뜯어보기 전까지는 보이지 않았습니다.

---

## S3 비용, 스캔량만 보면 안 됩니다

AWS에서 Athena를 쓰는 팀이라면 대부분 이렇게 알고 있습니다:

> "Athena 비용 = 스캔한 데이터량 × $5/TB"

틀린 말은 아닙니다. 하지만 이건 Athena **서비스 요금**일 뿐입니다. 실제로 Athena가 쿼리를 실행할 때 일어나는 일은 이렇습니다:

1. 대상 파일 목록을 S3에서 조회합니다 (LIST)
2. 각 파일을 열어서 읽습니다 (GET)
3. 결과를 S3에 저장합니다 (PUT)

**GET request 1,000건당 $0.0004** (ap-south-1 기준) — 작아 보이지만, 파일이 수십만 개면 얘기가 달라집니다.

<!-- TODO: 회사 비용 수치 공개 여부 확인 필요. 절대 금액 제거하고 비율/정규화(첫 달=100)로 변환하는 방안 검토 -->
저희 데이터 플랫폼의 S3 비용을 API operation별로 뜯어봤습니다:

| 월 | Storage | GetObject | 총 비용 | GET 비율 |
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

10개월 사이 스토리지는 45% 늘었지만, **GetObject 비용은 825% 폭등**했습니다. S3 비용의 절반 가까이가 파일을 읽는 데서 발생하고 있었습니다.

---

## 범인을 찾기까지

비용이 어디서 나오는지 정확히 파악하려면 "어떤 경로의 파일이 얼마나 읽히는지"를 알아야 합니다. 문제는 Cost Explorer가 S3 서비스 전체의 비용만 보여준다는 것입니다 — 어떤 버킷이, 어떤 경로가 비용을 유발하는지는 알 수 없습니다.

이걸 해결하기 위해 세 가지를 조합했습니다.

### 1. S3 Cost Allocation Tag + Cost Explorer

S3 버킷에 태그를 달고 (예: `purpose: datalake`), Billing 콘솔의 Cost Allocation Tags에서 해당 태그를 활성화합니다. 활성화하지 않으면 Cost Explorer에서 태그별 필터링이 되지 않습니다. 활성화 후 Cost Explorer에서 태그별로 그룹핑하면 버킷 단위로 스토리지 vs Request 비용 비율을 파악할 수 있습니다.

### 2. S3 Storage Class Analysis

버킷 내 prefix별 접근 패턴을 분석할 수 있는 기능입니다. 콘솔에서는 제한된 시각화만 제공하지만, destination bucket을 설정하면 날짜별로 prefix별 GetRequestCount, DataRetrieved_MB, Storage_MB 등을 CSV로 export할 수 있습니다. 매일 append되므로 추이 분석이 가능합니다.

29개 prefix에 대해 분석 그룹을 만들어 돌렸습니다. 동시에 여러 최적화가 진행되고 있었기 때문에, 다른 요인을 배제하기 위해 compaction 직전 안정 구간(6/5~6/8)의 평균을 기준으로 분석했습니다:

> **주의:** GetRequestCount 컬럼은 이름과 달리 GET + PUT 합산입니다. [AWS 문서](https://docs.aws.amazon.com/AmazonS3/latest/userguide/analytics-storage-class.html)에도 mislabelled라고 명시되어 있으니, 정확한 비용 산정 시 이 점을 감안해야 합니다.

| Prefix | Requests (M/day) | 일간 조회량 | 비율 |
|--------|------------|-----------|------|
| truecredits_changes | 718 | 117 TB | 92.7% |
| truecredits.db | 14 | 12 TB | 1.9% |
| tb_user.db | 9 | 7 TB | 1.2% |
| 나머지 26개 prefix | 34 | 32 TB | 4.2% |
| **합계** | **775** | **168 TB** | |

**truecredits_changes 하나가 전체 request의 93%를 차지**하고 있었습니다.

### 3. 스키마별 파일 분석

Storage Class Analysis가 비용의 "어디"를 알려줬다면, "왜"를 알기 위해서는 실제 파일 구조를 봐야 했습니다. S3의 CDC 경로를 분석했습니다:

| 지표 | 값 |
|------|------|
| 총 파일 수 (1주일분) | 454,572 |
| 총 크기 | 145 GB |
| 평균 파일 크기 | 335 KB |
| 100KB 미만 비율 | 79% |

평균 335KB. Parquet 파일로 운영하기에는 명백히 비효율적인 크기였습니다.

---

## Small File은 왜 생기는가

이건 CDC 파이프라인의 구조적 문제입니다.

Flink는 checkpoint interval마다 파일을 커밋합니다. 저희 설정은 5분입니다. 하루 288번, 테이블 300개 이상, 각 테이블마다 dt 파티션 — 이걸 곱하면 하루에만 수만 개의 파일이 생깁니다.

그리고 이 파일들은 "그 5분 동안 해당 테이블에 발생한 변경분"입니다. 대부분의 테이블은 5분에 수십~수백 건밖에 안 바뀝니다. 결과적으로 수 KB에서 수백 KB짜리 파일이 끝없이 쌓이게 됩니다.

파이프라인 가동 약 1년 2개월, CDC _changes 경로에 **3,140만 개의 파일**이 쌓여 있었습니다.

---

## 비용뿐만이 아닙니다

Small File 문제는 비용만의 문제가 아닙니다.

S3에서 파일을 읽으려면 파일마다 HTTP connection을 열어야 합니다. 128MB 파일 1개를 읽는 것과 335KB 파일 400개를 읽는 것은 전송할 데이터 총량은 비슷해도 **오버헤드가 수백 배** 다릅니다.

- 파티션/prefix의 파일 목록을 조회한 뒤, 각 object를 개별적으로 열어야 함
- 파일마다 GET 또는 range GET, Parquet footer/metadata 확인, schema 처리, task scheduling 오버헤드 발생
- connection 재사용이 안 되면 TCP handshake + TLS 협상까지 매번 발생

Athena든 Spark든, 엔진이 아무리 빨라도 수십만 개 파일을 열어야 하면 느려질 수밖에 없습니다.

---

## 실제로 얼마나 느려지는가

실제로 얼마나 차이가 나는지 확인하기 위해 벤치마크를 진행했습니다. S3 versioning으로 보존된 compaction 전 원본 파일(45,008개)을 별도 버킷에 복원하고, compaction 후 파일(310개)과 동일한 Athena 쿼리로 비교했습니다. 두 데이터셋의 row count는 59,184,128건으로 완전히 일치합니다.

```sql
-- Q1: 전체 COUNT (155일)
SELECT count(*)
FROM tc_loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
```
→ Before: 4,542ms / After: 1,403ms (**-69%**)

```sql
-- Q2: 필터 COUNT
SELECT count(*)
FROM tc_loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
  AND status = 'CLOSED'
```
→ Before: 2,138ms / After: 1,041ms (**-51%**)

```sql
-- Q3: GROUP BY 집계
SELECT status, count(*)
FROM tc_loan
WHERE dt >= '2026-01-01' AND dt <= '2026-06-05'
GROUP BY status
ORDER BY 2 DESC
```
→ Before: 2,752ms / After: 1,243ms (**-55%**)

```sql
-- Q4: 단일 날짜 COUNT
SELECT count(*)
FROM tc_loan
WHERE dt = '2026-03-01'
```
→ Before: 826ms / After: 560ms (**-32%**)

| 쿼리 | Before (45,008 files) | After (310 files) | 개선 |
|------|----------------------|-------------------|------|
| Q1. COUNT(*) 전체 | 4,542ms | 1,403ms | **-69%** |
| Q2. COUNT + 필터 | 2,138ms | 1,041ms | **-51%** |
| Q3. GROUP BY 집계 | 2,752ms | 1,243ms | **-55%** |
| Q4. 단일 날짜 COUNT | 826ms | 560ms | **-32%** |

전체 스캔, 필터, 집계 쿼리 전반에서 **32~69% 성능 개선**이 확인되었습니다. 스캔해야 할 데이터 총량은 동일하지만, 파일을 여는 오버헤드만으로 이 정도 차이가 발생합니다.

---

## 해결: Daily Compaction

해결 방법은 세 가지를 검토했습니다.

**1. Flink checkpoint interval 증가**
- 장점: 가장 단순함
- 단점: 데이터 freshness 저하, 비즈니스 모니터링·CDC 기반 복제 등 다운스트림 전체에 영향

**2. Flink → Iceberg 직접 적재**
- 장점: table format 차원에서 small file 관리 가능
- 단점: Flink sink 재설계 필요, migration 리스크 큼

**3. 사후 Daily Compaction**
- 장점: 기존 ingestion 경로 변경 없음, 점진적 적용 가능
- 단점: 추가 batch 비용, 원본 파일 교체 시 안정성 검증 필요

3번을 선택했습니다. 기존 파이프라인을 건드리지 않고, 가장 낮은 리스크로 같은 효과를 낼 수 있는 방법이었습니다:

- 파티션(dt) 단위로 모든 Small File을 읽어 128MB 단위로 재작성
- binlog position 기반 정렬 보존
- Airflow에서 매일 CDC 적재 완료 후 트리거

Compaction은 원본 파일을 직접 교체하는 작업이므로, 데이터를 잃지 않기 위한 안전 장치가 필요합니다. Safe Swap 패턴을 적용했습니다:

1. compaction 결과를 임시 경로(`_tmp_compact/`)에 먼저 작성
2. 임시 경로에 파일이 정상 생성되었는지 검증
3. 검증 통과 후 원본 삭제 → 임시 파일을 원래 경로로 복사 → 임시 파일 삭제

만약 3단계 중간에 장애가 발생하더라도, `_tmp_compact/`에 데이터가 남아있어 복구할 수 있습니다. 또한 compaction 전후 row count를 비교하여 데이터 유실이 없는지 검증합니다.

다만 S3는 원자적 rename/replace를 제공하지 않기 때문에, active partition에는 적용하지 않았습니다. CDC 적재가 완료된 전날 파티션만 대상으로 삼고, 쿼리와 충돌 가능성이 낮은 시간대에 실행했습니다.

단일 파티션 테스트 결과: **72,885개 → 839개 (99% 감소)**

520일치 백필도 진행했습니다. 3,140만 개 파일이 36.9만 개로 줄었습니다 (98.8% 감소).

---

## 결과

<!-- TODO: 비용 수치 공개 여부 확인 필요 -->

| 지표 | Before | After | 변화 |
|------|--------|-------|------|
| CDC _changes 파일 수 | 31,400,000 | 369,000 | -98.8% |
| 평균 파일 크기 | 335 KB | 파티션별 상이 | |
| Compaction target size | - | 128 MB | |

Compaction 전후 변화 (Storage Class Analysis 기준):

| 지표 | Before (6/5~6/8 avg) | After (6/10) | 변화 |
|------|---------------------|-------------|------|
| _changes request 수 | 718M/day | 254M/day | -65% |
| request 비용 (추정) | ~$8,600/month | ~$3,000/month | **-$5,600/month** |

동일 prefix의 compaction 전후 request 추이를 비교했으며, 쿼리량 변동이 있을 수 있어 실제 청구액과 1:1로 대응되지는 않습니다.

현재는 매일 자동으로 전날 파티션이 compaction되므로, Small File이 다시 누적되지 않습니다.

---

## 돌이켜보면

"Athena 비용 = 스캔량"이라는 상식이 오히려 문제를 늦게 발견하게 만들었습니다. Athena 서비스 요금만 보면 월 수백 달러 수준이라 심각해 보이지 않습니다. 하지만 그 뒤에서 S3 GET request가 하루 수억 건씩 일어나고 있었고, 그 비용은 S3 청구서에 묻혀 있었습니다.

**S3에 데이터를 적재하는 스트리밍 파이프라인을 운영하고 계시다면 한 번쯤 확인해보시길 권합니다:**

1. S3 버킷의 Request 비용이 Storage 비용 대비 어느 정도인지
2. Storage Class Analysis로 어떤 prefix가 GET을 많이 유발하는지
3. CDC 경로의 평균 파일 크기가 얼마인지

답이 "Request가 40% 이상", "특정 prefix가 90% 이상", "평균 1MB 미만"이라면 — 아마 같은 문제를 겪고 있을 것입니다.

---

*CDC 파이프라인 아키텍처에 대한 상세한 내용은 [회사 블로그](https://blog.afinit.com)에서 확인할 수 있습니다.*
