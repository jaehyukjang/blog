# 블로그: Debezium 실전 운영기

## 제목 후보
"Debezium CDC를 프로덕션에 넣고 나서 겪은 것들"

## 포지셔닝
회사 블로그 2편(CDC 아키텍처, Incremental Replication)의 후속.
회사 블로그 = "왜 선택했는가", 개인 블로그 = "선택한 후 실전에서 뭘 겪었는가"

## 다룰 주제
1. datetime(6) 마이크로초 정밀도 문제 (DFM-1318)
2. tinyint null 이슈 — TinyIntOneToBooleanConverter + fill_if_null (DFM-1482)
3. CDC duplicate events와 멱등성 보장 (DFM-1326)
4. 스키마 변경 자동 감지 — schema_hash 기반 (DFM-1483)
5. Debezium 장애 시 복구 전략
6. bulk job 핸들링 — TRUNCATE, 대량 UPDATE (DFM-1155)

## 관련 Jira
- DFM-1318: datetime(6) handling
- DFM-1482: NULL partition / tinyint issue
- DFM-1326: Debezium duplicate events
- DFM-1483: Auto-detect schema changes
- DFM-1155: Investigating broken case
- DFM-1274: Debezium failure due to schema change

## 강점
- 실제 운영해본 사람만 쓸 수 있는 내용
- 각 이슈가 독립적이라 시리즈로 쪼갤 수도 있음
- Debezium 사용자 커뮤니티에서 관심 높은 주제들

---

## ⚠️ 2026-08-31 재검증 — 기존 "tinyint" 프레임은 틀렸음

> 아래 "1편 목차 (2026-08-18)"는 검증 전 초안이라 부정확 (특히 지뢰 2를 tinyint로 프레임한 것).
> 실제 코드(jet-flink `SchemaGenerator.scala`) + Notion 디버깅 기록 + MySQL/Debezium 공식 문서로
> 재검증한 결과를 아래 "검증된 사건 정리"에 남긴다. 목차 초안은 히스토리로 보존.

---

## 🎯 구성 확정 (2026-08-31) — 2편 시리즈

> 8개를 한 편에 넣으면 "트러블슈팅 백과사전"이 됨 → 하나의 중심 질문으로 수렴하는 기존 글 문법 유지 위해 2편 분할.
> 시리즈명 후보: **"Debezium을 프로덕션에 넣고 나서 알게 된 것들"**

**시리즈 서사 (회사 블로그 = 설계, 개인 블로그 = 운영 현실)**
- 회사#1 (왜 CDC로 갔나 / 아키텍처) → 회사#2 (Incremental Replication 정합성) → **개인#1 (데이터가 조용히 틀린다)** → **개인#2 (커넥터 lifecycle)**
- 개인 블로그라 회사 글을 "선행편"으로 링크 → 아키텍처 반복 생략하고 사건부터 바로 진입
- 개인 블로그 강점: "잘못 짚은 가설 + 디버깅 반전"을 살림 (회사 글은 정제돼서 못 씀)

### 1편 — CDC가 에러 없이 틀리는 순간들 (Data Correctness)

- **중심 문장 (확정, 담백하게)**: **"CDC는 DB의 변경사항을 그냥 복사해주는 게 아니다. 잘 흘러간다고 데이터까지 맞는 건 아니다."**
  - 개인 블로그 톤: "처음에는 CDC를 소스의 변경사항을 그대로 전달하는 파이프라인이라고 생각했다. 그런데 운영해보니, 파이프라인이 잘 도는 것과 원본과 같은 데이터가 만들어지는 것은 다른 문제였다."
  - ⚠️ 추상어(의미 보존/경계) 전면에 내세우지 말 것. "잘 흘러간다고 데이터까지 맞는 건 아니다"가 진짜 메시지
  - ⚠️ "최종 테이블이 맞다고 CDC가 맞는 건 아니다"는 **1편 전체 결론 아님** (사건 1·2는 최종까지 틀림). 사건 8에서도 "MERGE가 가려주니 최종은 안전"으로 좁히지 말 것 — 경계/순서가 꼬이면 **최종 상태까지 어긋날 수 있음**
- 제목 후보: **"CDC는 소스를 그대로 복제하지 않는다"** (결론과 가장 잘 맞음) / "Debezium CDC 운영기 1 — 에러 없이 데이터가 틀리는 순간들"
- **사건 (타입 해석 → DB 저장방식 → snapshot 경계 순으로, 최종 데이터까지 달라질 수 있음):**
  1. **사건 1 datetime(6) — 값을 받았는데 단위를 잘못 해석**: 같은 int64여도 논리 타입(MicroTimestamp)에 따라 millis/micros로 단위가 갈림 → 잘못 해석하면 값이 1000배 틀림
  2. **사건 2 INSTANT ADD COLUMN — DB에서 보이는 상태와 CDC가 얻을 수 있는 정보가 다름**: MySQL 조회값(가상 default)과 실제 binlog가 가진 정보가 달라 **값 자체가 다르게 들어옴** (NULL이 0으로). + tinyint converter 오인 디버깅 반전
  3. **사건 8 snapshot locking — snapshot과 streaming 경계가 깔끔하지 않으면**: 같은 row가 중복/순서 꼬인 이벤트로 들어옴. 그 영향이 raw뿐 아니라 **downstream merge 결과(최종 상태)까지** 이어질 수 있음. 핵심은 중복 자체보다 **"어떤 이벤트가 최종값으로 선택되느냐"** — snapshot의 옛 상태와 streaming의 새 상태가 함께 들어왔을 때 순서 판단이 틀리면 오래된 값이 최종이 될 수도. (※ 현재 우리 구현은 hash MERGE로 흡수하지만, "MERGE가 있으니 안전"은 일반화하면 안 됨). 기존 B축 분류에서 1편으로 이동
- **결론 (세 사건 관통, 담백)**: "CDC를 운영하면서 중요했던 건 파이프라인이 잘 도는지가 아니라, 실제로 원본과 같은 데이터가 만들어지고 있는지를 계속 확인하는 일이었다" = 회사 Incremental Replication 글(방어적 정합성)의 **"왜 그런 검증이 필요했나"를 실제 사고로 보여주는** 후속

### 2편 — 커넥터는 생각대로 움직이지 않는다 (Operational Semantics)

- **중심 문장**: "운영 장애처럼 보이는 많은 현상은 버그가 아니라 Kafka Connect가 state·lifecycle을 관리하는 방식이었다."
- 제목 후보: "Debezium CDC 운영기 2 — 커넥터가 예상대로 움직이지 않을 때" / "Pause, Restart, Rebalance — 커넥터는 생각대로 움직이지 않는다"
- **사건:**
  1. **사건 3 schema history mismatch** (제일 셈, 완결된 장애 서사) — `_` 임시테이블 rename → 기억≠실제 → 죽음 → recovery도 실패 → history 토픽 리셋
  2. **사건 6 unparseable DDL** — 사건 3의 **짧은 두 번째 사례**로 붙임 ("스키마를 기억하려면 DDL을 파싱한다 → 파서가 새 문법 모르면?")
  3. **사건 4 Pause vs Restart** — Pause는 task·TCP 살아있음 → DNS 안 바뀜 / Restart는 recreate → 새 연결
  4. **사건 7 rebalance 5분** — "worker 죽으면 바로 넘어가겠지?" → 아니, 기본 5분 대기 → rolling엔 유리, 실장애엔 최대 5분 공백 trade-off
- **에필로그 (다음 단계, 장애 사례 아님)**: **사건 5 exactly-once** — 지금 at-least-once 중복을 PK merge로 흡수 중, 3.2.1+에서 EoS 지원하니 다음 업그레이드에서 평가. 단 transactional producer/read_committed 오버헤드부터 성능 확인. "답을 다 찾았다"가 아니라 "현재 선택 + 다음 판단"으로 마무리

---

## 검증된 사건 정리 (2026-08-31) — Debezium 운영 관점만

> 포커스: **Debezium(프로듀서/커넥터) 운영에서 겪은 문제**. db-streamer(Flink/Iceberg 소비자)
> 문제(NULL 파티션 MERGE 중복, small file 등)는 제외 — 별도 글감.

### 사건 1 — datetime(6): 물리 타입은 같은데 단위가 몰래 갈린다 (DFM-1318)

**현상**
- 소스 `timestamp/datetime` 값이 데이터레이크에서 시각이 어긋남 (심하면 1000배 미래)
- Notion 초기 관찰: "tx=timestamp인데 _changes=bigint" (DFM-1108 계열)

**진짜 원인 (Debezium 시간 타입 직렬화)**
- Debezium은 MySQL 시간 타입을 **정밀도(fsp)에 따라 다른 논리 타입으로** 직렬화:
  - `DATETIME` ~ `DATETIME(3)` → `io.debezium.time.Timestamp` = epoch **밀리초** (int64)
  - `DATETIME(4)` ~ `DATETIME(6)` → `io.debezium.time.MicroTimestamp` = epoch **마이크로초** (int64)
  - 경계 = fsp 4자리 (공식 문서 확인)
- **물리 타입(Kafka Connect `type`)은 둘 다 `int64`로 동일** → 값의 자릿수(millis=13 vs micros=16, 2025년 기준)로 구분은 되지만, 파이프라인이 시간 컬럼을 일괄 millis로 처리하면 `datetime(6)`만 조용히 1000배 틀어짐
- 단위 정보는 오직 스키마의 **논리 타입 이름(`name`)** 에만 있음 → `name`을 파싱해야 안전

**설계 갈림길: `time.precision.mode`**
- `adaptive_time_microseconds` (기본, 우리 커넥터도 명시 안 해서 이 값) → 정밀도별 적응 분기 (= 함정의 근원)
- `connect` → 전부 Connect 표준(millis) 통일, 대신 **마이크로초 정밀도 손실**

**우리 해결 (소비자에서 논리 타입 보고 단위 통일)**
- jet-flink `cdc-pipeline/.../schema/SchemaGenerator.scala`:
  - `addFieldToSchema` (L160-161): 스키마의 `type`(물리)/`name`(논리) 둘 다 읽음
  - 값 변환 (L82): `case Some("io.debezium.time.MicroTimestamp") => fieldValue.asLong() / 1000` → micros → millis
  - 스키마 생성 (L164, 171): `Timestamp`/`MicroTimestamp` 분기해 최종 Avro는 전부 `timestamp-millis`로 통일, MicroTimestamp는 `originalType` 태그 보존
- 배포: 2025-08-13 (Debezium pause → Flink lag 소진 확인 → 배포 → 재개, 무중단)

**"나만의 실수 아님" 근거 (블로그 훅)**
- 이 문제 전용 오픈소스: `holmofy/debezium-datetime-converter` ("deal with mysql datetime type problems")
- Debezium 공식: 시간 타입 처리는 초기부터 논쟁 (DBZ-91 / PR #86)
- Debezium 메일링에 반복 질문 ("DateTime conversion" 등)
- 구조적 이유 3가지: ① 물리 타입 다 int64 ② adaptive가 기본 ③ 에러 안 나고 통과 = 조용한 손상

**Refs**: jet-flink `SchemaGenerator.scala` L82/L160-161/L164-179 · Notion "Datetime data type mismatch in CDC" (Kafka 스키마 조각) · Debezium 공식 temporal types · MySQL fsp 0-6

---

### 사건 2 — DB엔 NULL인데 CDC엔 0: nullable + DEFAULT 컬럼의 직렬화 함정 (DFM-1105/1482)

> ⚠️⚠️ 2026-09-01 전면 재검증. 이전에 INSTANT ADD COLUMN이 원인이라 단정했으나 **틀렸음.**
> 실제 슬랙 스레드(p1754621186) + prod DB 확인 결과, INSTANT 아님. 진짜는 **converter/직렬화 계층의 default 처리**.

**현상 (검증됨)**
- `tc_loan.is_ivr_required`, `is_fv_required`: 앱이 안 쓰는 필드(2021년 추가), `tinyint DEFAULT 0`, nullable
- 앱이 값을 안 넣어 **DB엔 실제 NULL 저장** → **sqoop(SELECT)로는 NULL 그대로** 나옴
- 근데 **CDC(Debezium)로는 0**(= 스키마 DEFAULT)으로 들어옴 → sqoop NULL vs CDC 0 불일치 → tx v2(CDC 기반) 정합성 깨짐

**오인했던 범인들 (디버깅 반전 서사 — 둘 다 틀림)**
1. 처음: `TinyIntOneToBooleanConverter` + `database.tinyInt1isBit` 의심 → 3조합 실험 → **셋 다 무관** (Notion)
2. 그다음(이 노트 이전 버전): MySQL INSTANT ADD COLUMN 의심 → **이것도 틀림**
   - 반증: INSTANT면 SELECT도 가상 default(0)가 나와야 하는데, **소스 SELECT는 NULL**이었음 (모순)
   - prod 확인: 컬럼은 2021년 추가된 평범한 컬럼, 실제 NULL이 물리 저장됨, `binlog_row_image=FULL`이라 binlog에 NULL도 실림

**진짜 원인 (검증됨) — nullable + DEFAULT 컬럼의 직렬화 계층 default 처리**
- 알려진 이슈: **nullable 컬럼에 default가 정의돼 있으면, NULL 값이 이벤트에 default로 실린다**
- 근본은 **Debezium 자체가 아니라 Kafka Converter(Avro/Connect 직렬화)** — Avro/Connect 스키마에서 "optional 필드 + default value" 조합이면, NULL을 직렬화할 때 default로 대체
- Debezium은 0.9.0.CR1(2019, DBZ-1064)에서 이 방향을 고쳤으나, converter 계층에서 여전히 재발 (schema-registry #2314, kafka-connect-storage #716). `value.converter.ignore.default.for.nullables=true`로도 완전히 안 잡히는 경우 있음
- 우리 사건 인과: DEFAULT 0 정의 → 스키마가 "optional+default=0" → binlog의 NULL을 직렬화 시 0으로 대체 → CDC에 0

**검증 근거 (prod, 2026-09-01)**
- MySQL 8.4.10 (사건 당시 8.0.41 — DPM-2450: "8.0.41→8.4 upgrade"가 2025-12 시작이므로 사건 2025-08엔 8.0.41. 5.7 아님)
- `binlog_row_image=FULL`, `binlog_format=ROW` → binlog에 NULL 실제로 있음 (값이 없어서가 아님)
- 컬럼 순서: `is_ivr_required`(32)/`is_fv_required`(35)가 created_at(30)/updated_at(31)보다 뒤 = 나중에 ADD됨
- 지금 DEFAULT는 NULL (해결 적용됨), 실제 NULL 로우 실재

**우리 해결 (2갈래)**
1. 소스: **DEFAULT를 NULL로 변경** (tc_loan, m_loan) → 스키마에 default가 없어짐 → converter가 채울 게 없어 NULL 유지. 단 기존 값은 안 바뀜 (인프라팀 U01C6FXSZ2N 확인)
2. 소비자: db-streamer `jobs/initial_snapshot.py`의 `zero_if_null` normalize → sqoop 초기 스냅샷(NULL)을 0으로 맞춰 CDC(0)와 일치시킴 (양쪽 통일)

**"나만의 실수 아님" 근거 (블로그 훅)**
- Airbyte 동일 이슈: airbytehq/airbyte#28968 ("cdc replaces null value with the default value of that column") — CDC 도구 공통
- confluentinc/schema-registry#2314, kafka-connect-storage-cloud#716 — converter 계층 이슈로 등록
- Debezium이 2019년 고쳤는데도 직렬화 계층에서 재발 = 유명한 함정

**교훈 (블로그 앵글)**
- "nullable + DEFAULT가 함께 정의된 컬럼"은 CDC에서 위험 — 실제 NULL이 직렬화 시 default로 은폐됨
- 겉보기엔 타입/컨버터 문제 같지만 실은 **스키마의 optional+default 조합 × 직렬화 계층** 문제
- sqoop(SELECT)은 NULL 그대로, CDC(Debezium→converter)만 0 → **두 경로가 같은 값을 다르게 표현**하는 게 디버깅을 어렵게 함
- ⭐ 훅: "SELECT하면 NULL인데 CDC엔 0. binlog엔 NULL이 실려 있는데도(FULL) 0으로 바뀐다. 범인은 Debezium이 아니라 그 뒤 직렬화 계층이었다"

**Refs**: 슬랙 스레드 CMH626FV2/p1754621186 (앱팀: 2021년 추가·안 쓰는 필드·NULL 저장 / 본인: "CDC에서 자동 default 0" / 인프라팀: default→NULL 변경, 기존값 유지) · Notion "Fix CDC tinyint null issue" (3조합 실험) · prod DB 2026-09-01 (버전/binlog_row_image=FULL/컬럼순서) · DPM-2450 (MySQL 버전 타임라인) · Airbyte#28968 · schema-registry#2314 · db-streamer `jobs/initial_snapshot.py` (zero_if_null)

---

### 사건 3 — schema history 지옥: bulk 적재의 `_` 임시 테이블이 커넥터를 죽인다 (Notion 276c)

> 순수 Debezium 커넥터 운영 사건. 실제 에러 메시지 + 실제 해결 설정 코드로 검증됨.

**현상 (Notion 실제 스택트레이스)**
```
io.debezium.DebeziumException: Error processing row in device_history,
internal schema size 15, but row size 16, restart connector with schema recovery mode.
→ 커넥터 task FAILED
```

**원인**
- online schema change(gh-ost/pt-osc류)가 **`_` 접두 helper(임시) 테이블을 새 구조로 생성** → 대량 적재 → **rename으로 진짜 테이블을 갈아치움** (rename = 사실상 스키마 변경)
- Debezium은 각 테이블의 컬럼 구조를 **schema history 토픽**에 따로 기억함 (binlog엔 값만 있고 컬럼 구조는 없어서, 값↔컬럼 매칭에 스키마 필요)
- rename으로 `device` 구조가 바뀌었는데 history엔 옛 구조가 남음 → **기억(15컬럼) ≠ 실제 binlog row(16컬럼)** → 커넥터 FAILED
- 공식 에러 계열: "Data row is smaller than a column index, **internal schema representation is probably out of sync with real database schema**"
- 관련: bulk가 특정 테이블에 몰리면 Flink `keyBy(binlog_table)`가 단일 키 셔플 → 그 테이블 한 subtask가 다 처리 → parallelism 6→10 올려도 무효 (소비자측, 이번 글엔 부차적)

**1차 시도 실패 (에러가 시키는 recovery도 안 됨) — Notion 블록 32에 기록**
- `database.history.recovery.mode`(schema_only_recovery)로 restart → **또 FAILED**
- 이유 (공식 규명): schema_only_recovery는 history를 **다시 스냅샷**하되 binlog는 기존 position부터 이어읽음 — **단 "복구 구간에 스키마가 안 바뀌었다"는 전제 하에서만** 정상. 우리 케이스는 rename으로 **이미 스키마가 바뀐 뒤**라 전제가 깨져서 재구성한 스키마도 실제와 안 맞음 → 실패
- Debezium 메일링에도 동일 사례 ("schema_only_recovery does not work after purging the db history topic")

**진짜 해결 = Reset Schema (Notion 블록 34-38)**
- connector stop → schema history 토픽(`dbz-schema-changes.tb`) **삭제 → 재생성** → start
- **Debezium 공식 권장안과 일치**: "recovery가 불가하면 offset + schema history 토픽을 지우고 (schema-only) 스냅샷을 다시" → 우리가 한 게 정석

**별개 예방 (해결책 아님 — 재발 방지 성격)**
- `table.exclude.list: ".*\\._.*"` (common.json:15) — `_` helper 테이블은 어차피 캡처 대상이 아니니 **필요 없는 걸 스킵**하는 것. 이 죽음을 고친 해결책이 아니라, 이런 노이즈가 애초에 파이프라인에 안 들어오게 하는 예방
- ⚠️ 주의: `_` 제외는 진짜 `device` 테이블의 스키마 변경 감지와 **무관**. helper 노이즈 제거일 뿐

**블로그 앵글**
- "Debezium은 컬럼 구조를 schema history에 따로 기억한다. online schema change의 `_` helper 테이블 rename이 이 기억을 실제와 어긋나게 하면 `internal schema ... out of sync`로 커넥터가 죽는다. **에러가 시키는 schema_only_recovery도 안 통한다** — recovery는 '복구 구간에 스키마 안 바뀜'을 전제하는데 rename이 그 전제를 깨기 때문. 결국 schema history 토픽을 통째로 지우고 다시 스냅샷하는 게 공식 정답이었다. (`_` 제외는 helper 노이즈를 애초에 안 보게 하는 별개 예방)"

**Refs**: Notion "CDC Pipeline handling bulk job" (실제 스택트레이스/블록32 recovery FAILED/블록34-38 Reset Schema) · Debezium 공식 schema_only_recovery 동작·한계 + gh-ost/pt-osc helper 테이블 언급 · Debezium 메일링 "schema_only_recovery does not work after purging" · repos/debezium `connectors/prod/common.json:15`

---

### 사건 4 — master switchover: Pause로는 DNS가 안 바뀐다 (Notion 389c)

> 순수 Debezium 커넥터 운영 사건. DBA/DevOps 협업 인프라 이벤트. Pause/Restart 동작 차이 공식 검증됨.

**상황**
- 6/27 DB 마스터 스위치오버 → datapipe DNS를 Slave2(main311)→Slave1(main221) 변경 (Slave2 백업 시 복제지연 회피 목적)
- 대상: 4개 커넥터 (tb/tc/mifos/leadgen)

**Pause vs Restart — 동작 방식 차이 (핵심, 공식 검증됨)**

| | Pause | Restart |
|---|---|---|
| 태스크 프로세스 | **유지** (instantiated 그대로 살아있음) | **종료 후 재생성** (recreate task) |
| 멈추는 것 | polling만 (새 레코드 안 당김) | 태스크 전체 |
| DB 연결(binlog client TCP) | **유지됨** → 옛 주소 계속 물고 있음 | **새로 맺음** → DNS 재조회 → 새 주소 |
| DNS 변경 반영 | ❌ 안 됨 | ✅ 됨 |

- Pause 공식 정의: "source connector paused → Connect stops polling. **The connector and tasks stay instantiated** (task 프로세스는 계속 running)"
- Restart 공식 정의: "restarts tasks, **effectively recreating the task infrastructure**" → 재생성 시 Debezium이 DB 연결을 새로 맺으며 DNS 재조회

**초기 계획의 함정**
- 처음 계획: "Pause → DNS 변경 → Resume"
- 문제: Pause는 task를 살려둔 채 polling만 멈춤 → **binlog client의 TCP 연결이 그대로 살아있음** → DNS 바꿔도 커넥터는 여전히 옛 서버(Slave2)에 붙어 있음

**수정된 해결 (06-27 plan 수정)**
- **Pause → DNS 변경 확인(nslookup) → Restart(`restart?includeTasks=true`)**
- Restart가 task를 종료·재생성하며 **새 TCP 커넥션** → 변경된 DNS(Slave1)로 연결
- Pause 먼저 하는 이유: DNS 변경 순간의 불필요한 에러 로그 방지

**유실 없음의 근거 (restart해도 처음부터 안 읽음)**
- 공식 문서에 "restart는 처음부터 처리"라는 문구가 있으나 → **offset이 없는 신규 커넥터 한정.** offset(position) 저장돼 있으면 그 지점부터 이어감
- 우리는 **GTID 모드** → restart 시 마지막 GTID부터 이어받음 → 데이터 유실 없음 (Notion 기록과 일치)

**블로그 앵글**
- "Pause와 Restart는 비슷해 보여도 태스크를 대하는 방식이 다르다. Pause는 태스크를 살려둔 채 polling만 멈추므로 DB로의 TCP 연결이 그대로 유지된다 → DNS를 새 마스터로 바꿔도 옛 서버를 계속 물고 있다. Restart는 태스크를 없앴다 다시 만들며 연결을 새로 맺어 DNS를 재조회한다. 그래서 마스터 스위치오버엔 Pause가 아니라 Restart. offset/GTID가 있으니 처음부터 다시 읽지도 않는다."

**Refs**: Notion "CDC Debezium connector maintenance for DB master switchover (6/27)" (DNS 대상 4개/절차/plan 수정) · Kafka Connect 공식 pause vs restart 동작(pause=task 유지·polling만 / restart=task recreate) · GTID/offset 이어받기

---

### 사건 5 — at-least-once의 중복, 그리고 exactly-once로 가는 길 (Notion 2bcc + DFM-1825)

> 프레이밍: "at-least-once가 합리적"이 아님. **이 중복 문제를 겪었고 → 지금은 PK merge로 흡수 중 → 이제 Debezium이
> exactly-once를 지원하니(3.2.1+) → 버전 올리며 켜볼 예정 → 단 성능(오버헤드) 비교가 필요하다"** 는 미래지향/진행형.

**1. 문제 (겪은 것)**
- Debezium = **at-least-once**. worker restart/rebalance, offset commit 지연 시 **downstream 중복** 이벤트
- 왜: offset commit(어디까지 읽었는지 저장)이 실제 읽기보다 뒤처짐 → 재시작 시 저장된 offset부터 다시 → 이미 보낸 것 재전송
- 사건 3·4와 직결: pause/resume/restart를 운영에서 자주 하게 되는데 그때마다 중복이 깔림

**2. 현재 대응 (동작하지만 근본 해결은 아님)**
- 소비자에서 **PK 기반 upsert/merge**로 중복 흡수 (DFM-1825: "We rely on PK-based upsert/merge downstream to absorb this")
- 동작은 하지만 소스 레벨 해결은 아님 → 과도기

**3. 전환점 — 이제는 exactly-once를 지원한다**
- Kafka Connect `exactly.once.source.support`는 **커넥터가 `SourceConnector::exactlyOnceSupport()`에서 `SUPPORTED` 반환**해야 켤 수 있음
- Debezium은 **3.2.1부터** core 커넥터(MySQL 포함)에 exactly-once 추가. **우리 현재 3.0은 `UNSUPPORTED`** → 지금 켜면 preflight 실패
- → 예전엔 못 썼던 게(그래서 PK merge로 버팀) 이제 버전만 올리면 가능해짐

**4. 계획 & 확인할 것 (DFM-1825)**
- 계획: Debezium Connect 3.0 → 3.2.1+ 업그레이드하면서 exactly-once 켜보기 (3-worker distributed, 롤링 재시작)
- ⚠️ 단 **공짜가 아님 — 성능 비교 필수**:
  - 모든 source-task producer가 **transactional로 강제** (transaction marker + commit 조정), consumer는 `read_committed` 강제 → 처리량/end-to-end lag 벤치마크 필요 (stage에서 before/after)
  - 2단계 롤아웃 (`preparing` 전체 → `enabled`), 워커 하나라도 구버전이면 전체 보장 깨짐
  - `offset.flush.timeout.ms` 무시(EoS는 무제한) → stuck commit이 task failure로
- 결론 프레임: **"exactly-once로 갈 방향. 버전 올리며 켜보되, transactional 오버헤드가 CDC 처리량에 주는 영향을 성능 비교로 확인하고 결정한다."**

**블로그 앵글**
- "재시작마다 중복이 생기는 at-least-once를 PK merge로 버텨왔다. 이제 Debezium 3.2.1+가 exactly-once source를 지원하니, 버전을 올리며 켜볼 참이다. 다만 exactly-once는 공짜가 아니다 — 모든 producer가 transactional이 되고 소비자는 read_committed로 묶인다. 그래서 켜기 전에 성능부터 재본다."

**Refs**: Notion "Deduplicate Debezium duplicate events in Flink" (2bcc, at-least-once 배경) · DFM-1825 (3.2.1 업그레이드 + exactly-once 평가: SUPPORTED 조건/transactional 오버헤드/2단계 롤아웃/offset timeout) · Debezium 3.2 릴리스 노트 · KIP-618

---

### 사건 6 (가벼운 팁) — Debezium DDL 파서가 MySQL 새 문법을 못 읽는다 (unparseable DDL)

> 사건 3(schema history 어긋남)과 "커넥터가 DDL 처리에서 죽는다"는 큰 범주만 같고 **원인이 다른** 별개 사건.
> 사건 3 = 스키마 기억 어긋남(임시테이블). 이건 = 파서 자체가 문법을 못 읽음. 가벼운 팁성.

**원인**
- Debezium은 binlog에 흘러오는 DDL(`ALTER`, `CREATE`, `GRANT` 등)을 **자체 SQL 파서로 해석**해서 스키마 기억을 갱신함
- 근데 이 파서는 MySQL 문법을 재구현한 것이라, MySQL이 **새 문법을 추가하면 못 따라감**
- 실제 사례: MySQL 8.0의 새 GRANT 권한 키워드 `ALLOW_NONEXISTENT_DEFINER`, `APPLICATION_PASSWORD_ADMIN` → 파서가 모름 → 해당 DDL 파싱 실패 → 커넥터 죽음(안전 정지 기본 동작)

**해결**
- `schema.history.internal.skip.unparseable.ddl: true` (tb/tc/mifos/leadgen-connector.json) — 못 읽는 DDL을 죽지 말고 건너뛰기
- GRANT 등은 권한 문이라 테이블 구조와 무관 → 스킵해도 데이터 캡처엔 영향 없음

**블로그 앵글**
- "Debezium의 MySQL DDL 파서는 MySQL 문법의 재구현이라 항상 뒤처진다. MySQL 8.0 새 GRANT 권한 하나에 커넥터가 죽는다. `skip.unparseable.ddl:true`로 '모르는 DDL은 건너뛰게' 해야 한다 — 특히 데이터와 무관한 권한/DCL 문장 때문에 파이프라인이 멈추는 건 아깝다."

**Refs**: repos/debezium `connectors/prod/README.md:100-102` · `tb-connector.json:6`

---

### 사건 7 — 노드가 죽어도 task가 바로 안 넘어간다: rebalance 5분 대기 (DFM-1825 note + 최근 관찰)

> "버그인 줄 알았는데 사양이었다" 계열. distributed인데 failover가 안 되는 줄 알았더니 의도된 대기.

**겪은 것**
- 클러스터에서 특정 worker 노드가 죽으면 다른 노드로 task가 넘어가야 하는데 **바로 안 감**
- → "distributed 모드 구성이 안 됐나?" 의심 → 알고 보니 **일정 시간 대기가 정상** (기본 5분)

**진짜 이유 (공식 — KIP-415, Incremental Cooperative Rebalancing)**
- Kafka Connect 2.3+ 기본 rebalancing 프로토콜: worker가 그룹을 떠나면 **즉시 재분배 안 하고 `scheduled.rebalance.max.delay.ms`만큼 대기** → 기본값 **300000ms(5분)**. 우리 환경 unset → 기본값
- 대기 동안 그 worker의 task는 **unassigned로 방치**
- **왜**: 워커의 일시적 재시작/업그레이드를 관대하게 넘기려는 의도. worker가 5분 안에 돌아오면 → **원래 자기 task를 그대로 돌려받음**(불필요한 재분배 회피). 5분 넘겨야 → 남은 워커에 재할당

**즉 버그 아니라 의도된 설계**
- rolling restart(사건 4·5에서 하는 것)엔 유리 — 곧 돌아올 워커의 task를 섣불리 안 옮김
- 트레이드오프: **진짜 노드가 죽으면 그 몫의 CDC가 최대 5분 멈춤** → 빠른 failover 원하면 이 값 튜닝 (DFM-1825 note)

**⚠️ 튜닝 함정 (공식 — KAFKA-15693)**
- `scheduled.rebalance.max.delay.ms=0`으로 **아예 끄면 위험** → connector/task가 **무기한 unassigned**로 남을 수 있음. 0으로 두지 말 것

**블로그 앵글**
- "distributed인데 노드가 죽어도 task가 안 넘어간다? 고장 아니다. Kafka Connect는 worker가 떠나면 5분(`scheduled.rebalance.max.delay.ms` 기본값) 기다린 뒤 재분배한다 — 일시적 재시작을 관대하게 넘기려는 설계. rolling restart엔 유리하지만, 진짜 노드가 죽으면 그 몫 CDC가 최대 5분 멈춘다. 빠른 failover가 필요하면 조정하되 0으로 끄면 task가 영영 안 붙을 수 있다."

**Refs**: DFM-1825 "Related note" (scheduled.rebalance.max.delay.ms unset → 300000ms 기본, KIP-415 확인) · Kafka 공식 Administration 문서(5분 기본/워커 복귀 시 원 task 회수) · KAFKA-15693 (delay 비활성화 시 무기한 unassigned) · KIP-415

---

### 사건 8 — 스냅샷 locking mode: 같은 로우가 두 번 들어온다 (r/i duplicate) (DFM-1815, 로그 08-25)

> "괜찮아 보이는데 raw엔 중복이 남는다" 반전. 원인은 Debezium 커넥터 설정(snapshot.locking.mode).

**어쩌다 발견 (실제 정황)**
- CREDITCARD 신규 스키마(7테이블, 61MB)를 CDC에 온보딩(DFM-1815) — binlog 만료된 과거 데이터를 채우려 **임시 initial-snapshot 커넥터**를 띄움
- 그 과정에서 snapshot locking mode에 따라 raw `_changes`에 중복이 남을 수 있음을 마주침 → 커넥터를 `creditcard-snapshot-2`로 **`locking.mode=minimal`로 재배포** (로그 08-25 03:xx)

**문제 — `none` locking이면 r/i duplicate**
- `snapshot.locking.mode`가 스냅샷 시작 시 offset 잡는 방식을 정함:

| mode | 락 | r/i 중복 | replica 부하 |
|---|---|---|---|
| `minimal` | 시작 시에만 `FLUSH TABLES WITH READ LOCK`으로 offset 고정 후 즉시 해제 | **없음** | 미미 |
| `none` | 락 없음, offset 느슨하게 고정 | **있음** | 0 |
| `extended` | 스냅샷 내내 락 유지 | 없음 | 높음 — 회피 |

- `none`의 함정: 락이 없으면 스냅샷 경계가 흐려짐 → 스냅샷 도중 바뀐 로우가 `read`(스냅샷)로도 `insert`(라이브 CDC)로도 둘 다 나옴 = **같은 로우 중복(r/i duplicate)**

**왜 중요한가 — 영향이 raw뿐 아니라 최종 상태까지 갈 수 있다 ⭐ (핵심)**
- 현재 우리 구현: db-streamer의 **hash MERGE가 r/i 중복을 흡수** (우리 케이스 최종 상태는 정확). 하지만 **"MERGE가 있으니 최종은 안전"으로 일반화하면 안 됨**
- 진짜 문제는 중복 자체보다 **"어떤 이벤트가 최종값으로 선택되느냐"(순서/reconciliation)**:
  - snapshot의 옛 상태(`read`)와 streaming의 새 상태(`insert`/`update`)가 함께 들어왔을 때, downstream이 순서를 제대로 판단 못 하면 → **오래된 상태가 최종값이 될 수도** (merge 방식에 따라 최종 상태까지 어긋남)
- **raw 레이어엔 중복이 그대로 남음**: `dataflow`의 `daily_compact_changes`가 full-row `dropDuplicates()`를 쓰는데, `r` 로우와 `i` 로우는 `binlog_type`/`binlog_timestamp`가 달라서 → 같은 로우인데도 dedup 안 됨 → raw `_changes`를 **직접 읽는 소비자**(예: data-delivery-man)는 중복을 봄
- 즉: raw까지 새어나가고(직접 소비자), merge 방식에 따라 최종 상태까지 갈 수 있는 문제

**해결 (실제 — 로그 08-25)**
- 임시 스냅샷 커넥터를 `locking.mode=minimal`로 재배포 → r/i 중복 없이 raw까지 깨끗
- `minimal`은 DB 유저에 `RELOAD` + `LOCK TABLES` 권한 필요 (Leadgen `tc_esuser` 보유 확인). read replica에서도 `super_read_only=OFF`면 순간 락이라 안전

**블로그 앵글**
- "스냅샷 뜰 때 `locking.mode=none`이면 스냅샷 도중 바뀐 로우가 `read`와 `insert`로 두 번 들어온다(r/i duplicate). 무서운 건 Iceberg MERGE에선 흡수돼 안 보인다는 것 — 최종 테이블은 멀쩡하다. 하지만 raw `_changes`엔 중복이 남고, `r`과 `i`는 메타가 달라 compaction의 dedup도 못 잡는다. raw를 직접 읽는 소비자가 있으면 거기서 터진다. `minimal`은 스냅샷 시작에만 잠깐 락을 걸어 경계를 깔끔히 만든다."

**Refs**: repos/debezium `docs/snapshot-baseline-onboarding.md` (Locking 섹션) · 로그 2026-08-25 (CREDITCARD 온보딩: creditcard-snapshot-2 locking=minimal 재배포, r/i 중복 근거) · DFM-1815

---

## 공통 서사 (사건 전체 관통)

두 축으로 나뉨:

**A. 데이터가 조용히 틀린다 (사건 1·2)**
> "CDC는 소스를 그대로 복제하지 않는다 — 시간의 단위, 그리고 스키마 진화의 빈틈에서 조용히 어긋난다."
- 에러 없이 "그럴듯하게 틀린 값"이 흐름 → 소비자 신고 전엔 모름
- 물리적으론 정상(int64 맞음 / 0도 유효한 값) → 의미 레벨에서만 틀림

**B. 커넥터가 예상과 다르게 움직인다 (사건 3·4·6·7·8)**
> "Debezium/Kafka Connect의 동작·설정을 모르면, 정상 사양을 장애로 오인하거나 잘못된 방법으로 대응한다."
- schema history 어긋남, Pause≠연결끊김, 새 DDL 파싱 실패, rebalance 5분 대기, 스냅샷 locking으로 r/i 중복 — 다 "내부 동작을 알아야 제대로 대응"
- 사건 8은 추가로 "괜찮아 보이는데 raw엔 남는다"는 계층별 관점도
- 사건 5는 이 한계들을 넘어서려는 **다음 방향**(exactly-once)

## 다음 (남은 후보 — 미확인)

> 대부분 이미 사건 1~7로 승격됨. 아래만 미확인 상태로 남음.

- binlog 만료 / snapshot 재구성 (no_data, r/i dup) — repos/debezium `docs/snapshot-baseline-onboarding.md`
  (참고: `snapshot-baseline-onboarding.md`는 이미 읽음 — 만료 binlog 온보딩 절차. 사건으로 쓸지는 미정)
- 스키마 변경 자동 감지 (32dc, DFM-1483) — 소비자(db-streamer) 쪽이라 이번 글 범위 밖으로 제외

---

## 검증 메모 (2026-08-31) — "다들 겪는 문제인가" 확인 결과

> 8개 사건 전부 공식 문서/GitHub 이슈/전용 도구가 있는 **공통 함정**으로 확인됨 (나만의 특수 케이스 아님).
> → 블로그에서 "누구나 밟는 함정"이라고 쓸 근거는 충분. 단 아래 "인접 사례"는 **내가 직접 겪은 게 아니므로** 본문 사건으로는 안 씀. 참고용 메모만.

**공통 함정 근거 (본문에서 "다들 겪는다" 뒷받침용):**
- 사건1 datetime6 → 전용 커스텀 컨버터 2개(holmofy, itcig), Debezium 이슈 다수
- 사건2 INSTANT default → Airbyte 동일 이슈(#28968), CDC 도구 공통
- 사건3 schema history × gh-ost/pt-osc → **Debezium 공식 FAQ에 명시** (여러 버전 문서 반복)
- 사건4 Pause≠연결끊김 → 공식 문서 명시 + Medium "half-dead task"
- 사건8 locking r/i dup → **Debezium 공식 문서 명시** + GitHub DBZ-3854

**인접 사례 (안 겪음 — 본문 미사용, 메모만):**
- nanosecond precision → `9999-12-31` 같은 sentinel이 **1816년으로 조용히 오버플로우** (dbz#2075). 우리는 micros라 해당 없음. datetime6 심화 곁가지로 언급 여지는 있으나 직접 겪은 건 아님
- `snapshot.locking.mode=none` → 중복(r/i)뿐 아니라 **data loss**까지 가능 (DBZ-3854). 우리는 minimal로 갔으므로 유실은 안 겪음. 사건8에서 "none은 유실 위험까지"로 한 줄 보강 여지는 있으나 직접 겪은 건 아님

---

## [히스토리] 1편 목차 (2026-08-18 작성) — datetime6 + tinyint 두 개로 좁힘 (※ tinyint 프레임 오류, 위 재검증본 참고)

> 6개 주제 다 넣으면 길어서 완결 안 됨 → 타입 지뢰 2개로 1편. 나머지는 후속편.

**제목 후보**
- "Debezium을 프로덕션에 넣고 만난 타입 지뢰들 — datetime(6)과 tinyint"
- "CDC가 조용히 데이터를 바꾼다 — Debezium 타입 매핑 실전기"

**1. 들어가며 — CDC는 "그냥 복제"가 아니다**
- 소스 타입이 그대로 온다고 믿기 쉬움 → Debezium은 중간에서 자기 방식대로 변환 → 조용한 손상

**2. 지뢰 1 — datetime(6): 사라진 마이크로초 (DFM-1318)**
- 증상: 정밀도 손실 / 라운드트립 안 맞음
- 원인: 시간 타입 직렬화 방식 (epoch micros/millis, time.precision.mode)
- 디버깅 → 해결(설정/컨버터) → 교훈: 시간 타입 먼저 의심

**3. 지뢰 2 — tinyint(1): boolean인가 숫자인가, 그리고 NULL (DFM-1482)**
- 증상: tinyint(1) 예상과 다르게 + NULL에서 파티션 깨짐
- 원인: MySQL tinyint(1) ↔ boolean 모호성
- 해결: TinyIntOneToBooleanConverter + fill_if_null
- 교훈: 스키마의 "의도"와 "타입"이 다를 때 CDC가 드러냄

**4. 공통 패턴 — 타입 지뢰는 왜 조용한가**
- 에러 안 남, 데이터가 "그럴듯하게 틀림" → 소비자 신고 전엔 모름
- → 선제 검증(스키마/값 assertion) 필요 (H1 리뷰 성장포인트 "반응형→선제형 탐지"와 연결)

**5. 마치며 — Debezium 도입 전 타입 체크리스트**
- 점검할 것 3~4개 + 후속편 예고

**후속편 씨앗**: duplicate/멱등성(DFM-1326), 스키마 변경 자동 감지(DFM-1483), 장애 복구, bulk job(DFM-1155)
