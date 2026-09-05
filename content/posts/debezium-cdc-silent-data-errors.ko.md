---
title: "Debezium은 소스를 그대로 복제하지 않는다: 데이터가 조용히 어긋나는 세 가지 사례"
date: 2026-09-05
draft: false
tags: ["cdc", "debezium", "mysql", "data-engineering"]
description: "CDC 파이프라인이 잘 도는 것과 데이터가 기대한 의미와 형태로 만들어지는 것은 다른 문제였습니다. Debezium을 프로덕션에서 운영하며 만난, 에러 없이 데이터가 조용히 어긋나는 세 가지 사례를 정리했습니다."
cover:
  image: "/images/debezium-silent-errors-cover.png"
  alt: "Debezium CDC silent data errors"
  relative: false
---

회사 기술 블로그에서 [Debezium과 Flink로 CDC 파이프라인을 재설계](https://blog.afinit.com/cdc-pipeline-debezium-flink)하고, [이를 기반으로 데이터 복제 방식을 증분 처리 중심으로 전환한 이야기](https://blog.afinit.com/cdc-incremental-replication)를 다룬 적이 있습니다. MySQL의 변경분을 Debezium이 캡처하고, Kafka를 거쳐, Flink가 S3와 Iceberg 테이블로 복제하는 구조입니다.

전체 아키텍처와 선택 배경은 그 글들에서 다뤘으니 여기서는 생략하겠습니다. 이 글은 그 다음 이야기입니다.

Debezium을 운영하면서 겪은 일은 크게 두 종류였습니다. 하나는 데이터가 조용히 어긋나는 문제였고, 다른 하나는 커넥터가 예상과 다르게 움직이는 문제였습니다. 이번 글에서는 먼저 데이터가 어긋났던 경우들만 정리합니다.

처음에는 CDC를 이렇게 생각했습니다.

> MySQL의 row가 Debezium을 지나면, 같은 내용의 변경 이벤트가 나온다.

그런데 운영해보니 그렇지 않았습니다. **CDC 파이프라인은 여러 계층의 기본 설정과 해석 방식을 통과하면서, 값의 의미나 이벤트의 형태가 기대와 다르게 만들어질 수 있었습니다.** 어떤 문제는 consumer에서 보정해야 했고, 어떤 문제는 수집 방식 자체를 더 보수적으로 가져가야 했습니다. 이 글에서는 그 차이를 실감하게 해준 세 가지 사례를 정리합니다. 각각 타입을 해석하는 지점, 직렬화 계층이 기본값을 다루는 방식, 그리고 스냅샷의 경계에서 벌어진 일입니다.

---

## 1. 같은 int64인데 값이 1000배 커졌다: datetime(6)

처음 마주친 건 시간 값이었습니다. 소스 DB에서 `datetime` 컬럼이 데이터레이크로 오면서 값이 어긋나 있었습니다. 어떤 컬럼은 멀쩡한데, 어떤 컬럼은 시각이 말이 안 되게 미래로 튀어 있었습니다.

Kafka로 넘어온 메시지의 스키마를 열어보니 이렇게 생겨 있었습니다.

```json
{
  "field": "created_at",
  "type": "int64",
  "name": "io.debezium.time.MicroTimestamp"
}
```

여기서 두 가지를 봐야 합니다.

- `type`은 **물리 타입**입니다. 값을 어떤 그릇에 담는지를 말합니다. 여기서는 `int64`입니다.
- `name`은 **논리 타입**입니다. 이 `int64`가 실제로 무엇을 뜻하는지를 말합니다.

문제는 Debezium이 MySQL의 시간 타입을 정밀도에 따라 **다른 논리 타입, 다른 단위로** 내보낸다는 것이었습니다.

| MySQL 타입 | Debezium 논리 타입 (`name`) | 물리 타입 (`type`) | 단위 |
|---|---|---|---|
| `DATETIME`부터 `DATETIME(3)`까지 | `io.debezium.time.Timestamp` | `int64` | 밀리초 |
| `DATETIME(4)`부터 `DATETIME(6)`까지 | `io.debezium.time.MicroTimestamp` | `int64` | 마이크로초 |

기본 설정인 `time.precision.mode=adaptive_time_microseconds`에서는 `DATETIME(0)`부터 `DATETIME(3)`까지는 `Timestamp`(epoch 밀리초)로, `DATETIME(4)`부터 `DATETIME(6)`까지는 `MicroTimestamp`(epoch 마이크로초)로 직렬화됩니다. 둘 다 물리 타입은 똑같은 `int64`입니다.

실제로는 Debezium으로 들어오는 컬럼이 워낙 많다 보니, 시간 컬럼의 물리 타입이 `int64`라는 것만 보고 일괄적으로 "밀리초"로 매핑했습니다. 대부분의 `datetime` 컬럼은 그 가정이 맞았지만, `datetime(6)` 컬럼은 마이크로초 단위였습니다. 그 컬럼들만 값이 1000배 커졌고, 결과적으로 "말이 안 되게 미래로 튄 시각"이 되었습니다.

해결은 값이 아니라 **논리 타입(`name`)을 보고 단위를 맞추는** 것이었습니다. 저희는 소비자(Flink) 쪽에서 `MicroTimestamp`인 컬럼만 1000으로 나눠 밀리초로 통일하고, 최종적으로 모든 시간 컬럼을 하나의 표현으로 맞췄습니다.

```scala
// 논리 타입을 보고 단위를 맞춘다.
case Some("io.debezium.time.MicroTimestamp") => value.asLong() / 1000
case _                                       => value.asLong()
```

나중에 알고 보니 [`time.precision.mode`](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types)로 시간 타입 처리 방식을 바꿀 수 있었습니다. `connect`로 설정하면 밀리초 단위에 맞출 수 있지만, 마이크로초 정밀도는 잃을 수 있습니다. 결국 기본 설정을 유지한다면, 소비자에서 논리 타입을 보고 변환해야 합니다.

이때부터 스키마를 볼 때 `type`만으로는 부족하다고 생각하게 됐습니다. CDC 이벤트에서는 동일한 물리 타입이라도, 그 값이 어떤 의미와 단위를 갖는지는 논리 타입까지 봐야 했습니다.

---

## 2. DB에는 NULL인데 CDC에는 0으로 들어왔다

두 번째는 더 헷갈렸습니다. 어떤 컬럼이 소스 DB에서는 `NULL`인데, CDC를 거친 데이터에서는 `0`으로 들어와 있었습니다. 그 컬럼은 이렇게 정의돼 있었습니다.

```sql
`is_ivr_required` tinyint DEFAULT '0',
```

나중에 추가된 nullable 컬럼이었고, 기존에 있던 데이터의 해당 컬럼은 `NULL`로 남아 있었습니다. 확인 당시 컬럼 정의에는 `DEFAULT 0`이 붙어 있었지만, 소스 DB에서 직접 조회하면 분명히 `NULL`이 나왔습니다. 그런데 CDC로 넘어온 값만 `0`이었습니다.

처음에는 타입 컨버터를 의심했습니다. MySQL의 `tinyint(1)`은 boolean으로 해석될 수 있어서, Debezium에는 이걸 다루는 `TinyIntOneToBooleanConverter`라는 컨버터가 있습니다. "이 컨버터나 `database.tinyInt1isBit` 설정이 값을 바꾸는구나" 싶었습니다.

그래서 설정을 세 가지 조합으로 바꿔가며 테스트했습니다.

1. 컨버터 유지 + `tinyInt1isBit` 추가
2. 컨버터 제거 + `tinyInt1isBit` 추가
3. 컨버터 제거

결과는 셋 다 똑같았습니다. 범인은 컨버터가 아니었습니다. `tinyint`라는 타입도 사실 우연히 걸린 것뿐이었습니다.

여기서 먼저 확인한 건 Debezium이 애초에 이 컬럼 값을 받았는지였습니다. `binlog_row_image=MINIMAL`이면 변경에 필요한 컬럼만 binlog에 남기기 때문에 값이 빠졌을 가능성을 의심할 수 있습니다. 하지만 저희 설정은 `FULL`이었고, row 이벤트에는 전체 컬럼 값이 기록되어야 했습니다. 적어도 변경 row에서 해당 컬럼 자체가 빠져 생긴 문제는 아니었습니다. 이후 이벤트 스키마와 직렬화 과정을 확인하면서 원인을 `NULL`과 schema default 처리로 좁혔습니다.

진짜 원인은 **nullable 컬럼에 DEFAULT가 정의돼 있을 때, NULL 값이 직렬화되는 지점에서 그 DEFAULT로 대체될 수 있다**는 쪽에 있었습니다.

Debezium이 만드는 이벤트 스키마에서, `DEFAULT 0`인 nullable 컬럼은 "optional하고 기본값은 0"인 필드가 됩니다. 대략 이런 형태입니다.

```json
{
  "field": "is_ivr_required",
  "type": "int16",
  "optional": true,
  "default": 0
}
```

이 필드에 실제 값 `NULL`이 담겨도, Kafka Connect의 converter나 그 뒤에서 Connect 데이터를 읽는 쪽이 스키마의 default를 적용하면 `NULL`이 `0`으로 바뀔 수 있습니다. 소스 DB의 `NULL`과 CDC를 거친 뒤의 `0`은 의미가 전혀 다른 값인데, 둘 다 유효한 값이라 에러도 나지 않습니다.

처음에는 Debezium 자체가 값을 바꾼다고 생각했습니다. 하지만 확인해보니 비슷한 증상은 Debezium 코어에서도 과거에 한 번 수정된 적이 있고([DBZ-1064](https://issues.redhat.com/browse/DBZ-1064)), 저희 환경에서 더 직접적으로 봐야 했던 건 Kafka Connect의 `JsonConverter`였습니다. 저희는 schema를 포함한 JSON converter를 쓰고 있었고, Kafka 3.5.0부터는 nullable field의 `NULL`을 schema default로 바꿀지 제어하는 [`replace.null.with.default`](https://cwiki.apache.org/confluence/display/KAFKA/KIP-581%3A+Value+of+optional+null+field+which+has+default+value) 옵션이 있습니다. 기본값은 기존 동작을 유지하기 위해 `true`입니다.

즉 소스에서 의도한 `NULL`과, CDC를 거친 뒤의 값이 달랐던 것입니다. Debezium 앞단에서 컬럼 값이 빠진 문제가 아니라, 그 뒤 직렬화 계층을 지나면서 `NULL`이 schema default인 `0`으로 해석된 문제에 가까웠습니다.

돌이켜보면 `value.converter.replace.null.with.default=false`를 먼저 확인해볼 수 있는 문제였습니다. 당시에는 이 옵션을 먼저 보지 못했고, 처음에는 소스 컬럼에서 `DEFAULT`를 제거하는 쪽을 생각했습니다. 스키마에 채울 기본값이 없으면 같은 방식으로 `NULL`이 `0`으로 바뀔 여지가 줄어드니까요. 하지만 실제로는 비슷한 형태의 컬럼이 여러 테이블에 흩어져 있었고, 운영 중인 소스 스키마를 한꺼번에 바꾸는 것도 부담이 컸습니다. 결국 당시에는 기존 데이터와 CDC 경로의 불일치를 먼저 줄이는 것이 우선이었고, 소비자 쪽에서 두 경로가 같은 표현을 쓰도록 정규화했습니다. 다만 원본의 `NULL` 의미를 보존해야 하는 컬럼이라면, consumer 단계가 아니라 converter 단계에서 `NULL`을 보존하는 편이 맞습니다.

여기서 배운 건, 소스에서 조회되는 값과 CDC를 거친 값이 항상 같지는 않다는 것이었습니다. 특히 `NULL`과 `0`처럼 의미가 다른 두 값이, 파이프라인 어딘가에서 조용히 뒤바뀔 수 있었습니다.

---

## 3. 같은 row가 두 이벤트로 들어왔다: 스냅샷의 경계

앞의 두 사건은 값을 해석하거나 처리하는 과정에서 최종 값이 달라진 경우였습니다. 세 번째는 조금 달랐습니다. 값이 바뀐 문제는 아니었고, 이벤트가 만들어지는 방식이 기대와 달랐습니다. 현재 상태를 채우기 위해 만든 snapshot 이벤트와, 이후 binlog에서 이어서 들어온 streaming 이벤트에 같은 row가 함께 나타났습니다.

새 스키마를 처음부터 CDC로 수집하고 있었다면 가장 단순했을 겁니다. 하지만 운영하다 보면 이미 만들어져 있던 스키마를 나중에 CDC에 온보딩해야 하는 경우가 생깁니다. 이때 오래된 binlog는 이미 만료되어 있어서, 과거 변경 이벤트를 처음부터 다시 읽을 수 없습니다. 그래서 저희는 현재 상태를 한 번 채우는 initial snapshot과, 그 이후 변경분을 이어서 읽는 CDC 수집을 함께 사용했습니다. 이 과정에서 `snapshot.locking.mode`를 바꿔가며 테스트하다가, `none`을 사용했을 때 raw CDC에 같은 PK가 두 이벤트로 나타나는 현상을 확인했습니다.

`snapshot.locking.mode`는 initial snapshot의 초기 단계에서 데이터베이스 락을 어떻게 사용할지를 정합니다. 기본값인 `minimal`은 초기 schema와 metadata, binlog position을 확보하는 동안만 global read lock을 사용하고, 이후 row scan은 `REPEATABLE READ` transaction으로 일관성을 유지합니다. `none`은 이 초기 lock을 생략합니다.

Debezium 문서에서 `none`의 주된 경고는 스냅샷 중 schema change가 있으면 안전하지 않다는 쪽입니다. 하지만 저희가 겪은 문제는 schema change가 없는 상황에서도 나타났습니다. `none`으로 initial snapshot을 수행했을 때 같은 PK가 snapshot 이벤트(`read`)와 streaming 이벤트(`insert`) 양쪽에 나타났고, `minimal`로 바꾼 뒤에는 같은 현상을 관찰하지 못했습니다.

| mode | 스냅샷 락 | 우리 환경에서 관찰한 결과 |
|---|---|---|
| `minimal` | 초기 단계에만 사용 | 중복 없음 |
| `none` | 사용하지 않음 | 같은 PK가 `read`/`insert` 두 이벤트로 나타남 |

다만 이 결과만으로 `none`이 항상 같은 중복을 만든다고 보기는 어렵습니다. 저희처럼 뒤늦게 스키마를 온보딩하면서 initial snapshot과 이후 CDC 수집을 이어 붙이는 구조에서는, snapshot 경계를 어떻게 잡느냐가 raw CDC의 해석 방식에 영향을 줄 수 있었습니다.

여기서 중요한 건 중복 그 자체보다, downstream이 이 이벤트들을 어떤 의미로 해석하느냐였습니다. Debezium의 snapshot 이벤트(`read`)는 DB에서 실제로 INSERT가 발생했다는 뜻이 아니라, "스냅샷 시점에 이 row가 존재했다"는 뜻입니다. 반면 streaming 이벤트(`insert`/`update`)는 binlog에서 읽은 실제 변경 이벤트입니다.

두 종류의 이벤트가 같은 PK에 대해 함께 존재하면, raw CDC를 읽는 쪽에서는 그것을 그대로 보게 됩니다. 여기서 raw CDC는 아직 복제 테이블로 병합되기 전의 원천 이벤트 로그입니다. 복제 테이블을 만드는 경로에서는 이런 중복을 흡수할 수 있더라도, raw 데이터를 직접 읽는 소비자가 있다면 이야기가 달라집니다. 결국 raw CDC는 row당 하나의 현재 상태가 아니라, 같은 PK에 대한 여러 이벤트가 존재할 수 있는 change log입니다.

그래서 이후 신규 스키마 온보딩에서는 Debezium의 기본값이기도 한 `minimal`을 사용했습니다. 저희 환경에서는 `minimal`로 변경한 뒤 앞서 관찰한 `read`/`insert` 중복을 다시 확인하지 못했고, 초기 단계에만 락을 사용하기 때문에 전체 snapshot 동안 write를 막는 `extended`보다 운영 부담도 작았습니다.

이후에는 initial snapshot으로 만든 이벤트를 raw change log처럼 다룰 때 더 조심하게 됐습니다. 스냅샷은 단순히 "현재 row를 한 번 읽는 작업"이 아니라, 이후 streaming 이벤트와 이어 붙여 해석해야 하는 경계가 있는 작업이었습니다.

---

## 정리하며

세 사건은 겉으로는 전혀 다른 문제였습니다. 시간 값의 단위, 직렬화 과정의 기본값 처리, 스냅샷의 경계. 하지만 공통점은 같았습니다. 파이프라인은 멀쩡했지만, 데이터는 기대한 형태로 들어오지 않았습니다.

우리는 보통 CDC를 이렇게 상상합니다.

> DB에 있는 것 → binlog → Debezium → 그대로 복제

그런데 실제로는 그 사이에 여러 단계가 들어갑니다. 값의 단위를 어떻게 읽을지, 직렬화 계층이 `NULL`을 어떻게 다룰지, 스냅샷과 스트리밍의 경계에서 같은 변경이 어떻게 표현될지. 각 단계에서 데이터의 의미나 형태가 조금씩 달라질 수 있었습니다.

그래서 CDC를 운영하면서 중요했던 건, **파이프라인이 잘 도는지가 아니라 실제로 데이터가 기대한 의미와 형태로 만들어지고 있는지를 계속 확인하는 일**이었습니다. connector와 converter의 기본 설정, consumer의 해석 방식, 스냅샷과 streaming의 경계가 모두 데이터의 최종 형태에 영향을 줄 수 있었습니다.

이번 글이 "잘 흘러가도 데이터는 조용히 어긋날 수 있다"는 이야기였다면, 다음 글에서는 조금 다른 종류의 이야기를 정리해보려 합니다. schema history, pause와 restart의 차이, worker failover와 rebalance처럼, **Debezium과 Kafka Connect가 프로덕션에서 실제로 어떻게 움직이는지**에 대한 이야기입니다.
