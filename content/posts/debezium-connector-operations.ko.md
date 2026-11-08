---
title: "Debezium 커넥터는 예상대로 움직이지 않는다: 운영 중 겪은 네 가지 사례"
date: 2026-11-09T00:00:00+09:00
draft: false
tags: ["cdc", "debezium", "kafka-connect", "data-engineering"]
description: "Debezium과 Kafka Connect 운영 중 겪은 스키마 불일치, SQL 파싱 오류, DB 연결 유지, task 재할당 지연 사례와 대응 과정을 정리했습니다."
cover:
  image: "/images/debezium-connector-operations-cover.png"
  alt: "Debezium connector operational semantics"
  relative: false
---

[지난 글](https://backpressured.dev/posts/debezium-cdc-silent-data-errors/)에서는 파이프라인은 정상적으로 동작하지만 데이터가 기대와 다르게 들어오는 세 가지 사례를 정리했습니다. 시간 값의 단위가 달라지고, NULL이 기본값으로 바뀌고, 같은 row가 스냅샷과 스트리밍 이벤트에 함께 나타났습니다. 파이프라인이 잘 도는 것만으로는 데이터까지 맞다고 볼 수 없었습니다.

이번 글에서는 **Debezium 커넥터와 Kafka Connect가 운영 중 예상과 다르게 동작했던 사례들**을 정리합니다.

스키마 변경 뒤 커넥터가 멈추기도 했고, 테이블 구조와 무관한 권한 문장을 해석하지 못해 수집이 중단되기도 했습니다. DB 호스트를 변경했는데도 커넥터가 이전 DB에 계속 연결되어 있거나, worker 노드가 죽었는데 task가 다른 노드로 바로 넘어가지 않는 경우도 있었습니다. 원인도 대응 방법도 달랐지만, 공통적으로 확인해야 할 것이 있었습니다. **Debezium과 Kafka Connect가 각 상황을 어떻게 처리하고, 설정이나 명령이 어디까지 영향을 미치는지**였습니다.

네 가지 운영 사례를 살펴본 뒤, 마지막으로 재시작 과정에서 발생하는 중복 이벤트와 exactly-once 적용을 검토하고 있는 이유를 덧붙이겠습니다.

---

## 1. 커넥터가 갑자기 죽었다: schema history의 어긋남

운영 DB에서 컬럼을 추가하던 중, Debezium 커넥터가 다음 에러를 남기며 수집을 멈췄습니다.

```
io.debezium.DebeziumException: Error processing row in …,
internal schema size 15, but row size 16, restart connector with schema recovery mode.
```

Debezium이 기억하는 테이블은 15개 컬럼인데, binlog에서 읽은 row에는 16개 컬럼이 있다는 뜻입니다. 어느 값을 어느 컬럼에 대응시켜야 할지 판단할 수 없어 task가 FAILED 상태로 멈췄습니다.

**Debezium은 binlog의 row 이벤트를 해석하기 위해 각 테이블의 스키마를 관리합니다.** 처음에는 DB에서 현재 테이블 구조를 읽고, 이후 binlog에서 DDL을 만날 때마다 메모리에 있는 스키마를 갱신합니다. 이 스키마를 바탕으로 row의 값을 컬럼에 대응시키고, Kafka로 보낼 이벤트를 구성합니다.

초기 스키마와 이후 변경 이력을 저장하는 곳이 **schema history 토픽**입니다. 이벤트마다 이 토픽을 조회하는 것은 아니지만, 재시작할 때는 저장된 이력을 읽어 수집을 이어갈 위치에 맞는 스키마를 복원합니다.

당시 DBA 팀은 온라인 스키마 변경 방식으로 작업을 진행했습니다. 새 컬럼과 초기값을 반영한 교체용 테이블에 데이터를 옮긴 뒤, `RENAME TABLE`로 원본을 교체하는 방식이었습니다.

이후 테이블 교체 과정에서 스키마 불일치가 발생했습니다. 새 스키마를 가진 테이블이 원본 이름을 넘겨받았지만, Debezium은 해당 이름에 연결된 기존 15개 컬럼의 스키마를 유지하고 있었습니다. 그 결과 교체된 테이블에서 들어온 16개 컬럼의 row를 해석하지 못한 것입니다.

복구에는 다음 설정을 적용했습니다.

```json
{
  "snapshot.mode": "recovery"
}
```

`recovery`는 현재 DB의 테이블 구조를 다시 읽습니다. [공식 문서](https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-property-snapshot-mode)에서는 커넥터가 멈춘 뒤 스키마가 변경됐다면 이 모드를 사용하지 말라고 안내합니다.

이번에는 테이블이 새 스키마로 교체된 뒤, Debezium이 기존 스키마로 데이터를 해석하다가 멈춘 경우였습니다. `recovery`로 교체된 테이블의 구조를 다시 읽으면서 불일치가 해소됐고, 수집을 재개할 수 있었습니다.

---

## 2. 테이블 구조와 무관한 권한 문장 하나에 커넥터가 멈춘다

앞에서는 Debezium이 기억하는 스키마와 실제 row의 구조가 달라 수집이 멈췄습니다. 이번에는 DB 사용자에게 권한을 부여하는 `GRANT` 문을 처리하다가 수집이 멈춘 사례입니다.

Debezium은 binlog의 행 변경 이벤트를 읽어 데이터를 수집하고, SQL 문장을 파싱해 스키마 변경을 추적합니다. 이때 파싱하는 SQL에는 `CREATE`·`ALTER`뿐 아니라 `GRANT` 같은 권한 부여 문장도 포함됩니다. 따라서 테이블 구조와 무관한 SQL이라도, Debezium의 파서가 해당 문법을 처리하지 못하면 수집이 중단될 수 있습니다.

이 문제는 MySQL 버전을 올리는 과정에서 발생했습니다. MySQL 8.4.6 환경에서 `ALLOW_NONEXISTENT_DEFINER` 권한이 포함된 `GRANT` 문이 binlog에 기록됐지만, 당시 사용하던 Debezium의 파서는 이를 처리하지 못했습니다. **MySQL에서는 유효한 SQL이었지만, Debezium 파서와의 호환성 문제로 CDC 수집이 중단된 것입니다.**

당시에는 파싱할 수 없는 문장을 건너뛰도록 다음 설정을 적용했습니다.

```json
{
  "schema.history.internal.skip.unparseable.ddl": true
}
```

설정을 적용한 뒤에는 문제가 된 문장을 건너뛰고 수집이 정상적으로 재개됐습니다.

여기서 주의할 점은, **이 옵션이 테이블 구조와 무관한 문장만 골라서 건너뛰는 것은 아니라는 점**입니다. 파싱에 실패한 문장이 컬럼을 추가하거나 변경하는 DDL이라면, 이를 건너뛰면서 Debezium이 필요한 스키마 변경을 놓칠 수 있습니다. 기본값이 `false`인 이유도 여기에 있습니다. [공식 문서](https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-property-schema-history-internal-skip-unparseable-ddl)에서도 이 설정으로 인한 데이터 유실이나 잘못된 해석 가능성을 경고합니다.

이 옵션은 파싱에 실패한 문장을 건너뛸 뿐, 건너뛰어도 안전한지 판단해주지는 않습니다. 따라서 운영 환경에 적용할 때는 이러한 주의사항을 충분히 검토해야 합니다.

글을 정리하면서 확인한 [`schema.history.internal.ddl.filter`](https://github.com/debezium/debezium/blob/v3.0.6.Final/debezium-connector-binlog/src/main/java/io/debezium/connector/binlog/BinlogStreamingChangeEventSource.java#L724)는 특정 SQL을 파싱 전에 제외하는 내부 설정으로, 이러한 파싱 오류에 대응하기 위한 대안으로 검토할 수 있습니다.

---

## 3. DNS를 바꿨는데 이전 DB에 계속 연결된다

DB 마스터 스위치오버에 맞춰, Debezium이 접속하는 호스트명의 DNS 레코드를 새 DB 노드의 주소로 변경해야 했습니다. 처음에는 커넥터를 Pause한 뒤 DNS를 변경하고 Resume하면 새 노드에 연결될 것으로 예상했습니다.

하지만 Pause 상태에서도 기존 DB와의 TCP 연결은 유지됐습니다. DNS를 변경하고 Resume하는 것만으로는 연결 대상이 바뀌지 않았습니다.

**Pause는 task를 종료하지 않고 메시지 처리를 멈추며, task가 사용 중인 리소스도 그대로 유지합니다.** 따라서 처리를 재개하는 것과 DB 연결을 새로 맺는 것은 구분해야 했습니다.

연결을 새로 맺기 위해 DNS 변경을 확인한 뒤, `includeTasks=true`를 지정해 task까지 재시작했습니다.

```http
POST /connectors/{name}/restart?includeTasks=true
```

여기서 중요한 것은 **재시작 대상에 task를 포함했다는 점**입니다. [Kafka Connect 공식 문서](https://kafka.apache.org/35/kafka-connect/user-guide/)에 따르면 `includeTasks`의 기본값은 `false`이므로, 옵션 없이 restart API를 호출하면 커넥터 인스턴스만 재시작하고 task는 재시작하지 않습니다.

이 작업에서는 task를 포함한 재시작 후 새 DB 노드에 연결되어 수집을 재개한 것을 확인했습니다.

---

## 4. 노드가 죽었는데 task가 안 넘어간다: rebalance의 5분

Kafka Connect를 distributed 모드로 운영하면서, worker 노드 하나가 내려가면 다른 worker가 task를 곧바로 넘겨받을 것으로 예상했습니다. 하지만 실제로는 해당 노드가 맡던 수집이 멈춘 채, task가 다른 worker로 재할당되기까지 시간이 걸렸습니다.

원인은 worker 이탈 후 재할당을 늦추는 설정에 있었습니다. Kafka Connect의 incremental cooperative rebalancing에서 [`scheduled.rebalance.max.delay.ms`](https://kafka.apache.org/36/kafka-connect/administration/)는 이때 재할당을 지연할 수 있는 최대 시간을 정하며, 기본값은 **5분(300000ms)**입니다. 당시에는 이 값을 별도로 설정하지 않아 기본값을 사용하고 있었습니다.

이는 worker가 재시작이나 업그레이드로 잠시 이탈할 때 불필요한 task 이동을 줄이기 위한 동작입니다. 다만 worker가 복귀하지 않는 장애 상황에서는 이 대기 시간만큼 수집 재개가 늦어질 수 있습니다. 여기에 장애 감지와 task 재시작에 걸리는 시간도 있으므로, 설정값을 전체 복구 시간의 상한으로 볼 수는 없습니다.

운영 환경에서는 rolling restart의 편의성과 필요한 복구 시간을 함께 고려해 대기 시간을 정해야 합니다.

---

## 그리고 아직 풀어가는 것: at-least-once와 exactly-once

커넥터를 재시작한 뒤에는 연결과 수집 상태뿐 아니라 중복 이벤트도 확인해야 했습니다. 재시작 시 저장된 offset부터 수집을 이어가는 과정에서, 이미 전송한 이벤트가 다시 전달될 수 있기 때문입니다.

지금까지는 downstream에서 PK 기반 merge로 중복을 처리해왔습니다. 이후 [Debezium 3.3에서 MySQL을 포함한 core 커넥터에 Kafka Connect 기반 exactly-once 지원이 추가된 것](https://debezium.io/blog/2025/10/01/debezium-3-3-final-released/)을 확인해, 버전 업그레이드와 함께 적용을 검토하려 합니다. 적용 범위와 조건을 확인하고, 처리량과 지연에 미치는 영향을 측정한 뒤 도입 여부를 결정할 생각입니다.

---

## 정리하며

이번 사례들은 서로 다른 문제였지만, 공통적으로 Debezium과 Kafka Connect의 동작 방식과 적용 범위를 이해해야 상황을 해석하고 대응 방법을 판단할 수 있었습니다.

Debezium은 테이블 스키마를 기억하고, SQL을 파싱해 변경 이력을 추적합니다. Kafka Connect는 task의 실행 상태와 리소스를 관리하고, worker가 이탈하면 정해진 절차에 따라 작업을 재할당합니다. 각각의 동작은 나름의 이유가 있지만, 이를 모르고 운영하면 예상하지 못한 장애나 지연으로 이어질 수 있습니다.

운영하면서 배운 것은 **설정이나 명령이 무엇을 하는지만큼, 무엇을 하지 않는지 이해하는 것도 중요하다**는 점이었습니다. 이번에 겪은 문제들도 그 차이를 확인하는 과정에서 동작을 이해하고 대응 방법을 정리할 수 있었습니다.
