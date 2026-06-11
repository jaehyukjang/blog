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
