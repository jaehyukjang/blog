# 링크드인 포스트: CDC Small File 블로그 공유

## 상태
- [ ] After 비용 모니터링 기간 확보 (며칠 더)
- [ ] 결과 수치 보강
- [ ] 링크드인 공유

## 초안

CDC 파이프라인에서 발생하는 Small File 문제와, 그 뒤에 숨어있는 S3 Request 비용에 대해 정리했습니다.

Athena 비용은 스캔량만 보면 된다고 생각하기 쉽지만, 실제로는 S3 GetObject 비용이 전체 S3 비용의 절반 가까이 차지하고 있었습니다. 원인은 Flink가 5분마다 생성하는 수백 KB짜리 Parquet 파일이 1년간 3,100만 개 넘게 쌓인 것이었습니다.

이 글에서는 비용 분석 방법(Cost Allocation Tag, Storage Class Analysis), Small File이 비용과 쿼리 성능에 미치는 영향, 그리고 Daily Compaction으로 해결한 과정을 공유합니다.

👉 한국어: https://jaehyukjang.github.io/blog/posts/cdc-small-file-hidden-cost/
👉 English: https://jaehyukjang.github.io/blog/en/posts/cdc-small-file-hidden-cost/

## 태그
#DataEngineering #CDC #AWS #S3 #CostOptimization #Debezium #Flink #Athena
