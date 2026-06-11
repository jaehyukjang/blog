# 블로그: JumpCloud OIDC + STS 마이그레이션

## 제목 후보
"7년 된 IAM 키를 폐기하기까지: JupyterHub 인증을 OIDC + STS로 전환한 이야기"

## 구조

1. **도입** — 2018년에 만든 static IAM key 4개가 아직 살아있었다. Google OAuth + 정적 키 조합의 문제점
2. **왜 바꿔야 했는가** — 장기 키 리스크, 감사 추적 불가, 권한 분리 불가
3. **기술 선택** — JumpCloud OIDC (왜 Google OAuth 대신), STS AssumeRole (왜 static key 대신)
4. **3단계 마이그레이션**
   - Phase 1: 인증 전환 (Google → JumpCloud OIDC)
   - Phase 2: 인가 전환 (Static Key → STS AssumeRole)
   - Phase 3: 토큰 라이프사이클 + 정적 키 폐기
5. **겪은 문제들**
   - OIDC trailing slash 한 글자 버그 → 50건 silent fallback
   - refresh_token 안 나오는 문제 (offline_access scope)
   - CLIENT_IP 보존 vs 토큰 갱신 트레이드오프
   - print() vs logger → 컨테이너 환경 디버깅 교훈
6. **폴백 전략** — 4주간 static key 폴백 유지 후 단계적 폐기
7. **결과** — Before/After 아키텍처, 7년 된 키 4개 삭제 완료
8. **돌이켜보면** — 인증 마이그레이션은 기술보다 운영 전환이 어렵다

## 관련 Jira
- DFM-1473: JumpCloud OIDC migration
- DFM-1492: STS AssumeRole infrastructure
- DFM-1502: Static key deactivation

## 타임라인
- 2026-03-31: OIDC 구현 시작
- 2026-04-01: OIDC 배포 완료
- 2026-04-01~02: STS 인프라 + OIDC trailing slash 버그 수정
- 2026-04-27: Static key fallback 50건 발견, 토큰 갱신 구현
- 2026-04-28: offline_access scope 누락 발견
- 2026-04-29: Static key 4개 비활성화
- 2026-05-03: Static key 4개 삭제 완료

## 핵심 데이터
- 정적 키 4개 (2018~2026, 최대 7년)
- STS 세션: 12시간
- 폴백 이벤트: ~50건/7일
- 폴백 기간: 4주

## 강점
- "trailing slash 한 글자" 디버깅 스토리
- 보안 + 인프라 → DE뿐 아니라 DevOps/SRE 독자도 관심
- 단계적 전환 + 폴백 전략이 실무적으로 유용
