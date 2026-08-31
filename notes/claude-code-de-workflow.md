# 블로그: Claude Code로 DE 업무 자동화

## 제목 후보
"Claude Code로 데이터 엔지니어링 업무 자동화하기 — CLAUDE.md 설계기"

## 포지셔닝
코딩 자동화가 아니라 업무 오케스트레이션 자동화.
DE/엔지니어링 리더의 컨텍스트 스위칭 문제를 Claude Code로 어떻게 해결했는가.

## 강점
- 세션 간 컨텍스트 유지 — STATUS.md + 로그로 즉시 복원
- 멀티 도구 오케스트레이션 — Jira/Notion/Google Docs/Athena/슬랙을 한 세션에서
- 운영 지식 축적 — 업무 과정 자체가 문서화

## 글 구조 (초안)

### 1. 왜 Claude Code인가
- DE 업무: AWS CLI, SSH, Athena, Jira, Notion, Google Docs, 슬랙 — 도구 파편화
- 코딩보다 오케스트레이션이 필요

### 2. CLAUDE.md — 562줄짜리 운영 매뉴얼
- "내가 매번 말하는 걸 한 번만 적는다"
- 20+ 섹션: 세션 시작, 태스크 생성, 로깅, 주간 업데이트, 백업 등

### 3. 자동화 패턴 — 실제 예시
- 세션 시작 → 컨텍스트 자동 복원
- 태스크 생성 → Jira + Notion + STATUS.md 동시
- 주간 업데이트 → 로그/Jira/Google Docs 수집 → 슬랙 초안
- 백업 → 민감 파일 자동 제외 후 커밋+푸시

### 4. 도구 통합
- AWS CLI (로컬 vs JupyterHub 분리)
- jupyter_exec.py, gdocs.py
- Jira/Notion API curl 직접 호출 (MCP 호환 이슈)

### 5. 지식 관리 체계
- knowledge/, logs/, STATUS.md, secrets/

### 6. 실패와 교훈
- CLAUDE.md 길어질수록 지시 무시
- 컨텍스트 윈도우 한계
- "하지 마라" 규칙이 계속 늘어남

### 7. Before/After (수치 필요)

## 상태
아직 확신 없음. 수치/경험 더 쌓이면 재검토.
