# 블로그: 슬랙 봇에서 AI 에이전트 프레임워크까지

## 제목 후보
"슬랙 봇에서 AI 에이전트 프레임워크까지 — 4개월간의 삽질기"

## 포지셔닝
회사 블로그 후보(AI 에이전트로 데이터 플랫폼 운영하기)와는 별도.
회사 블로그 = 도입 결과와 운영 수치, 개인 블로그 = 설계 판단과 실패에서 배운 것.

## 핵심 스토리
슬랙 봇 직접 개발 → 프레임워크(OpenClaw) 도입 → 멀티 에이전트 설계 → 현실의 벽 → 싱글 operator로 전환

## 글 구조

### 1. 직접 만들어봤다
- 슬랙 봇으로 시작 — DE 운영 자동화가 목표
- 세션/컨텍스트 관리에서 벽을 만남
- 사내 다른 팀(ML 등)은 자체 개발 중이었지만, DE팀은 소수 인원(4~5명) + 본업이 바빠서 자체 개발에 시간 쏟기 어려움
- 오픈소스를 레버리지하는 게 합리적이라 판단

### 2. OpenClaw을 선택했다
- 당시 핫했던 에이전트 프레임워크
- (선택 이유 보충 필요 — 다른 프레임워크 대비 장점이 있었는지?)

### 3. 멀티 에이전트로 설계했다
- 초기 구조 (2026-03, v1):
  - 에이전트 6개: jess, parker, gray, devesh (개인) + operator (운영) + public (외부 대응)
  - per-member workspace: /workspace/jess, /workspace/parker 등 각각 독립
  - docker-compose: workspace 5개 볼륨 마운트
  - 에이전트 전용 문서: OPERATIONS-PLAYBOOK.md, Pattern Matcher skill, Incident Logger skill
  - requireMention: false — 멘션 없어도 응답
- 기대한 그림:
  - operator가 알림 자동 대응
  - 개인 에이전트가 각 멤버의 업무 보조
  - public이 외부 팀 요청 대응
  - 좋은 패턴은 개인 브랜치 → master PR로 공유

### 4. 현실은 달랐다
- **workspace 설계가 매우 어려움** — 에이전트 6개한테 각각 "넌 이렇게 행동해"를 문서로 설계하는 건 프롬프트 엔지니어링 난이도가 극도로 높음. 의도대로 동작 안 하는 경우 다수
- **개인/공통 영역 경계 모호** — 크레덴셜이 공유 컨텍스트로 새어나갈 리스크
- **PR로 패턴 공유가 깔끔하지 않음** — 개인 작업물과 공유할 패턴이 뒤섞여서 구분 없이 넘기는 경우 빈번
- **개인 에이전트가 사실상 불필요** — 팀원들이 각자 Claude Code 등 자기 스타일로 AI 도구를 이미 잘 쓰고 있었음. 공유 플랫폼 위의 개인 에이전트는 각자 선호도를 반영하기 어려움
- **public vs operator 역할 구분이 애매** — 둘 다 채널에서 멘션 받고 knowledge 보고 답하는 거라 나눈 의미가 없었음 → public이 제일 먼저 제거됨 (2026-03-31)
- **코어도 안정화 안 된 상태** — operator 자체가 아직 제대로 안 돌아가는데 복잡한 멀티 에이전트 운영은 시기상조

### 5. 싱글 operator로 돌아왔다 (2026-06, v2)
- 에이전트 1개: operator만
- workspace 통합: /workspace/master 하나
- knowledge-first 전환:
  - 에이전트 전용 문서(RESPONSE-PATTERNS.md, TEAM-MEMORY.md, skills/, operations/) 삭제
  - 팀 전체가 쓰는 DE knowledge 레포를 SSOT로
  - 33개 DE 레포 sandbox에 직접 마운트
- 채널 제어 세분화:
  - requireMention: true — 멘션에만 응답
  - thread.requireExplicitMention: true — 스레드에서도 명시적 멘션 필요
  - 에이전트가 "안 끼어드는 것"이 "잘 끼어드는 것"만큼 중요
- systemPrompt 간소화: 채널 컨텍스트만, 행동 로직은 AGENTS.md 하나로 집약

### 6. 배운 것
- 코어(운영 자동화)가 먼저, 확장(개인 에이전트)은 나중에
- 복잡한 아키텍처로 시작하지 말고, 단순한 구조에서 필요에 의해 확장
- 에이전트 역할을 나눠봤자 실제 행동이 비슷하면 나눈 의미가 없다
- 에이전트 전용 지식을 따로 관리하면 이중 부담 — 팀 문서를 직접 참조하는 게 맞음

## 필요한 수치 (아직 부족)
- 패턴 학습 건수 (memory/patterns/)
- 자동 대응 vs 에스컬레이션 비율
- 운영 전후 MTTR 변화

## 참고 자료
- quaritch 레포 git 히스토리 (50 commits, 2026-03-19 ~ 현재)
- workspace 레포 git 히스토리 (에이전트 자동 커밋 포함)
- knowledge/ai-automation.md — 현행 아키텍처
- 초기 커밋 a5c1113 (6 에이전트 구조)
- public 제거 커밋 47562be (2026-03-31)
- 싱글 전환 커밋 95ad095 (2026-06-09)

## 메모
- knowledge/ai-automation.md에 현행 아키텍처 업데이트 완료 (별도)
- 회사 블로그 주제 후보(docs/blog/README.md)에도 메모됨
- 수치가 쌓이면 회사 블로그는 "도입 결과", 개인 블로그는 "설계 판단 히스토리"로 분리 가능
- OIDC/STS 단독 글은 독자 좁음 → 어피닛 블로그 거버넌스 글에 흡수
