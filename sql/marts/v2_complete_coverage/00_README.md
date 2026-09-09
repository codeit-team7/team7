# 전체 원초 테이블 커버리지 마트 v2

## 결론: 3개가 아니라 4개 작업축

이번에 새로 정비할 큰 분석축은 아래 4개입니다.

1. **바이럴·친구요청 확산**
2. **질문 후보 관계**
3. **Hackle 24일 행동·방문 세션**
4. **사용자 활동·리텐션 v2**

다만 이것은 “주제/파이프라인이 4개”라는 뜻이지, DB 테이블을 딱 4개만
만든다는 뜻은 아닙니다. 서로 다른 분석 단위를 한 표에 억지로 넣으면 중복
집계가 생기므로 각 축은 **원행 상세층 + 바로 쓰는 요약층**으로 구성합니다.

예를 들어 바이럴은 다음 2층이 핵심입니다.

```text
친구요청 1건 상세 mart_friend_request_event_v2
                    ↓ 안전하게 집계
학교 1개×1일 요약 mart_school_viral_daily_v2
```

개인 발신자·수신자·상태를 볼 때는 위 표, 학교 확산 곡선을 볼 때는 아래 표를
사용합니다. 학교 일별 표만 만들면 누가 누구에게 보냈는지가 사라집니다.

## 원칙

- 분석 노트북은 `final.*` 원초 테이블을 직접 읽지 않습니다.
- 원초 PK 한 건을 보존하는 상세 fact/dim/bridge를 먼저 만듭니다.
- 요약 마트는 상세 v2 마트에서만 집계합니다.
- `NULL/미관측`을 실제 0으로 바꾸지 않습니다.
- 현재 7개 마트 및 CSV를 삭제하거나 덮어쓰지 않습니다.
- 새 결과에는 `_v2`를 붙이고 QA를 통과한 뒤 기존 마트의 사용 여부를 정합니다.

## 팀원이 직접 사용할 핵심 결과

| 분석 목적 | 사용할 결과 | 한 행의 뜻 |
|---|---|---|
| 개인 간 초대/요청 분석 | `mart_friend_request_event_v2` | 친구요청 원행 1건 |
| 반복 요청·최종상태 요약 | `mart_friend_request_pair_summary_v2` | 발신자→수신자 방향쌍 1개 |
| 학교 바이럴 곡선 | `mart_school_viral_daily_v2` | 학교 1개×날짜 1일 |
| 사용자 바이럴 프로필 | `mart_user_viral_profile_v2` | 사용자 1명 |
| 질문 후보 관계 | `mart_question_candidate_exposure_v2` | 후보 원초 행 1건 |
| 질문 후보 통합 조회 | `vw_question_candidate_analysis_v2` | 후보 원초 행에 질문·owner·현재 관계를 조회 시 연결 |
| 질문세트/조각/투표 | `mart_question_set_record_v2`, `mart_question_piece_record_v2`, `mart_vote_record_v2` | 각 원초 레코드 1건 |
| Hackle 행동(편의 VIEW) | `mart_hackle_event_enriched_24d_v2` | Hackle 이벤트 1건 |
| Hackle 방문 퍼널 | `vw_hackle_visit_session_24d_v2` | 30분 비활동 기준 분석용 방문 1회 |
| 사용자 활동일 | `mart_user_activity_daily_v2` | 기록이 하나 이상 관측된 사용자×날짜 |
| 누적 사용자 상태 | `mart_user_cumulative_state_1y_v2` | 사용자 1명 |
| 결제·포인트·프로모션 | `mart_value_event_v2` | 가치 이벤트 원행 1건 |
| 신고·차단·피드백 | `mart_safety_event_v2` | 안전/피드백 원행 또는 누적 스냅샷 1건 |
| 가입·탈퇴 | `mart_lifecycle_event_v2` | 가입 또는 탈퇴 원행 1건 |

## 지원 결과

- `mart_user_acquisition_profile_v2`: 전체 사용자 가입·현재 소속·40명 맥락
- `dim_school_current_v2`, `dim_group_current_v2`: 사용자 0명인 기준행도 보존
- `mart_user_contact_record_v2`, `bridge_contact_inviter_v2`: 연락처 원행과 초대자 JSON 원소
- `bridge_current_friend_edge_v2`: 현재 친구 JSON 원소를 필요할 때만 펼치는 VIEW(물리 저장 0GB)
- `bridge_school_neighbor_v2`: 인근학교 방향성 관계 원행
- `bridge_question_set_piece_v2`, `bridge_question_piece_owner_v2`: 세트 위치와 owner 연결
- `bridge_question_owner_candidate_relation_v2`: 실제 질문에 등장한 owner-candidate 쌍의 현재 관계
- `bridge_hackle_session_property_raw_v2`, `bridge_hackle_device_property_raw_v2`: Hackle 속성 원행
- `fact_hackle_event_24d_v2`: 반복 문자열을 사전키로 바꾼 1,144만 건의 좁은 이벤트 fact
- `bridge_hackle_event_visit_assignment_v2`: 원 세션 안에서 15/30/60분 비활동 기준 방문 배정(기본 30분)
- `dim_hackle_event_text_attribute_v2`, `dim_hackle_visit_30m_v2`: 이벤트 문자열 사전과 30분 방문 요약
- `dim_hackle_session_resolved_v2`, `dim_hackle_device_resolved_v2`, `dim_hackle_user_resolved_v2`: 충돌을 강제 선택하지 않은 분석용 대표값
- `dim_hackle_user_property_v2`: Hackle 사용자 속성 전행
- `mart_attendance_record_v2`, `bridge_attendance_day_v2`: 출석 원행과 날짜 JSON 원소
- `dim_source_observation_calendar_v2`: 원천별 최소~최대 관측 범위
- `dim_promo_event_v2`, `mart_promo_event_receipt_v2`: 이벤트 정의와 지급 원행

## 기존 7개 처리

| 기존 마트 | 처리 |
|---|---|
| `mart_user_network_snapshot` | 원본 유지. v2의 사용자/관계 상세로 확장 |
| `mart_user_journey_funnel_1y` | 삭제하지 않되 순차 퍼널 사용 중단. `mart_user_cumulative_state_1y_v2`로 대체 |
| `mart_user_activity_daily(제거)` | 리텐션 사용 중단. `mart_user_activity_daily_v2`로 재구축 |
| `mart_safety_event` | 원본 유지. 원천 의미를 분리한 v2 사용 |
| `mart_question_exposure` | 세트×위치 부모 마트로 유지. 후보 상세 v2를 1:N 연결 |
| `mart_point_payment_event` | 원본 유지. 프로모션과 원천 범위를 명확히 한 v2 사용 |
| `mart_funnel_session` | 검증 후 사용 중단. Hackle event v2와 방문 view로 대체 |

## 절대 해석하면 안 되는 것

- 친구요청 `A`는 **현재 추출본의 최종 수락 상태**이지, 생성 당일 수락 이벤트가 아닙니다.
- 친구요청 수락률은 가입 전환율 `c`가 아닙니다. 수신자는 이미 계정이 있습니다.
- 현재 친구·학교·학년·반은 2023년 질문 당시 상태가 아닙니다.
- 질문 후보 원천에는 화면 표시 순서가 없습니다.
- `opening_time`은 실제 사용자가 질문을 본 시각으로 확정되지 않았습니다.
- Hackle 원본 `session_id`는 며칠 이어질 수 있습니다. 새 방문 ID는 분석자가 정한 30분 기준입니다.
- DB와 Hackle 사이 +9시간 상대 차이가 보여도 공식 UTC/KST 기준은 미확정입니다.
- 탈퇴 원천에는 `user_id`가 없으므로 개인 가입자와 연결할 수 없습니다.
- sparse activity에 행이 없다는 사실은 확인된 비활동과 같지 않습니다.

## 원천 25개 커버리지

`07_qa_complete_coverage_v2.sql`이 25개 원초표 각각에 대해 원천 행수와 1:1
보존 객체 행수를 대사합니다. `mart_build_source_coverage_qa_v2`의 25개 행이
모두 `PASS`, `mart_build_integrity_qa_v2`가 모두 `PASS`여야 구축 완료입니다.

## 실행 순서

1. `01_user_acquisition_and_school_bridge_v2.sql`
2. `02_friend_request_and_school_viral_v2.sql`
3. `03_question_candidate_exposure_v2.sql`
4. `04_hackle_event_enriched_24d_v2.sql`
5. `05_value_safety_lifecycle_v2.sql`
6. `06_user_activity_and_cumulative_v2.sql`
7. `07_qa_complete_coverage_v2.sql`

한 번에 실행할 때는 `08_build_all_v2.sql`을 사용합니다. 다만 C드라이브 여유
공간을 단계 사이에 확인하려면 아래 3단계 실행을 권장합니다.

```powershell
.\run_build_v2.ps1 -Stage stage1
.\run_build_v2.ps1 -Stage stage2
.\run_build_v2.ps1 -Stage stage3
```

각 단계는 비밀번호를 다시 묻습니다. 실행 출력은 같은 폴더의
`last_build_v2_stage1.log` 등으로도 저장됩니다.

runner는 단계별 대형 CTAS·인덱스·윈도우 작업의 임시 복사 공간까지 고려해
다음 최소 여유 공간을 강제합니다.

| 실행 모드 | 최소 C드라이브 여유 공간 | 이유 |
|---|---:|---|
| `all` | 40GB | 세 단계 결과와 대형 임시파일이 순차적으로 함께 존재 |
| `stage1` | 30GB | 1,714만 친구요청 fact·pair 요약·학교 일별 패널 |
| `stage2` | 25GB | 476만 후보와 1,144만 Hackle 이벤트·방문 윈도우 |
| `stage3` | 15GB | 가치·안전·sparse 사용자 일별·최종 QA |

`stage2`는 stage1의 전체 결과 객체가 있을 때만, `stage3`는 stage1과
stage2의 전체 결과 객체가 모두 있을 때만 실행됩니다. 빠진 객체가 있으면
runner가 누락된 이름을 출력하고 해당 단계의 첫 DDL 전에 중단합니다. 따라서
깨끗한 DB에서는 `stage2`나 `stage3`부터 시작할 수 없습니다. 앞 단계를 다시
만들었다면 그 아래 단계도 순서대로 다시 실행해 오래된 요약 마트가 남지 않게
해야 합니다.

40GB는 최종 테이블 크기만 뜻하지 않습니다. `CREATE TABLE AS SELECT` 뒤
인덱스를 추가하는 동안 이전 복사본과 새 복사본, 정렬 임시파일이 동시에 생길
수 있어 필요한 작업 여유까지 포함한 시작 기준입니다. 단계 사이에는 실제
여유 공간을 다시 확인하고, 중단된 구형 빌드의 임시/미완성 객체 정리가 끝난
뒤 다음 단계를 시작합니다.

모든 스크립트는 MySQL 8.0 기준이며 `final`을 읽어 `votes_mart`에 새 `_v2`
객체를 만듭니다. 1,700만 건 친구요청과 1,100만 건 Hackle 때문에 오래 걸릴
수 있지만, 3,610만 현재 친구 관계는 물리화하지 않고 질문에 실제 등장한
owner-candidate 쌍만 별도 저장합니다. 반복 문자열은 차원표에 한 번만 저장합니다.
