# v2 마트 구축 전체 SQL·QA 실행 아카이브

## 이 폴더가 보존하는 범위

이 폴더는 2026-09-08 오전부터 2026-09-09 새벽까지 약 10시간 30분 동안
`final` 원초 데이터를 읽어 `votes_mart`의 v2 분석 객체를 구축하면서 작성·실행한
SQL과 실행 도구를 한곳에 보존한 아카이브다.

- SQL: **29개, 총 7,915줄**
- 실행 도구: PowerShell 3개, Python 오프라인 검사기 1개
- 보존된 실행 로그: 6개
- 최종 DB 검증: 원초 25개 커버리지 및 무결성 QA **전체 PASS**
- 기존 7개 마트는 삭제하거나 덮어쓰지 않고 `_v2` 객체만 생성

비밀번호·API 키·토큰은 파일에 저장하지 않았다. MySQL 비밀번호는 실행할 때
터미널에서 직접 입력하도록 되어 있다.

## 반드시 알아야 할 점

1. `01`~`07`은 최종 정의가 반영된 **깨끗한 DB 재구축용 본선 SQL**이다.
2. `02z`, `04b`~`04c`, `05c`~`05d`, `06d`~`06h`는 장시간 빌드가 중간에
   실패하거나 연결이 끊긴 뒤 이미 커밋된 결과를 재사용하기 위해 만든
   **당시의 복구·재개 SQL**이다. 실행 이력을 재현하기 위해 삭제하면 안 된다.
3. `08*`는 본선 또는 복구 SQL을 순서대로 호출하는 실행 진입점이다.
4. `07_qa_complete_coverage_v2.sql`이 최종 전체 QA다. QA 실패 시 MySQL 배치를
   오류로 종료하도록 guard가 포함되어 있다.
5. `last_*.log`는 보존된 실행 증거다. runner가 단계별 `last` 로그를 쓰는 구조라
   동일 단계의 이전 시도 콘솔 출력은 덮어써졌을 수 있다. 따라서 로그 6개가
   모든 시행착오의 콘솔 출력 전체를 뜻하지는 않지만, 작업 중 작성된 SQL 파일
   29개는 전부 이 폴더에 남아 있다.
6. SQL 내부 `SOURCE C:/Users/lucy5/Desktop/team7/...` 경로는 실제 실행 당시의
   경로다. 다른 컴퓨터에서 재실행할 때는 clone 위치에 맞게 경로를 바꿔야 한다.

## 가장 단순한 최종 재구축 경로

깨끗한 환경에서 다시 만들 때는 아래 본선 순서만 실행한다. 현재 본선 SQL에는
작업 중 발견한 타입·정의 수정이 반영되어 있으므로 과거 복구 파일을 다시 실행할
필요는 없다.

1. `01_user_acquisition_and_school_bridge_v2.sql`
2. `02_friend_request_and_school_viral_v2.sql`
3. `03_question_candidate_exposure_v2.sql`
4. `04_hackle_event_enriched_24d_v2.sql`
5. `05_value_safety_lifecycle_v2.sql`
6. `06_user_activity_and_cumulative_v2.sql`
7. `07_qa_complete_coverage_v2.sql`

한 번에 호출하는 기록용 진입점은 `08_build_all_v2.sql`, 저장공간을 확인하며
나누어 실행하는 진입점은 `08a`, `08b`, `08c`다. 실제로는 1,700만 건 친구요청,
1,100만 건 Hackle 이벤트와 인덱스 생성 때문에 분할 실행을 권장한다.

## SQL 29개 전체 목록

### 1. 본선 구축 SQL

| 파일 | 역할 |
|---|---|
| `01_user_acquisition_and_school_bridge_v2.sql` | 학교·그룹·사용자 가입·연락처·현재 친구·인근학교 기준정보 |
| `02_friend_request_and_school_viral_v2.sql` | 친구요청 원행, 방향쌍, 사용자 바이럴 프로필, 학교×일 확산 마트 |
| `03_question_candidate_exposure_v2.sql` | 질문·세트·조각·투표·후보·owner 관계 |
| `04_hackle_event_enriched_24d_v2.sql` | Hackle 원행 보존, 속성 정규화, 사용자 식별, 30분 방문 재세션화 |
| `05_value_safety_lifecycle_v2.sql` | 포인트·결제·프로모션, 안전·피드백, 가입·탈퇴, 출석 |
| `06_user_activity_and_cumulative_v2.sql` | sparse 사용자×활동일과 사용자 누적 상태 |
| `07_qa_complete_coverage_v2.sql` | 원초 25개 행수 커버리지, PK·중복·범위·NULL·기간 무결성 QA |

### 2. 구축 중 생성된 수정·복구·재개 SQL

| 파일 | 생성 이유 |
|---|---|
| `02z_rate_precision_repair_v2.sql` | 초기 DECIMAL 정밀도 경고 후 사용자·학교 비율 컬럼을 복구하고 재검증 |
| `04b_hackle_resume_after_text_dim_v2.sql` | Hackle 문자열 차원 생성 이후부터 대용량 구축 재개 |
| `04c_hackle_finalize_after_assignment_v2.sql` | 이벤트→방문 배정 이후 인덱스·방문 요약·최종 객체 생성 재개 |
| `05c_value_event_recovery_v2.sql` | 가치 이벤트 구축 중단 후 안전한 체크포인트에서 재개 |
| `05d_safety_lifecycle_recovery_tail_v2.sql` | 안전·생애주기·출석·관측기간 꼬리 단계 복구 |
| `06d_user_activity_staged_recovery_v2.sql` | 사용자 일별 활동을 원천별 누산기로 나눠 재구축 |
| `06e_user_cumulative_state_tail_v2.sql` | 일별 활동 완료 후 사용자 1행 누적 상태 생성 |
| `06f_user_activity_resume_after_stage5_v2.sql` | 활동 누산 5단계 완료 지점에서 재개 |
| `06g_user_activity_resume_after_stage7_v2.sql` | 활동 누산 7단계 완료 지점에서 Hackle·가입 단계를 재개 |
| `06h_user_activity_project_publish_after_stage7_v2.sql` | 누산기를 최종 사용자×일 마트로 투영·교체·검증 |

### 3. 실행 진입점과 안전 중단 SQL

| 파일 | 역할 |
|---|---|
| `08_build_all_v2.sql` | 본선 01~07 전체 호출 |
| `08a_build_stage_1_core_v2.sql` | Stage 1: 사용자·학교·친구요청 호출 |
| `08b_build_stage_2_question_hackle_v2.sql` | Stage 2: 질문과 Hackle 호출 |
| `08b2_build_stage_2_hackle_only_v2.sql` | 질문 단계 완료 후 Hackle만 재호출 |
| `08b3_build_stage_2_hackle_resume_v2.sql` | Hackle 중간 체크포인트 복구 호출 |
| `08b4_build_stage_2_hackle_finalize_v2.sql` | Hackle 방문 배정 이후 최종화 호출 |
| `08c_build_stage_3_value_activity_qa_v2.sql` | Stage 3 본선 가치·활동·최종 QA 호출 |
| `08d_build_stage_3_recover_v2.sql` | Stage 3 가치 단계 복구 진입점 |
| `08e_build_stage_3_activity_recover_v2.sql` | Stage 3 활동 누산 복구 진입점 |
| `08f_build_stage_3_activity_resume_after5_v2.sql` | 활동 5단계 이후 복구와 QA 호출 |
| `08g_build_stage_3_activity_resume_after7_v2.sql` | 활동 7단계 이후 복구·publish·누적·전체 QA 호출 |
| `stop_orphan_build_v2.sql` | 끊긴 이전 MySQL 대용량 빌드 연결을 식별·중지하기 위한 안전 SQL |

## SQL 외 실행·검사 파일

| 파일 | 역할 |
|---|---|
| `run_build_v2.ps1` | 단계 선택, 여유공간 확인, 선행 객체 검사, MySQL 실행, 로그 저장 |
| `run_rate_precision_repair_v2.ps1` | 비율 정밀도 복구 SQL 실행기 |
| `stop_orphan_build_v2.ps1` | 고아 빌드 중지 SQL 실행기 |
| `validate_sql_bundle.py` | SQL 파일·SOURCE 관계·위험 패턴을 DB 접속 전에 검사 |

## 보존된 실행 로그

| 로그 | 확인할 내용 |
|---|---|
| `last_build_v2_stage1.log` | Stage 1 실제 생성 결과와 1,714만 친구요청 행 대사 |
| `last_rate_precision_repair_v2.log` | 비율 정밀도 복구 및 범위·값 일치 PASS |
| `last_build_v2_stage2.log` | 질문 후보 관계 구축 결과 |
| `last_build_v2_stage2_hackle.log` | Hackle 최초 재실행 중 발생한 구문 오류 기록 |
| `last_build_v2_stage3_activity_recover.log` | 활동 마트 단계별 누산 복구 진행 기록 |
| `last_build_v2_stage3_activity_resume_after7.log` | 최종 재개, publish, 원초 커버리지·무결성 전체 PASS 기록 |

## 최종 성공 판정 근거

최종 로그 `last_build_v2_stage3_activity_resume_after7.log`의 끝에서 다음을 확인한다.

```text
final_qa_fail_count  final_qa_status
0                    PASS
```

같은 로그에는 주요 분석 영역의 최종 행 수도 남아 있다.

| 영역 | 최종 행 수 |
|---|---:|
| 친구요청 | 17,147,175 |
| 질문 후보 | 4,769,609 |
| Hackle 24일 이벤트 | 11,441,319 |
| sparse 사용자 활동일 | 8,224,667 |

`07_qa_complete_coverage_v2.sql`은 결과를 두 테이블에 남긴다.

- `votes_mart.mart_build_source_coverage_qa_v2`: 원초 25개별 보존 행수 대사
- `votes_mart.mart_build_integrity_qa_v2`: 키 중복, 범위, NULL, 기간 등 무결성 검사

두 QA 테이블에서 `qa_status='FAIL'`이 0건이어야 최종 성공이다.

## GitHub에 올릴 때

이 폴더 전체를 올린다. 복구 SQL은 최신 재구축에 필요 없더라도 실제 장시간
구축 과정을 설명하는 재현 기록이므로 제외하지 않는다. `.gitignore`에는 이
폴더의 `last_*.log`만 예외로 두어 QA 증거 로그도 Git에 포함되도록 설정했다.

