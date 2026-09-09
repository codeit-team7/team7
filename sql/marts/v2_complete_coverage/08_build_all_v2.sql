/*
  MySQL CLI/Workbench에서 이 파일을 실행하면 v2 전체를 순서대로 구축한다.
  기존 7개 마트는 삭제하거나 덮어쓰지 않는다.

  주의: SOURCE는 mysql client 명령이다. Workbench에서는 이 파일 전체를
  여는 대신 아래 01~07 파일을 번호순으로 실행해도 된다.
*/

SELECT '[1/7] 사용자·학교 기준정보 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/01_user_acquisition_and_school_bridge_v2.sql;
SELECT '[1/7] 사용자·학교 기준정보 완료' AS build_progress;

SELECT '[2/7] 친구요청·학교 확산 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/02_friend_request_and_school_viral_v2.sql;
SELECT '[2/7] 친구요청·학교 확산 완료' AS build_progress;

SELECT '[3/7] 질문·후보 관계 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/03_question_candidate_exposure_v2.sql;
SELECT '[3/7] 질문·후보 관계 완료' AS build_progress;

SELECT '[4/7] Hackle 이벤트·방문 세션 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/04_hackle_event_enriched_24d_v2.sql;
SELECT '[4/7] Hackle 이벤트·방문 세션 완료' AS build_progress;

SELECT '[5/7] 가치·안전·생애주기 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/05_value_safety_lifecycle_v2.sql;
SELECT '[5/7] 가치·안전·생애주기 완료' AS build_progress;

SELECT '[6/7] 활동·누적 상태 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06_user_activity_and_cumulative_v2.sql;
SELECT '[6/7] 활동·누적 상태 완료' AS build_progress;

SELECT '[7/7] 전 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
SELECT '[7/7] 전체 구축·검사 완료' AS build_progress;
