/* Stage 2 복구: 이미 검증된 질문·후보 객체는 유지하고 Hackle만 구축 */
SELECT '[1/1] Hackle 이벤트·방문 세션 복구 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/04_hackle_event_enriched_24d_v2.sql;
SELECT '[1/1] Stage 2 Hackle 복구 완료' AS build_progress;
