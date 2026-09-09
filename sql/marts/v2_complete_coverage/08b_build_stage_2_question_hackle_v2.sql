/* Stage 2: 질문·후보 관계 + Hackle 이벤트·분석용 방문 */
SELECT '[1/2] 질문·후보 관계 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/03_question_candidate_exposure_v2.sql;
SELECT '[1/2] 질문·후보 관계 완료' AS build_progress;

SELECT '[2/2] Hackle 이벤트·방문 세션 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/04_hackle_event_enriched_24d_v2.sql;
SELECT '[2/2] Stage 2 완료' AS build_progress;
