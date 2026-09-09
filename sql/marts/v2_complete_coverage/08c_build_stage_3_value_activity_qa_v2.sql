/* Stage 3: 가치·안전·생애주기 + 활동·누적 + 최종 QA */

/*
   stage2_hackle_finalize에서 클라이언트가 비정상 종료돼도 서버가 마지막
   읽기 전용 COUNT DISTINCT 조인을 계속 수행할 수 있다. 30분 이상 실행 중이고
   해당 문장의 고유한 세 구문이 모두 일치하는 단 하나의 세션만 종료한다.
   0개 또는 2개 이상이면 어떤 세션도 종료하지 않는다.
*/
SELECT COUNT(*),MAX(ID)
INTO @stale_stage2_qa_count,@stale_stage2_qa_id
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND TIME>=1800
  AND INFO LIKE '%COUNT(DISTINCT u.service_user_id)%'
  AND INFO LIKE '%COUNT(DISTINCT a.derived_visit_id_30m)%'
  AND INFO LIKE '%ambiguous_session_forced_user_assignments%';

SELECT
    @stale_stage2_qa_count AS stale_stage2_qa_candidates,
    CASE WHEN @stale_stage2_qa_count=1 THEN @stale_stage2_qa_id END
        AS stale_stage2_qa_thread_to_stop;

SET @stop_stale_stage2_qa_sql = IF(
    @stale_stage2_qa_count=1,
    CONCAT('KILL QUERY ',@stale_stage2_qa_id),
    'DO 0'
);
PREPARE stmt_stop_stale_stage2_qa FROM @stop_stale_stage2_qa_sql;
EXECUTE stmt_stop_stale_stage2_qa;
DEALLOCATE PREPARE stmt_stop_stale_stage2_qa;

SELECT '[1/3] 가치·안전·생애주기 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/05_value_safety_lifecycle_v2.sql;
SELECT '[1/3] 가치·안전·생애주기 완료' AS build_progress;

SELECT '[2/3] 활동·누적 상태 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06_user_activity_and_cumulative_v2.sql;
SELECT '[2/3] 활동·누적 상태 완료' AS build_progress;

SELECT '[3/3] 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
SELECT '[3/3] Stage 3 완료: 전체 QA 모두 PASS' AS build_progress;
