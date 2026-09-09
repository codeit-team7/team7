/* Stage 3 recovery: stuck value load 중지 + 분할 적재 + 후속 구축 + 최종 QA */

/*
   20분을 넘긴 기존 05-C INSERT 중 고유한 세 원천 표식이 모두 있는 세션만
   후보로 삼는다. 정확히 하나일 때만 KILL QUERY를 실행하며, 0개/복수이면
   아무 세션도 중지하지 않는다.
*/
SELECT COUNT(*), MAX(ID)
INTO @stale_stage3_value_count, @stale_stage3_value_id
FROM information_schema.PROCESSLIST
WHERE ID <> CONNECTION_ID()
  AND COMMAND = 'Query'
  AND TIME > 1200
  AND INFO LIKE '%INSERT INTO votes_mart.mart_value_event_v2%'
  AND INFO LIKE '%accounts_pointhistory%'
  AND INFO LIKE '%hackle_events%';

SELECT
    @stale_stage3_value_count AS stale_stage3_value_candidates,
    CASE WHEN @stale_stage3_value_count=1 THEN @stale_stage3_value_id END
        AS stale_stage3_value_thread_to_stop;

SET @stop_stale_stage3_value_sql = IF(
    @stale_stage3_value_count=1,
    CONCAT('KILL QUERY ', @stale_stage3_value_id),
    'DO 0'
);
PREPARE stmt_stop_stale_stage3_value FROM @stop_stale_stage3_value_sql;
EXECUTE stmt_stop_stale_stage3_value;
DEALLOCATE PREPARE stmt_stop_stale_stage3_value;

SELECT '[1/4] 가치 이벤트 분할 복구 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/05c_value_event_recovery_v2.sql;
SELECT '[1/4] 가치 이벤트 분할 복구 완료' AS build_progress;

SELECT '[2/4] 안전·생애주기 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/05d_safety_lifecycle_recovery_tail_v2.sql;
SELECT '[2/4] 안전·생애주기 구축 완료' AS build_progress;

SELECT '[3/4] 활동·누적 상태 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06_user_activity_and_cumulative_v2.sql;
SELECT '[3/4] 활동·누적 상태 구축 완료' AS build_progress;

SELECT '[4/4] 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
/* 07의 동적 QA guard가 실패하면 mysql batch가 여기 도달하지 않는다. */
SELECT '[4/4] Stage 3 recovery 완료: 전체 QA 모두 PASS' AS build_progress;

