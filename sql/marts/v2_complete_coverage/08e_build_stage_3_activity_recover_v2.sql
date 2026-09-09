/* Stage 3 activity recovery: unique stale activity stop + staged activity + cumulative + QA */

/*
   다른 세션에서 실행 중인 기존 06-D giant-CTE 문장을 세 표식으로 식별한다.
   - 실행 중인 일치 문장이 없으면 이미 끝났거나 중단된 것으로 보고 복구를 진행한다.
   - 실행 중인 일치 문장이 정확히 하나이고 20분을 넘었을 때만 KILL QUERY한다.
   - 실행 중인데 0 stale(아직 20분 미만) 또는 복수이면 DDL 전에 hard abort한다.
*/
SELECT COUNT(*), MAX(ID)
INTO @active_activity_count, @active_activity_id
FROM information_schema.PROCESSLIST
WHERE ID <> CONNECTION_ID()
  AND COMMAND = 'Query'
  AND INFO LIKE '%CREATE TABLE votes_mart.mart_user_activity_daily_v2%'
  AND INFO LIKE '%attendance_day AS%'
  AND INFO LIKE '%friend_day AS%';

SELECT COUNT(*), MAX(ID)
INTO @stale_activity_count, @stale_activity_id
FROM information_schema.PROCESSLIST
WHERE ID <> CONNECTION_ID()
  AND COMMAND = 'Query'
  AND TIME > 1200
  AND INFO LIKE '%CREATE TABLE votes_mart.mart_user_activity_daily_v2%'
  AND INFO LIKE '%attendance_day AS%'
  AND INFO LIKE '%friend_day AS%';

SELECT
    @active_activity_count AS active_activity_candidates,
    @stale_activity_count AS stale_activity_candidates,
    CASE
      WHEN @active_activity_count=1 AND @stale_activity_count=1
      THEN @stale_activity_id
    END AS activity_thread_to_stop;

SET @activity_stop_guard_sql = IF(
    @active_activity_count=0
    OR (@active_activity_count=1 AND @stale_activity_count=1
        AND @active_activity_id=@stale_activity_id),
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RECOVERY_UNSAFE_PROCESS_MATCH__'
);
PREPARE stmt_activity_stop_guard FROM @activity_stop_guard_sql;
EXECUTE stmt_activity_stop_guard;
DEALLOCATE PREPARE stmt_activity_stop_guard;

SET @stop_stale_activity_sql = IF(
    @active_activity_count=1 AND @stale_activity_count=1,
    CONCAT('KILL QUERY ', @stale_activity_id),
    'DO 0'
);
PREPARE stmt_stop_stale_activity FROM @stop_stale_activity_sql;
EXECUTE stmt_stop_stale_activity;
DEALLOCATE PREPARE stmt_stop_stale_activity;

/*
   KILL QUERY는 interrupt 요청이므로, 큰 CTAS가 임시 객체를 정리하는 동안 같은
   PROCESSLIST 행이 잠시 남을 수 있다. 1초 간격으로 최대 300초만 기다린다.
   제한 시간 뒤에도 남아 있으면 아래 hard guard가 recovery DDL을 막는다.
*/
DROP PROCEDURE IF EXISTS votes_mart._wait_activity_query_stop_v2;
DELIMITER $$
CREATE PROCEDURE votes_mart._wait_activity_query_stop_v2()
BEGIN
    DECLARE wait_attempt INT DEFAULT 0;
    DECLARE matching_query_count INT DEFAULT 1;

    WHILE matching_query_count > 0 AND wait_attempt < 300 DO
        SELECT COUNT(*)
        INTO matching_query_count
        FROM information_schema.PROCESSLIST
        WHERE ID <> CONNECTION_ID()
          AND COMMAND = 'Query'
          AND INFO LIKE '%CREATE TABLE votes_mart.mart_user_activity_daily_v2%'
          AND INFO LIKE '%attendance_day AS%'
          AND INFO LIKE '%friend_day AS%';

        IF matching_query_count > 0 THEN
            DO SLEEP(1);
            SET wait_attempt = wait_attempt + 1;
        END IF;
    END WHILE;

    SET @remaining_activity_count = matching_query_count;
    SET @activity_cleanup_wait_seconds = wait_attempt;
END$$
DELIMITER ;

CALL votes_mart._wait_activity_query_stop_v2();
DROP PROCEDURE votes_mart._wait_activity_query_stop_v2;

SELECT
    @activity_cleanup_wait_seconds AS activity_cleanup_wait_seconds,
    @remaining_activity_count AS remaining_activity_queries;

SET @activity_stopped_guard_sql = IF(
    @remaining_activity_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_QUERY_STILL_RUNNING__'
);
PREPARE stmt_activity_stopped_guard FROM @activity_stopped_guard_sql;
EXECUTE stmt_activity_stopped_guard;
DEALLOCATE PREPARE stmt_activity_stopped_guard;

SELECT '[1/3] staged activity recovery 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06d_user_activity_staged_recovery_v2.sql;
SELECT '[1/3] staged activity recovery 완료' AS build_progress;

SELECT '[2/3] 누적 상태 tail 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06e_user_cumulative_state_tail_v2.sql;
SELECT '[2/3] 누적 상태 tail 완료' AS build_progress;

SELECT '[3/3] 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
/* 07의 동적 QA guard가 실패하면 mysql batch가 여기 도달하지 않는다. */
SELECT '[3/3] Stage 3 activity recovery 완료: 전체 QA 모두 PASS' AS build_progress;
