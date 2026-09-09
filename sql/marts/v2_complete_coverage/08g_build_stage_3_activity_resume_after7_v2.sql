/* ============================================================================
   Stage 3 activity resume-after-7 wrapper

   Exactly one stale, known 06-F stage-8 statement may be stopped.  The client
   connection (not only the statement) is killed to prevent the old mysql batch
   from advancing.  The wrapper waits for connection/rollback cleanup, then
   validates committed stages 1~7 before starting the event_sk-sequential path.
============================================================================ */

SET @activity_stage8_stale_seconds=1500;

/* Exact 06-F stage-8 statement signatures only. */
SELECT COUNT(*), MAX(ID)
INTO @active_activity_stage8_count, @active_activity_stage8_id
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND (
      (
          INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_hackle_day_v2%'
          AND INFO LIKE '%fact_hackle_event_24d_v2%'
      )
      OR (
          INFO LIKE '%INTO%hackle_source_group_count%'
          AND INFO LIKE '%fact_hackle_event_24d_v2%'
      )
      OR (
          INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_accum_v2%'
          AND INFO LIKE '%_wrk_user_activity_hackle_day_v2%'
          AND INFO LIKE '%hackle_event_count%'
      )
  );

SELECT COUNT(*), MAX(ID)
INTO @stale_activity_stage8_count, @stale_activity_stage8_id
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND TIME>=@activity_stage8_stale_seconds
  AND (
      (
          INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_hackle_day_v2%'
          AND INFO LIKE '%fact_hackle_event_24d_v2%'
      )
      OR (
          INFO LIKE '%INTO%hackle_source_group_count%'
          AND INFO LIKE '%fact_hackle_event_24d_v2%'
      )
      OR (
          INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_accum_v2%'
          AND INFO LIKE '%_wrk_user_activity_hackle_day_v2%'
          AND INFO LIKE '%hackle_event_count%'
      )
  );

/* Any simultaneous activity-work-table query makes takeover unsafe. */
SELECT COUNT(*)
INTO @active_activity_work_query_count
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND INFO LIKE '%votes_mart._wrk_user_activity_%';

SELECT
    @activity_stage8_stale_seconds AS stale_seconds_required,
    @active_activity_stage8_count AS exact_stage8_candidates,
    @stale_activity_stage8_count AS stale_exact_stage8_candidates,
    @active_activity_work_query_count AS all_activity_work_queries,
    CASE
      WHEN @active_activity_stage8_count=1
       AND @stale_activity_stage8_count=1
       AND @active_activity_stage8_id=@stale_activity_stage8_id
       AND @active_activity_work_query_count=1
      THEN @stale_activity_stage8_id
    END AS connection_to_stop;

SET @activity_stage8_stop_guard_sql = IF(
    (
        @active_activity_stage8_count=0
        AND @stale_activity_stage8_count=0
        AND @active_activity_work_query_count=0
    )
    OR (
        @active_activity_stage8_count=1
        AND @stale_activity_stage8_count=1
        AND @active_activity_stage8_id=@stale_activity_stage8_id
        AND @active_activity_work_query_count=1
    ),
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_UNSAFE_STAGE8_PROCESS_MATCH__'
);
PREPARE stmt_activity_stage8_stop_guard
    FROM @activity_stage8_stop_guard_sql;
EXECUTE stmt_activity_stage8_stop_guard;
DEALLOCATE PREPARE stmt_activity_stage8_stop_guard;

SET @activity_stage8_connection_to_stop = CASE
    WHEN @active_activity_stage8_count=1
     AND @stale_activity_stage8_count=1
     AND @active_activity_stage8_id=@stale_activity_stage8_id
     AND @active_activity_work_query_count=1
    THEN @stale_activity_stage8_id
END;

/*
   Connection kill is deliberate: KILL QUERY would let the old SOURCE batch race
   into validation/merge/stage 9.  Only the unique exact stale ID can reach here.
*/
SET @stop_stale_activity_stage8_connection_sql = IF(
    @activity_stage8_connection_to_stop IS NOT NULL,
    CONCAT('KILL CONNECTION ', @activity_stage8_connection_to_stop),
    'DO 0'
);
PREPARE stmt_stop_stale_activity_stage8_connection
    FROM @stop_stale_activity_stage8_connection_sql;
EXECUTE stmt_stop_stale_activity_stage8_connection;
DEALLOCATE PREPARE stmt_stop_stale_activity_stage8_connection;

/* Wait up to 15 minutes; the process row remains while server-side rollback runs. */
DROP PROCEDURE IF EXISTS votes_mart._wait_activity_stage8_connection_stop_v2;
DELIMITER $$
CREATE PROCEDURE votes_mart._wait_activity_stage8_connection_stop_v2()
BEGIN
    DECLARE wait_attempt INT DEFAULT 0;
    DECLARE matching_connection_count INT DEFAULT 0;

    IF @activity_stage8_connection_to_stop IS NOT NULL THEN
        SELECT COUNT(*)
        INTO matching_connection_count
        FROM information_schema.PROCESSLIST
        WHERE ID=@activity_stage8_connection_to_stop;

        WHILE matching_connection_count>0 AND wait_attempt<900 DO
            DO SLEEP(1);
            SET wait_attempt=wait_attempt+1;
            SELECT COUNT(*)
            INTO matching_connection_count
            FROM information_schema.PROCESSLIST
            WHERE ID=@activity_stage8_connection_to_stop;
        END WHILE;
    END IF;

    SET @activity_stage8_cleanup_wait_seconds=wait_attempt;
    SET @activity_stage8_remaining_connection_count=matching_connection_count;
END$$
DELIMITER ;

CALL votes_mart._wait_activity_stage8_connection_stop_v2();
DROP PROCEDURE votes_mart._wait_activity_stage8_connection_stop_v2;

/* Recheck every activity work-table statement after rollback cleanup. */
SELECT COUNT(*)
INTO @activity_stage8_other_work_query_count
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND INFO LIKE '%votes_mart._wrk_user_activity_%';

SELECT
    @activity_stage8_cleanup_wait_seconds AS cleanup_wait_seconds,
    @activity_stage8_remaining_connection_count AS remaining_stopped_connection,
    @activity_stage8_other_work_query_count AS other_activity_work_queries;

SET @activity_stage8_cleanup_guard_sql = IF(
    @activity_stage8_remaining_connection_count=0
    AND @activity_stage8_other_work_query_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_STAGE8_CONNECTION_OR_QUERY_REMAINS__'
);
PREPARE stmt_activity_stage8_cleanup_guard
    FROM @activity_stage8_cleanup_guard_sql;
EXECUTE stmt_activity_stage8_cleanup_guard;
DEALLOCATE PREPARE stmt_activity_stage8_cleanup_guard;

SELECT '[1/4] stages 1~7 validation + sequential stage 8/9 시작'
    AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06g_user_activity_resume_after_stage7_v2.sql;
SELECT '[1/4] stages 1~7 validation + sequential stage 8/9 완료'
    AS build_progress;

SELECT '[2/4] activity final projection/publish 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06h_user_activity_project_publish_after_stage7_v2.sql;
SELECT '[2/4] activity final projection/publish 완료' AS build_progress;

SELECT '[3/4] 누적 상태 tail 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06e_user_cumulative_state_tail_v2.sql;
SELECT '[3/4] 누적 상태 tail 완료' AS build_progress;

SELECT '[4/4] 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
/* 07의 동적 QA guard가 실패하면 mysql batch가 여기 도달하지 않는다. */

/* Full QA PASS is the sole cleanup gate. */
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after7_old;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_signup_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_identified_event_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_session_map_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_value_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_ping_day_v2;
DROP TABLE votes_mart._wrk_user_activity_accum_v2;

SELECT '[4/4] Stage 3 resume-after-7 완료: 전체 QA 모두 PASS'
    AS build_progress;

