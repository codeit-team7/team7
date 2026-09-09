/* ============================================================================
   Stage 3 activity resume-after-5 wrapper

   호출 시점 기본 조건: 기존 06-D tail INSERT가 45분 이상 실행 중인 경우.
   정확히 한 개의 알려진 tail writer만 stale일 때 그 연결을 종료하고, 연결과
   rollback 정리가 끝난 뒤에만 1~5 checkpoint를 보존한 06-F를 시작한다.
============================================================================ */

SET @activity_resume_stale_seconds = 2700;

SELECT COUNT(*), MAX(ID)
INTO @active_activity_tail_count, @active_activity_tail_id
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_accum_v2%'
  AND (
      INFO LIKE '%db_ping_received_record_count%'
      OR INFO LIKE '%point_earn_event_count%'
      OR INFO LIKE '%hackle_event_count%'
      OR (INFO LIKE '%signup_record_count%'
          AND INFO LIKE '%mart_user_acquisition_profile_v2%')
  );

SELECT COUNT(*), MAX(ID)
INTO @stale_activity_tail_count, @stale_activity_tail_id
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND TIME>=@activity_resume_stale_seconds
  AND INFO LIKE '%INSERT INTO votes_mart._wrk_user_activity_accum_v2%'
  AND (
      INFO LIKE '%db_ping_received_record_count%'
      OR INFO LIKE '%point_earn_event_count%'
      OR INFO LIKE '%hackle_event_count%'
      OR (INFO LIKE '%signup_record_count%'
          AND INFO LIKE '%mart_user_acquisition_profile_v2%')
  );

SELECT
    @activity_resume_stale_seconds AS stale_seconds_required,
    @active_activity_tail_count AS active_tail_candidates,
    @stale_activity_tail_count AS stale_tail_candidates,
    CASE
      WHEN @active_activity_tail_count=1
       AND @stale_activity_tail_count=1
       AND @active_activity_tail_id=@stale_activity_tail_id
      THEN @stale_activity_tail_id
    END AS connection_to_stop;

/* 0개(이미 끝남/수동 종료됨) 또는 정확히 1개의 stale 일치만 허용한다. */
SET @activity_tail_stop_guard_sql = IF(
    @active_activity_tail_count=0
    OR (
        @active_activity_tail_count=1
        AND @stale_activity_tail_count=1
        AND @active_activity_tail_id=@stale_activity_tail_id
    ),
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_UNSAFE_TAIL_PROCESS_MATCH__'
);
PREPARE stmt_activity_tail_stop_guard FROM @activity_tail_stop_guard_sql;
EXECUTE stmt_activity_tail_stop_guard;
DEALLOCATE PREPARE stmt_activity_tail_stop_guard;

SET @activity_connection_to_stop = CASE
    WHEN @active_activity_tail_count=1
     AND @stale_activity_tail_count=1
     AND @active_activity_tail_id=@stale_activity_tail_id
    THEN @stale_activity_tail_id
END;

/*
   KILL QUERY 뒤 기존 mysql batch가 다음 문장으로 넘어가는 작은 race를 없애기 위해
   이 유일하게 식별된 build 연결 자체를 종료한다. 현재 INSERT가 아직 실행 중이면
   그 한 문장만 rollback되고 이전 autocommit 1~5단계는 보존된다. 막 커밋된 경우도
   06-F의 replacement merge가 같은 값을 다시 써서 결과가 동일하다.
*/
SET @stop_stale_activity_connection_sql = IF(
    @activity_connection_to_stop IS NOT NULL,
    CONCAT('KILL CONNECTION ', @activity_connection_to_stop),
    'DO 0'
);
PREPARE stmt_stop_stale_activity_connection
    FROM @stop_stale_activity_connection_sql;
EXECUTE stmt_stop_stale_activity_connection;
DEALLOCATE PREPARE stmt_stop_stale_activity_connection;

/* 연결 행이 사라질 때까지 최대 15분 기다린다. rollback 정리가 길면 hard abort한다. */
DROP PROCEDURE IF EXISTS votes_mart._wait_activity_resume_connection_stop_v2;
DELIMITER $$
CREATE PROCEDURE votes_mart._wait_activity_resume_connection_stop_v2()
BEGIN
    DECLARE wait_attempt INT DEFAULT 0;
    DECLARE matching_connection_count INT DEFAULT 0;

    IF @activity_connection_to_stop IS NOT NULL THEN
        SELECT COUNT(*)
        INTO matching_connection_count
        FROM information_schema.PROCESSLIST
        WHERE ID=@activity_connection_to_stop;

        WHILE matching_connection_count>0 AND wait_attempt<900 DO
            DO SLEEP(1);
            SET wait_attempt=wait_attempt+1;
            SELECT COUNT(*)
            INTO matching_connection_count
            FROM information_schema.PROCESSLIST
            WHERE ID=@activity_connection_to_stop;
        END WHILE;
    END IF;

    SET @activity_resume_cleanup_wait_seconds=wait_attempt;
    SET @activity_resume_remaining_connection_count=matching_connection_count;
END$$
DELIMITER ;

CALL votes_mart._wait_activity_resume_connection_stop_v2();
DROP PROCEDURE votes_mart._wait_activity_resume_connection_stop_v2;

/* 같은 누산기를 읽거나 쓰는 다른 긴 문장이 남아 있으면 resume DDL을 막는다. */
SELECT COUNT(*)
INTO @activity_resume_other_accum_query_count
FROM information_schema.PROCESSLIST
WHERE ID<>CONNECTION_ID()
  AND COMMAND='Query'
  AND INFO LIKE '%votes_mart._wrk_user_activity_accum_v2%';

SELECT
    @activity_resume_cleanup_wait_seconds AS cleanup_wait_seconds,
    @activity_resume_remaining_connection_count AS remaining_stopped_connection,
    @activity_resume_other_accum_query_count AS other_accumulator_queries;

SET @activity_resume_cleanup_guard_sql = IF(
    @activity_resume_remaining_connection_count=0
    AND @activity_resume_other_accum_query_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_CONNECTION_OR_QUERY_REMAINS__'
);
PREPARE stmt_activity_resume_cleanup_guard
    FROM @activity_resume_cleanup_guard_sql;
EXECUTE stmt_activity_resume_cleanup_guard;
DEALLOCATE PREPARE stmt_activity_resume_cleanup_guard;

SELECT '[1/3] activity resume-after-5 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06f_user_activity_resume_after_stage5_v2.sql;
SELECT '[1/3] activity resume-after-5 완료' AS build_progress;

SELECT '[2/3] 누적 상태 tail 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/06e_user_cumulative_state_tail_v2.sql;
SELECT '[2/3] 누적 상태 tail 완료' AS build_progress;

SELECT '[3/3] 원천 커버리지·무결성 검사 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/07_qa_complete_coverage_v2.sql;
/* 07의 동적 QA guard가 실패하면 mysql batch가 여기 도달하지 않는다. */

/* 전체 QA PASS 뒤에만 rollback backup과 resume checkpoint를 정리한다. */
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after5_old;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_signup_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_value_day_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_ping_day_v2;
DROP TABLE votes_mart._wrk_user_activity_accum_v2;

SELECT '[3/3] Stage 3 resume-after-5 완료: 전체 QA 모두 PASS'
    AS build_progress;
