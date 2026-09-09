/* ============================================================================
   06-G recovery tail. 06-F에서 이미 커밋된 1~7단계 누산기와 ping/value
   side table을 먼저 정확히 대사한 뒤, 8단계 Hackle만 event_sk 순차 경로로
   다시 만들고 9단계 및 최종 tail 대사를 수행한다.

   이 파일은 현재 실행 중인 연결을 종료하지 않는다. 08-G wrapper가 유일한
   stale 06-F stage-8 연결을 식별/종료하고 rollback 완료를 확인한 뒤 SOURCE한다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;
SET SESSION autocommit=1;

/* --------------------------------------------------------------------------
   0. resume checkpoint: table shape + committed stages 1~5 totals
---------------------------------------------------------------------------- */
SET @activity_accum_exists = (
    SELECT COUNT(*)
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='_wrk_user_activity_accum_v2'
      AND TABLE_TYPE='BASE TABLE'
);
SET @activity_accum_exists_guard_sql = IF(
    @activity_accum_exists=1,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_ACCUMULATOR_MISSING__'
);
PREPARE stmt_activity_accum_exists_guard FROM @activity_accum_exists_guard_sql;
EXECUTE stmt_activity_accum_exists_guard;
DEALLOCATE PREPARE stmt_activity_accum_exists_guard;

SELECT COUNT(*)
INTO @activity_accum_column_count
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='votes_mart'
  AND TABLE_NAME='_wrk_user_activity_accum_v2';

SELECT COUNT(*)
INTO @activity_accum_required_column_count
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='votes_mart'
  AND TABLE_NAME='_wrk_user_activity_accum_v2'
  AND COLUMN_NAME IN (
      'user_id','activity_date','signup_record_count',
      'attendance_raw_element_count','attendance_record_count',
      'friend_request_sent_count','friend_request_received_count',
      'friend_request_sent_final_a_count','friend_request_sent_final_p_count',
      'friend_request_sent_final_r_count','db_question_set_created_count',
      'db_vote_record_created_count','db_ping_received_record_count',
      'ping_current_read_record_count','ping_current_answered_record_count',
      'point_earn_event_count','point_spend_event_count',
      'db_payment_success_count','db_payment_fail_count','promo_receipt_count',
      'hackle_event_count','hackle_visit_count',
      'hackle_question_start_count','hackle_question_complete_count',
      'hackle_shop_event_count'
  );

SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
INTO @activity_accum_pk_signature
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA='votes_mart'
  AND TABLE_NAME='_wrk_user_activity_accum_v2'
  AND INDEX_NAME='PRIMARY';

SET @activity_accum_shape_guard_sql = IF(
    @activity_accum_column_count=25
    AND @activity_accum_required_column_count=25
    AND @activity_accum_pk_signature='user_id,activity_date',
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_ACCUMULATOR_SHAPE_INVALID__'
);
PREPARE stmt_activity_accum_shape_guard FROM @activity_accum_shape_guard_sql;
EXECUTE stmt_activity_accum_shape_guard;
DEALLOCATE PREPARE stmt_activity_accum_shape_guard;

/* 누산기는 한 번만 순차 스캔해 1~5단계의 모든 보존 metric을 읽는다. */
SELECT
    COUNT(*),
    COALESCE(SUM(attendance_raw_element_count),0),
    COALESCE(SUM(attendance_record_count),0),
    COALESCE(SUM(friend_request_sent_count),0),
    COALESCE(SUM(friend_request_received_count),0),
    COALESCE(SUM(friend_request_sent_final_a_count),0),
    COALESCE(SUM(friend_request_sent_final_p_count),0),
    COALESCE(SUM(friend_request_sent_final_r_count),0),
    COALESCE(SUM(db_question_set_created_count),0),
    COALESCE(SUM(db_vote_record_created_count),0)
INTO
    @activity_checkpoint_row_count,
    @accum_attendance_raw_count,
    @accum_attendance_record_count,
    @accum_friend_sent_count,
    @accum_friend_received_count,
    @accum_friend_sent_a_count,
    @accum_friend_sent_p_count,
    @accum_friend_sent_r_count,
    @accum_question_set_count,
    @accum_vote_actor_count
FROM votes_mart._wrk_user_activity_accum_v2 FORCE INDEX (PRIMARY);

SELECT
    COUNT(*),
    COUNT(DISTINCT user_id, attendance_date, attendance_record_id)
INTO
    @source_attendance_raw_count,
    @source_attendance_record_count
FROM votes_mart.bridge_attendance_day_v2
WHERE attendance_date IS NOT NULL;

SELECT
    COALESCE(SUM(send_user_id IS NOT NULL AND request_created_at_raw IS NOT NULL),0),
    COALESCE(SUM(receive_user_id IS NOT NULL AND request_created_at_raw IS NOT NULL),0),
    COALESCE(SUM(send_user_id IS NOT NULL AND request_created_at_raw IS NOT NULL
                 AND final_status_code='A'),0),
    COALESCE(SUM(send_user_id IS NOT NULL AND request_created_at_raw IS NOT NULL
                 AND final_status_code='P'),0),
    COALESCE(SUM(send_user_id IS NOT NULL AND request_created_at_raw IS NOT NULL
                 AND final_status_code='R'),0)
INTO
    @source_friend_sent_count,
    @source_friend_received_count,
    @source_friend_sent_a_count,
    @source_friend_sent_p_count,
    @source_friend_sent_r_count
FROM votes_mart.mart_friend_request_event_v2;

SELECT COUNT(DISTINCT question_set_id)
INTO @source_question_set_count
FROM votes_mart.mart_question_set_record_v2
WHERE question_set_owner_user_id IS NOT NULL
  AND question_set_created_at IS NOT NULL;

SELECT COUNT(*)
INTO @source_vote_actor_count
FROM votes_mart.mart_vote_record_v2
WHERE voter_user_id IS NOT NULL
  AND vote_record_created_at IS NOT NULL;

SELECT
    @activity_checkpoint_row_count AS checkpoint_rows,
    @accum_attendance_raw_count AS accum_attendance_raw,
    @source_attendance_raw_count AS source_attendance_raw,
    @accum_friend_sent_count AS accum_friend_sent,
    @source_friend_sent_count AS source_friend_sent,
    @accum_friend_received_count AS accum_friend_received,
    @source_friend_received_count AS source_friend_received,
    @accum_question_set_count AS accum_question_sets,
    @source_question_set_count AS source_question_sets,
    @accum_vote_actor_count AS accum_vote_actor,
    @source_vote_actor_count AS source_vote_actor;

SET @activity_stage1_5_guard_sql = IF(
    @activity_checkpoint_row_count>0
    AND @accum_attendance_raw_count=@source_attendance_raw_count
    AND @accum_attendance_record_count=@source_attendance_record_count
    AND @accum_friend_sent_count=@source_friend_sent_count
    AND @accum_friend_received_count=@source_friend_received_count
    AND @accum_friend_sent_a_count=@source_friend_sent_a_count
    AND @accum_friend_sent_p_count=@source_friend_sent_p_count
    AND @accum_friend_sent_r_count=@source_friend_sent_r_count
    AND @accum_question_set_count=@source_question_set_count
    AND @accum_vote_actor_count=@source_vote_actor_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_STAGE_1_5_CHECKPOINT_INVALID__'
);
PREPARE stmt_activity_stage1_5_guard FROM @activity_stage1_5_guard_sql;
EXECUTE stmt_activity_stage1_5_guard;
DEALLOCATE PREPARE stmt_activity_stage1_5_guard;

/* --------------------------------------------------------------------------
   1. committed stages 6~7: side/source/key/metric checkpoint
---------------------------------------------------------------------------- */
SET @activity_stage6_7_side_table_count = (
    SELECT COUNT(*)
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_TYPE='BASE TABLE'
      AND TABLE_NAME IN (
          '_wrk_user_activity_ping_day_v2',
          '_wrk_user_activity_value_day_v2'
      )
);
SET @activity_ping_required_column_count = (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='_wrk_user_activity_ping_day_v2'
      AND COLUMN_NAME IN (
          'user_id','activity_date','db_ping_received_record_count',
          'ping_current_read_record_count','ping_current_answered_record_count'
      )
);
SET @activity_value_required_column_count = (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='_wrk_user_activity_value_day_v2'
      AND COLUMN_NAME IN (
          'user_id','activity_date','point_earn_event_count',
          'point_spend_event_count','db_payment_success_count',
          'db_payment_fail_count','promo_receipt_count'
      )
);
SET @activity_ping_pk_signature = (
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='_wrk_user_activity_ping_day_v2'
      AND INDEX_NAME='PRIMARY'
);
SET @activity_value_pk_signature = (
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='_wrk_user_activity_value_day_v2'
      AND INDEX_NAME='PRIMARY'
);
SET @activity_stage6_7_shape_guard_sql = IF(
    @activity_stage6_7_side_table_count=2
    AND @activity_ping_required_column_count=5
    AND @activity_value_required_column_count=7
    AND @activity_ping_pk_signature='user_id,activity_date'
    AND @activity_value_pk_signature='user_id,activity_date',
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_STAGE_6_7_SIDE_SHAPE_INVALID__'
);
PREPARE stmt_activity_stage6_7_shape_guard
    FROM @activity_stage6_7_shape_guard_sql;
EXECUTE stmt_activity_stage6_7_shape_guard;
DEALLOCATE PREPARE stmt_activity_stage6_7_shape_guard;

SELECT '[activity resume-after-7] validate ping source and side' AS build_progress;
SELECT
    COUNT(*),
    COUNT(DISTINCT chosen_user_id, DATE(vote_record_created_at)),
    COALESCE(SUM(ping_has_read_current=1),0),
    COALESCE(SUM(ping_answer_status_current IN ('A','P')),0)
INTO
    @ping_source_received_count,
    @ping_source_group_count,
    @ping_source_read_count,
    @ping_source_answered_count
FROM votes_mart.mart_vote_record_v2
WHERE chosen_user_id IS NOT NULL
  AND vote_record_created_at IS NOT NULL;

SELECT
    COUNT(*),
    COALESCE(SUM(db_ping_received_record_count),0),
    COALESCE(SUM(ping_current_read_record_count),0),
    COALESCE(SUM(ping_current_answered_record_count),0)
INTO
    @ping_side_group_count,
    @ping_side_received_count,
    @ping_side_read_count,
    @ping_side_answered_count
FROM votes_mart._wrk_user_activity_ping_day_v2 FORCE INDEX (PRIMARY);

SELECT COUNT(*)
INTO @ping_merge_mismatch_count
FROM votes_mart._wrk_user_activity_ping_day_v2 AS s FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
  ON k.user_id=s.user_id AND k.activity_date=s.activity_date
WHERE k.user_id IS NULL
   OR NOT (k.db_ping_received_record_count <=> s.db_ping_received_record_count)
   OR NOT (k.ping_current_read_record_count <=> s.ping_current_read_record_count)
   OR NOT (k.ping_current_answered_record_count <=> s.ping_current_answered_record_count);

SELECT '[activity resume-after-7] validate DB value source and side' AS build_progress;
SELECT
    COUNT(DISTINCT service_user_id, DATE(event_at_raw)),
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_EARN'),0),
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_SPEND'),0),
    COALESCE(SUM(source_table='accounts_paymenthistory'),0),
    COALESCE(SUM(source_table='accounts_failpaymenthistory'),0),
    COALESCE(SUM(source_table='event_receipts'),0)
INTO
    @value_source_group_count,
    @value_source_earn_count,
    @value_source_spend_count,
    @value_source_payment_success_count,
    @value_source_payment_fail_count,
    @value_source_promo_count
FROM votes_mart.mart_value_event_v2
WHERE source_system='DB'
  AND service_user_id IS NOT NULL
  AND event_at_raw IS NOT NULL;

SELECT
    COUNT(*),
    COALESCE(SUM(point_earn_event_count),0),
    COALESCE(SUM(point_spend_event_count),0),
    COALESCE(SUM(db_payment_success_count),0),
    COALESCE(SUM(db_payment_fail_count),0),
    COALESCE(SUM(promo_receipt_count),0)
INTO
    @value_side_group_count,
    @value_side_earn_count,
    @value_side_spend_count,
    @value_side_payment_success_count,
    @value_side_payment_fail_count,
    @value_side_promo_count
FROM votes_mart._wrk_user_activity_value_day_v2 FORCE INDEX (PRIMARY);

SELECT COUNT(*)
INTO @value_merge_mismatch_count
FROM votes_mart._wrk_user_activity_value_day_v2 AS s FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
  ON k.user_id=s.user_id AND k.activity_date=s.activity_date
WHERE k.user_id IS NULL
   OR NOT (k.point_earn_event_count <=> s.point_earn_event_count)
   OR NOT (k.point_spend_event_count <=> s.point_spend_event_count)
   OR NOT (k.db_payment_success_count <=> s.db_payment_success_count)
   OR NOT (k.db_payment_fail_count <=> s.db_payment_fail_count)
   OR NOT (k.promo_receipt_count <=> s.promo_receipt_count);

/* 한 번의 누산기 PK 순차 스캔으로 6~7 전역 합계를 확인한다. */
SELECT
    COALESCE(SUM(db_ping_received_record_count),0),
    COALESCE(SUM(ping_current_read_record_count),0),
    COALESCE(SUM(ping_current_answered_record_count),0),
    COALESCE(SUM(point_earn_event_count),0),
    COALESCE(SUM(point_spend_event_count),0),
    COALESCE(SUM(db_payment_success_count),0),
    COALESCE(SUM(db_payment_fail_count),0),
    COALESCE(SUM(promo_receipt_count),0)
INTO
    @accum_ping_received_count,
    @accum_ping_read_count,
    @accum_ping_answered_count,
    @accum_value_earn_count,
    @accum_value_spend_count,
    @accum_value_payment_success_count,
    @accum_value_payment_fail_count,
    @accum_value_promo_count
FROM votes_mart._wrk_user_activity_accum_v2 FORCE INDEX (PRIMARY);

SET @activity_stage6_7_guard_sql = IF(
    @ping_source_group_count=@ping_side_group_count
    AND @ping_source_received_count=@ping_side_received_count
    AND @ping_source_read_count=@ping_side_read_count
    AND @ping_source_answered_count=@ping_side_answered_count
    AND @ping_merge_mismatch_count=0
    AND @accum_ping_received_count=@ping_side_received_count
    AND @accum_ping_read_count=@ping_side_read_count
    AND @accum_ping_answered_count=@ping_side_answered_count
    AND @value_source_group_count=@value_side_group_count
    AND @value_source_earn_count=@value_side_earn_count
    AND @value_source_spend_count=@value_side_spend_count
    AND @value_source_payment_success_count=@value_side_payment_success_count
    AND @value_source_payment_fail_count=@value_side_payment_fail_count
    AND @value_source_promo_count=@value_side_promo_count
    AND @value_merge_mismatch_count=0
    AND @accum_value_earn_count=@value_side_earn_count
    AND @accum_value_spend_count=@value_side_spend_count
    AND @accum_value_payment_success_count=@value_side_payment_success_count
    AND @accum_value_payment_fail_count=@value_side_payment_fail_count
    AND @accum_value_promo_count=@value_side_promo_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_STAGE_6_7_CHECKPOINT_INVALID__'
);
PREPARE stmt_activity_stage6_7_guard FROM @activity_stage6_7_guard_sql;
EXECUTE stmt_activity_stage6_7_guard;
DEALLOCATE PREPARE stmt_activity_stage6_7_guard;

/* --------------------------------------------------------------------------
   2. stage 8: event_sk-sequential identified Hackle fallback
---------------------------------------------------------------------------- */
SELECT '[activity resume 8/9] identified session map' AS build_progress;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_identified_event_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_session_map_v2;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_day_v2;

CREATE TABLE votes_mart._wrk_user_activity_hackle_session_map_v2
ENGINE=InnoDB
AS
SELECT
    hs.session_sk,
    hu.service_user_id
FROM votes_mart.dim_hackle_session_resolved_v2 AS hs
JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
WHERE hu.service_user_id IS NOT NULL
  AND 1=0;

ALTER TABLE votes_mart._wrk_user_activity_hackle_session_map_v2
    ADD PRIMARY KEY (session_sk);

INSERT INTO votes_mart._wrk_user_activity_hackle_session_map_v2 (
    session_sk, service_user_id
)
SELECT
    hs.session_sk,
    hu.service_user_id
FROM votes_mart.dim_hackle_session_resolved_v2 AS hs FORCE INDEX (PRIMARY)
STRAIGHT_JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu FORCE INDEX (PRIMARY)
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
WHERE hu.service_user_id IS NOT NULL
ORDER BY hs.session_sk;
SET @hackle_session_map_insert_count=ROW_COUNT();

SELECT COUNT(*)
INTO @hackle_session_map_row_count
FROM votes_mart._wrk_user_activity_hackle_session_map_v2 FORCE INDEX (PRIMARY);

SET @hackle_session_map_guard_sql = IF(
    @hackle_session_map_insert_count=@hackle_session_map_row_count
    AND @hackle_session_map_row_count>0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_HACKLE_SESSION_MAP_INVALID__'
);
PREPARE stmt_hackle_session_map_guard FROM @hackle_session_map_guard_sql;
EXECUTE stmt_hackle_session_map_guard;
DEALLOCATE PREPARE stmt_hackle_session_map_guard;

SELECT '[activity resume 8/9] event_sk-sequential identified event snapshot'
    AS build_progress;
CREATE TABLE votes_mart._wrk_user_activity_hackle_identified_event_v2
ENGINE=InnoDB
AS
SELECT
    h.event_sk,
    im.service_user_id AS user_id,
    DATE(h.event_datetime_raw) AS activity_date,
    hva.derived_visit_id_30m,
    CAST(hek.attribute_value_raw='click_question_start' AS UNSIGNED)
        AS is_question_start,
    CAST(hek.attribute_value_raw='complete_question' AS UNSIGNED)
        AS is_question_complete,
    CAST(hek.attribute_value_raw IN
        ('view_shop','click_purchase','complete_purchase') AS UNSIGNED)
        AS is_shop_event
FROM votes_mart.fact_hackle_event_24d_v2 AS h
JOIN votes_mart._wrk_user_activity_hackle_session_map_v2 AS im
  ON im.session_sk=h.original_session_sk
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
  ON hva.event_sk=h.event_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hek
  ON hek.text_attribute_sk=h.event_key_attribute_sk
WHERE h.event_datetime_raw IS NOT NULL
  AND 1=0;

ALTER TABLE votes_mart._wrk_user_activity_hackle_identified_event_v2
    ADD PRIMARY KEY (event_sk);

INSERT INTO votes_mart._wrk_user_activity_hackle_identified_event_v2 (
    event_sk, user_id, activity_date, derived_visit_id_30m,
    is_question_start, is_question_complete, is_shop_event
)
SELECT
    h.event_sk,
    im.service_user_id AS user_id,
    DATE(h.event_datetime_raw) AS activity_date,
    hva.derived_visit_id_30m,
    CAST(hek.attribute_value_raw='click_question_start' AS UNSIGNED),
    CAST(hek.attribute_value_raw='complete_question' AS UNSIGNED),
    CAST(hek.attribute_value_raw IN
        ('view_shop','click_purchase','complete_purchase') AS UNSIGNED)
FROM votes_mart.fact_hackle_event_24d_v2 AS h FORCE INDEX (PRIMARY)
STRAIGHT_JOIN votes_mart._wrk_user_activity_hackle_session_map_v2 AS im
    FORCE INDEX (PRIMARY)
  ON im.session_sk=h.original_session_sk
STRAIGHT_JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
    FORCE INDEX (PRIMARY)
  ON hva.event_sk=h.event_sk
STRAIGHT_JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hek
    FORCE INDEX (PRIMARY)
  ON hek.text_attribute_sk=h.event_key_attribute_sk
WHERE h.event_datetime_raw IS NOT NULL
ORDER BY h.event_sk;
SET @hackle_identified_insert_count=ROW_COUNT();

SELECT COUNT(*)
INTO @hackle_identified_row_count
FROM votes_mart._wrk_user_activity_hackle_identified_event_v2 FORCE INDEX (PRIMARY);

SET @hackle_identified_guard_sql = IF(
    @hackle_identified_insert_count=@hackle_identified_row_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_HACKLE_IDENTIFIED_EVENT_INVALID__'
);
PREPARE stmt_hackle_identified_guard FROM @hackle_identified_guard_sql;
EXECUTE stmt_hackle_identified_guard;
DEALLOCATE PREPARE stmt_hackle_identified_guard;

SELECT '[activity resume 8/9] narrow Hackle daily aggregate' AS build_progress;
CREATE TABLE votes_mart._wrk_user_activity_hackle_day_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    hackle_event_count BIGINT UNSIGNED NOT NULL,
    hackle_visit_count BIGINT UNSIGNED NOT NULL,
    hackle_question_start_count BIGINT UNSIGNED NOT NULL,
    hackle_question_complete_count BIGINT UNSIGNED NOT NULL,
    hackle_shop_event_count BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id, activity_date)
) ENGINE=InnoDB;

INSERT INTO votes_mart._wrk_user_activity_hackle_day_v2 (
    user_id, activity_date,
    hackle_event_count, hackle_visit_count,
    hackle_question_start_count, hackle_question_complete_count,
    hackle_shop_event_count
)
SELECT
    user_id,
    activity_date,
    COUNT(*),
    COUNT(DISTINCT derived_visit_id_30m),
    COALESCE(SUM(is_question_start),0),
    COALESCE(SUM(is_question_complete),0),
    COALESCE(SUM(is_shop_event),0)
FROM votes_mart._wrk_user_activity_hackle_identified_event_v2
GROUP BY user_id, activity_date
ORDER BY user_id, activity_date;

SELECT
    COUNT(DISTINCT user_id, activity_date),
    COUNT(*),
    COUNT(DISTINCT user_id, activity_date, derived_visit_id_30m),
    COALESCE(SUM(is_question_start),0),
    COALESCE(SUM(is_question_complete),0),
    COALESCE(SUM(is_shop_event),0)
INTO
    @hackle_source_group_count,
    @hackle_source_event_count,
    @hackle_source_visit_count,
    @hackle_source_question_start_count,
    @hackle_source_question_complete_count,
    @hackle_source_shop_count
FROM votes_mart._wrk_user_activity_hackle_identified_event_v2;

SELECT
    COUNT(*),
    COALESCE(SUM(hackle_event_count),0),
    COALESCE(SUM(hackle_visit_count),0),
    COALESCE(SUM(hackle_question_start_count),0),
    COALESCE(SUM(hackle_question_complete_count),0),
    COALESCE(SUM(hackle_shop_event_count),0)
INTO
    @hackle_side_group_count,
    @hackle_side_event_count,
    @hackle_side_visit_count,
    @hackle_side_question_start_count,
    @hackle_side_question_complete_count,
    @hackle_side_shop_count
FROM votes_mart._wrk_user_activity_hackle_day_v2 FORCE INDEX (PRIMARY);

SET @hackle_side_guard_sql = IF(
    @hackle_side_group_count=@hackle_source_group_count
    AND @hackle_side_event_count=@hackle_source_event_count
    AND @hackle_side_visit_count=@hackle_source_visit_count
    AND @hackle_side_question_start_count=@hackle_source_question_start_count
    AND @hackle_side_question_complete_count=@hackle_source_question_complete_count
    AND @hackle_side_shop_count=@hackle_source_shop_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_HACKLE_SIDE_INVALID__'
);
PREPARE stmt_hackle_side_guard FROM @hackle_side_guard_sql;
EXECUTE stmt_hackle_side_guard;
DEALLOCATE PREPARE stmt_hackle_side_guard;

SELECT '[activity resume 8/9] Hackle PK-ordered merge' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    hackle_event_count, hackle_visit_count,
    hackle_question_start_count, hackle_question_complete_count,
    hackle_shop_event_count
)
SELECT
    user_id, activity_date,
    hackle_event_count, hackle_visit_count,
    hackle_question_start_count, hackle_question_complete_count,
    hackle_shop_event_count
FROM votes_mart._wrk_user_activity_hackle_day_v2 FORCE INDEX (PRIMARY)
ORDER BY user_id, activity_date
ON DUPLICATE KEY UPDATE
    hackle_event_count=VALUES(hackle_event_count),
    hackle_visit_count=VALUES(hackle_visit_count),
    hackle_question_start_count=VALUES(hackle_question_start_count),
    hackle_question_complete_count=VALUES(hackle_question_complete_count),
    hackle_shop_event_count=VALUES(hackle_shop_event_count);

SELECT COUNT(*)
INTO @hackle_merge_mismatch_count
FROM votes_mart._wrk_user_activity_hackle_day_v2 AS s FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
  ON k.user_id=s.user_id AND k.activity_date=s.activity_date
WHERE k.user_id IS NULL
   OR NOT (k.hackle_event_count <=> s.hackle_event_count)
   OR NOT (k.hackle_visit_count <=> s.hackle_visit_count)
   OR NOT (k.hackle_question_start_count <=> s.hackle_question_start_count)
   OR NOT (k.hackle_question_complete_count <=> s.hackle_question_complete_count)
   OR NOT (k.hackle_shop_event_count <=> s.hackle_shop_event_count);

SET @hackle_merge_guard_sql = IF(
    @hackle_merge_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_HACKLE_MERGE_INVALID__'
);
PREPARE stmt_hackle_merge_guard FROM @hackle_merge_guard_sql;
EXECUTE stmt_hackle_merge_guard;
DEALLOCATE PREPARE stmt_hackle_merge_guard;

/* stage 9 + final accumulator totals are byte-for-byte the proven 06-F tail. */
SELECT '[activity resume 9/9] signup side table' AS build_progress;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_signup_day_v2;
CREATE TABLE votes_mart._wrk_user_activity_signup_day_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    signup_record_count BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id, activity_date)
) ENGINE=InnoDB;

INSERT INTO votes_mart._wrk_user_activity_signup_day_v2 (
    user_id, activity_date, signup_record_count
)
SELECT user_id, DATE(signup_at), 1
FROM votes_mart.mart_user_acquisition_profile_v2 FORCE INDEX (PRIMARY)
WHERE signup_at IS NOT NULL
ORDER BY user_id, DATE(signup_at);

SELECT COUNT(*)
INTO @signup_source_count
FROM votes_mart.mart_user_acquisition_profile_v2
WHERE signup_at IS NOT NULL;

SELECT COUNT(*), COALESCE(SUM(signup_record_count),0)
INTO @signup_side_group_count, @signup_side_count
FROM votes_mart._wrk_user_activity_signup_day_v2;

SET @signup_side_guard_sql = IF(
    @signup_side_group_count=@signup_source_count
    AND @signup_side_count=@signup_source_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_SIGNUP_SIDE_INVALID__'
);
PREPARE stmt_signup_side_guard FROM @signup_side_guard_sql;
EXECUTE stmt_signup_side_guard;
DEALLOCATE PREPARE stmt_signup_side_guard;

SELECT '[activity resume 9/9] signup PK-ordered merge' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date, signup_record_count
)
SELECT user_id, activity_date, signup_record_count
FROM votes_mart._wrk_user_activity_signup_day_v2 FORCE INDEX (PRIMARY)
ORDER BY user_id, activity_date
ON DUPLICATE KEY UPDATE
    signup_record_count=VALUES(signup_record_count);

SELECT COUNT(*)
INTO @signup_merge_mismatch_count
FROM votes_mart._wrk_user_activity_signup_day_v2 AS s FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
  ON k.user_id=s.user_id AND k.activity_date=s.activity_date
WHERE k.user_id IS NULL
   OR NOT (k.signup_record_count <=> s.signup_record_count);

SET @signup_merge_guard_sql = IF(
    @signup_merge_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_SIGNUP_MERGE_INVALID__'
);
PREPARE stmt_signup_merge_guard FROM @signup_merge_guard_sql;
EXECUTE stmt_signup_merge_guard;
DEALLOCATE PREPARE stmt_signup_merge_guard;

/* tail 전 단계의 전역 합계를 누산기 한 번의 순차 스캔으로 최종 대사한다. */
SELECT
    COUNT(*),
    COALESCE(SUM(signup_record_count),0),
    COALESCE(SUM(db_ping_received_record_count),0),
    COALESCE(SUM(ping_current_read_record_count),0),
    COALESCE(SUM(ping_current_answered_record_count),0),
    COALESCE(SUM(point_earn_event_count),0),
    COALESCE(SUM(point_spend_event_count),0),
    COALESCE(SUM(db_payment_success_count),0),
    COALESCE(SUM(db_payment_fail_count),0),
    COALESCE(SUM(promo_receipt_count),0),
    COALESCE(SUM(hackle_event_count),0),
    COALESCE(SUM(hackle_visit_count),0),
    COALESCE(SUM(hackle_question_start_count),0),
    COALESCE(SUM(hackle_question_complete_count),0),
    COALESCE(SUM(hackle_shop_event_count),0)
INTO
    @activity_final_accum_row_count,
    @activity_final_signup_count,
    @activity_final_ping_received_count,
    @activity_final_ping_read_count,
    @activity_final_ping_answered_count,
    @activity_final_earn_count,
    @activity_final_spend_count,
    @activity_final_payment_success_count,
    @activity_final_payment_fail_count,
    @activity_final_promo_count,
    @activity_final_hackle_event_count,
    @activity_final_hackle_visit_count,
    @activity_final_hackle_question_start_count,
    @activity_final_hackle_question_complete_count,
    @activity_final_hackle_shop_count
FROM votes_mart._wrk_user_activity_accum_v2 FORCE INDEX (PRIMARY);

SET @activity_tail_totals_guard_sql = IF(
    @activity_final_accum_row_count>=@activity_checkpoint_row_count
    AND @activity_final_signup_count=@signup_side_count
    AND @activity_final_ping_received_count=@ping_side_received_count
    AND @activity_final_ping_read_count=@ping_side_read_count
    AND @activity_final_ping_answered_count=@ping_side_answered_count
    AND @activity_final_earn_count=@value_side_earn_count
    AND @activity_final_spend_count=@value_side_spend_count
    AND @activity_final_payment_success_count=@value_side_payment_success_count
    AND @activity_final_payment_fail_count=@value_side_payment_fail_count
    AND @activity_final_promo_count=@value_side_promo_count
    AND @activity_final_hackle_event_count=@hackle_side_event_count
    AND @activity_final_hackle_visit_count=@hackle_side_visit_count
    AND @activity_final_hackle_question_start_count=@hackle_side_question_start_count
    AND @activity_final_hackle_question_complete_count=@hackle_side_question_complete_count
    AND @activity_final_hackle_shop_count=@hackle_side_shop_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_TAIL_TOTALS_INVALID__'
);
PREPARE stmt_activity_tail_totals_guard FROM @activity_tail_totals_guard_sql;
EXECUTE stmt_activity_tail_totals_guard;
DEALLOCATE PREPARE stmt_activity_tail_totals_guard;

