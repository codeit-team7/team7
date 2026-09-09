/* ============================================================================
   06-F recovery tail. 이미 커밋된 06-D 1~5단계 누산기를 보존하고 6~9단계만
   작은 PK side table에서 집계한 뒤 (user_id, activity_date) 순서로 병합한다.

   안전성/동치 원칙
   - 이 파일은 votes_mart._wrk_user_activity_accum_v2를 DROP/재생성하지 않는다.
   - 시작 시 누산기 스키마와 1~5단계 원천 총계를 대사한다.
   - 각 6~9단계는 원천 -> PK side table -> 총계 guard -> PK 순 병합 ->
     key/metric guard 순서다.
   - 병합은 값을 더하지 않고 원천 재계산값으로 교체하므로 재실행해도 중복되지
     않는다. 각 INSERT는 autocommit 단위라 중단 시 그 문장만 전부 rollback된다.
   - publish 직전 누산기 전체 tail 총계를 다시 한 번 side table과 대사한다.
   - publish는 새 테이블 검증 뒤 원자적 RENAME으로 수행한다. 이전 결과는 전체
     QA가 끝날 때까지 *_resume_after5_old 이름으로 남긴다.
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

SELECT '[activity resume 6/9] ping side table' AS build_progress;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_ping_day_v2;
CREATE TABLE votes_mart._wrk_user_activity_ping_day_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    db_ping_received_record_count BIGINT UNSIGNED NOT NULL,
    ping_current_read_record_count BIGINT UNSIGNED NOT NULL,
    ping_current_answered_record_count BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id, activity_date)
) ENGINE=InnoDB;

INSERT INTO votes_mart._wrk_user_activity_ping_day_v2 (
    user_id, activity_date,
    db_ping_received_record_count,
    ping_current_read_record_count,
    ping_current_answered_record_count
)
SELECT
    chosen_user_id AS user_id,
    DATE(vote_record_created_at) AS activity_date,
    COUNT(*) AS db_ping_received_record_count,
    COALESCE(SUM(ping_has_read_current=1),0) AS ping_current_read_record_count,
    COALESCE(SUM(ping_answer_status_current IN ('A','P')),0)
        AS ping_current_answered_record_count
FROM votes_mart.mart_vote_record_v2 FORCE INDEX (idx_vote_chosen_time)
WHERE chosen_user_id IS NOT NULL
  AND vote_record_created_at IS NOT NULL
GROUP BY chosen_user_id, DATE(vote_record_created_at)
ORDER BY chosen_user_id, DATE(vote_record_created_at);

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
FROM votes_mart._wrk_user_activity_ping_day_v2;

SET @ping_side_guard_sql = IF(
    @ping_side_group_count=@ping_source_group_count
    AND @ping_side_received_count=@ping_source_received_count
    AND @ping_side_read_count=@ping_source_read_count
    AND @ping_side_answered_count=@ping_source_answered_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_PING_SIDE_INVALID__'
);
PREPARE stmt_ping_side_guard FROM @ping_side_guard_sql;
EXECUTE stmt_ping_side_guard;
DEALLOCATE PREPARE stmt_ping_side_guard;

SELECT '[activity resume 6/9] ping PK-ordered merge' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    db_ping_received_record_count,
    ping_current_read_record_count,
    ping_current_answered_record_count
)
SELECT
    user_id, activity_date,
    db_ping_received_record_count,
    ping_current_read_record_count,
    ping_current_answered_record_count
FROM votes_mart._wrk_user_activity_ping_day_v2 FORCE INDEX (PRIMARY)
ORDER BY user_id, activity_date
ON DUPLICATE KEY UPDATE
    db_ping_received_record_count=VALUES(db_ping_received_record_count),
    ping_current_read_record_count=VALUES(ping_current_read_record_count),
    ping_current_answered_record_count=VALUES(ping_current_answered_record_count);

SELECT COUNT(*)
INTO @ping_merge_mismatch_count
FROM votes_mart._wrk_user_activity_ping_day_v2 AS s FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
  ON k.user_id=s.user_id AND k.activity_date=s.activity_date
WHERE k.user_id IS NULL
   OR NOT (k.db_ping_received_record_count <=> s.db_ping_received_record_count)
   OR NOT (k.ping_current_read_record_count <=> s.ping_current_read_record_count)
   OR NOT (k.ping_current_answered_record_count <=> s.ping_current_answered_record_count);

SET @ping_merge_guard_sql = IF(
    @ping_merge_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_PING_MERGE_INVALID__'
);
PREPARE stmt_ping_merge_guard FROM @ping_merge_guard_sql;
EXECUTE stmt_ping_merge_guard;
DEALLOCATE PREPARE stmt_ping_merge_guard;

SELECT '[activity resume 7/9] DB value side table' AS build_progress;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_value_day_v2;
CREATE TABLE votes_mart._wrk_user_activity_value_day_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    point_earn_event_count BIGINT UNSIGNED NOT NULL,
    point_spend_event_count BIGINT UNSIGNED NOT NULL,
    db_payment_success_count BIGINT UNSIGNED NOT NULL,
    db_payment_fail_count BIGINT UNSIGNED NOT NULL,
    promo_receipt_count BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id, activity_date)
) ENGINE=InnoDB;

INSERT INTO votes_mart._wrk_user_activity_value_day_v2 (
    user_id, activity_date,
    point_earn_event_count, point_spend_event_count,
    db_payment_success_count, db_payment_fail_count, promo_receipt_count
)
SELECT
    service_user_id AS user_id,
    DATE(event_at_raw) AS activity_date,
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_EARN'),0),
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_SPEND'),0),
    COALESCE(SUM(source_table='accounts_paymenthistory'),0),
    COALESCE(SUM(source_table='accounts_failpaymenthistory'),0),
    COALESCE(SUM(source_table='event_receipts'),0)
FROM votes_mart.mart_value_event_v2 FORCE INDEX (idx_value_user_time)
WHERE source_system='DB'
  AND service_user_id IS NOT NULL
  AND event_at_raw IS NOT NULL
GROUP BY service_user_id, DATE(event_at_raw)
ORDER BY service_user_id, DATE(event_at_raw);

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
FROM votes_mart._wrk_user_activity_value_day_v2;

SET @value_side_guard_sql = IF(
    @value_side_group_count=@value_source_group_count
    AND @value_side_earn_count=@value_source_earn_count
    AND @value_side_spend_count=@value_source_spend_count
    AND @value_side_payment_success_count=@value_source_payment_success_count
    AND @value_side_payment_fail_count=@value_source_payment_fail_count
    AND @value_side_promo_count=@value_source_promo_count,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_VALUE_SIDE_INVALID__'
);
PREPARE stmt_value_side_guard FROM @value_side_guard_sql;
EXECUTE stmt_value_side_guard;
DEALLOCATE PREPARE stmt_value_side_guard;

SELECT '[activity resume 7/9] DB value PK-ordered merge' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    point_earn_event_count, point_spend_event_count,
    db_payment_success_count, db_payment_fail_count, promo_receipt_count
)
SELECT
    user_id, activity_date,
    point_earn_event_count, point_spend_event_count,
    db_payment_success_count, db_payment_fail_count, promo_receipt_count
FROM votes_mart._wrk_user_activity_value_day_v2 FORCE INDEX (PRIMARY)
ORDER BY user_id, activity_date
ON DUPLICATE KEY UPDATE
    point_earn_event_count=VALUES(point_earn_event_count),
    point_spend_event_count=VALUES(point_spend_event_count),
    db_payment_success_count=VALUES(db_payment_success_count),
    db_payment_fail_count=VALUES(db_payment_fail_count),
    promo_receipt_count=VALUES(promo_receipt_count);

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

SET @value_merge_guard_sql = IF(
    @value_merge_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_VALUE_MERGE_INVALID__'
);
PREPARE stmt_value_merge_guard FROM @value_merge_guard_sql;
EXECUTE stmt_value_merge_guard;
DEALLOCATE PREPARE stmt_value_merge_guard;

SELECT '[activity resume 8/9] identified Hackle side table' AS build_progress;
DROP TABLE IF EXISTS votes_mart._wrk_user_activity_hackle_day_v2;
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
    hu.service_user_id AS user_id,
    DATE(h.event_datetime_raw) AS activity_date,
    COUNT(*) AS hackle_event_count,
    COUNT(DISTINCT hva.derived_visit_id_30m) AS hackle_visit_count,
    COALESCE(SUM(hek.attribute_value_raw='click_question_start'),0),
    COALESCE(SUM(hek.attribute_value_raw='complete_question'),0),
    COALESCE(SUM(hek.attribute_value_raw IN
        ('view_shop','click_purchase','complete_purchase')),0)
FROM votes_mart.fact_hackle_event_24d_v2 AS h
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
  ON hva.event_sk=h.event_sk
JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
  ON hs.session_sk=h.original_session_sk
JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hek
  ON hek.text_attribute_sk=h.event_key_attribute_sk
WHERE hu.service_user_id IS NOT NULL
  AND h.event_datetime_raw IS NOT NULL
GROUP BY hu.service_user_id, DATE(h.event_datetime_raw)
ORDER BY hu.service_user_id, DATE(h.event_datetime_raw);

/* side aggregate가 보존해야 할 원천 join grain/metric 총계를 독립 대사한다. */
SELECT
    COUNT(DISTINCT hu.service_user_id, DATE(h.event_datetime_raw)),
    COUNT(*),
    COUNT(DISTINCT hu.service_user_id, DATE(h.event_datetime_raw),
                   hva.derived_visit_id_30m),
    COALESCE(SUM(hek.attribute_value_raw='click_question_start'),0),
    COALESCE(SUM(hek.attribute_value_raw='complete_question'),0),
    COALESCE(SUM(hek.attribute_value_raw IN
        ('view_shop','click_purchase','complete_purchase')),0)
INTO
    @hackle_source_group_count,
    @hackle_source_event_count,
    @hackle_source_visit_count,
    @hackle_source_question_start_count,
    @hackle_source_question_complete_count,
    @hackle_source_shop_count
FROM votes_mart.fact_hackle_event_24d_v2 AS h
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
  ON hva.event_sk=h.event_sk
JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
  ON hs.session_sk=h.original_session_sk
JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hek
  ON hek.text_attribute_sk=h.event_key_attribute_sk
WHERE hu.service_user_id IS NOT NULL
  AND h.event_datetime_raw IS NOT NULL;

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
FROM votes_mart._wrk_user_activity_hackle_day_v2;

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

SELECT '[activity resume 8/9] identified Hackle PK-ordered merge' AS build_progress;
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

/* --------------------------------------------------------------------------
   10. final projection + guarded atomic publish
---------------------------------------------------------------------------- */
SELECT '[activity resume] final projection' AS build_progress;
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after5_new;

CREATE TABLE votes_mart.mart_user_activity_daily_v2_resume_after5_new (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    PRIMARY KEY (user_id, activity_date)
) AS
SELECT
    k.user_id,
    k.activity_date,
    DATEDIFF(k.activity_date, DATE(p.signup_at)) AS days_since_signup,
    COALESCE(k.signup_record_count,0) AS signup_record_count,
    COALESCE(k.attendance_raw_element_count,0) AS attendance_raw_element_count,
    COALESCE(k.attendance_record_count,0) AS attendance_record_count,
    COALESCE(k.friend_request_sent_count,0) AS friend_request_sent_count,
    COALESCE(k.friend_request_received_count,0) AS friend_request_received_count,
    COALESCE(k.friend_request_sent_final_a_count,0) AS friend_request_sent_final_a_count,
    COALESCE(k.friend_request_sent_final_p_count,0) AS friend_request_sent_final_p_count,
    COALESCE(k.friend_request_sent_final_r_count,0) AS friend_request_sent_final_r_count,
    COALESCE(k.db_question_set_created_count,0) AS db_question_set_created_count,
    COALESCE(k.db_vote_record_created_count,0) AS db_vote_record_created_count,
    COALESCE(k.db_ping_received_record_count,0) AS db_ping_received_record_count,
    COALESCE(k.ping_current_read_record_count,0) AS ping_current_read_record_count,
    COALESCE(k.ping_current_answered_record_count,0) AS ping_current_answered_record_count,
    COALESCE(k.point_earn_event_count,0) AS point_earn_event_count,
    COALESCE(k.point_spend_event_count,0) AS point_spend_event_count,
    COALESCE(k.db_payment_success_count,0) AS db_payment_success_count,
    COALESCE(k.db_payment_fail_count,0) AS db_payment_fail_count,
    COALESCE(k.promo_receipt_count,0) AS promo_receipt_count,
    COALESCE(k.hackle_event_count,0) AS hackle_event_count,
    COALESCE(k.hackle_visit_count,0) AS hackle_visit_count,
    COALESCE(k.hackle_question_start_count,0) AS hackle_question_start_count,
    COALESCE(k.hackle_question_complete_count,0) AS hackle_question_complete_count,
    COALESCE(k.hackle_shop_event_count,0) AS hackle_shop_event_count,
    c.signup_source_range_flag,
    c.friend_request_source_range_flag,
    c.attendance_source_range_flag,
    c.question_db_source_range_flag,
    c.value_db_source_range_flag,
    c.hackle_24d_source_range_flag,
    (
        COALESCE(k.attendance_raw_element_count,0)>0 OR
        COALESCE(k.friend_request_sent_count,0)>0 OR
        COALESCE(k.db_vote_record_created_count,0)>0 OR
        COALESCE(k.db_payment_success_count,0)>0 OR
        COALESCE(k.db_payment_fail_count,0)>0 OR
        COALESCE(k.hackle_event_count,0)>0
    ) AS user_initiated_activity_proxy_flag,
    (
        COALESCE(k.signup_record_count,0)+
        COALESCE(k.attendance_raw_element_count,0)+
        COALESCE(k.friend_request_sent_count,0)+
        COALESCE(k.friend_request_received_count,0)+
        COALESCE(k.db_question_set_created_count,0)+
        COALESCE(k.db_vote_record_created_count,0)+
        COALESCE(k.db_ping_received_record_count,0)+
        COALESCE(k.point_earn_event_count,0)+
        COALESCE(k.point_spend_event_count,0)+
        COALESCE(k.db_payment_success_count,0)+
        COALESCE(k.db_payment_fail_count,0)+
        COALESCE(k.promo_receipt_count,0)+
        COALESCE(k.hackle_event_count,0)
    )>0 AS any_record_observed_flag,
    (
        COALESCE(k.signup_record_count,0)+
        COALESCE(k.friend_request_received_count,0)+
        COALESCE(k.db_question_set_created_count,0)+
        COALESCE(k.db_ping_received_record_count,0)+
        COALESCE(k.point_earn_event_count,0)>0
        AND
        COALESCE(k.attendance_raw_element_count,0)+
        COALESCE(k.friend_request_sent_count,0)+
        COALESCE(k.db_vote_record_created_count,0)+
        COALESCE(k.db_payment_success_count,0)+
        COALESCE(k.db_payment_fail_count,0)+
        COALESCE(k.hackle_event_count,0)=0
    ) AS context_or_system_only_day_flag,
    (p.user_id IS NULL) AS user_dimension_missing_flag
FROM votes_mart._wrk_user_activity_accum_v2 AS k FORCE INDEX (PRIMARY)
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id=k.user_id
LEFT JOIN votes_mart.dim_source_observation_calendar_v2 AS c
  ON c.calendar_date=k.activity_date
ORDER BY k.user_id, k.activity_date;

ALTER TABLE votes_mart.mart_user_activity_daily_v2_resume_after5_new
    ADD INDEX idx_activity_day_user (activity_date, user_id),
    ADD INDEX idx_activity_proxy (user_initiated_activity_proxy_flag, activity_date),
    ALGORITHM=INPLACE,
    LOCK=NONE;

SELECT COUNT(*)
INTO @activity_new_row_count
FROM votes_mart.mart_user_activity_daily_v2_resume_after5_new;

SELECT COALESCE(SUM(
    NOT (n.signup_record_count <=> k.signup_record_count)
    OR NOT (n.attendance_raw_element_count <=> k.attendance_raw_element_count)
    OR NOT (n.attendance_record_count <=> k.attendance_record_count)
    OR NOT (n.friend_request_sent_count <=> k.friend_request_sent_count)
    OR NOT (n.friend_request_received_count <=> k.friend_request_received_count)
    OR NOT (n.friend_request_sent_final_a_count <=> k.friend_request_sent_final_a_count)
    OR NOT (n.friend_request_sent_final_p_count <=> k.friend_request_sent_final_p_count)
    OR NOT (n.friend_request_sent_final_r_count <=> k.friend_request_sent_final_r_count)
    OR NOT (n.db_question_set_created_count <=> k.db_question_set_created_count)
    OR NOT (n.db_vote_record_created_count <=> k.db_vote_record_created_count)
    OR NOT (n.db_ping_received_record_count <=> k.db_ping_received_record_count)
    OR NOT (n.ping_current_read_record_count <=> k.ping_current_read_record_count)
    OR NOT (n.ping_current_answered_record_count <=> k.ping_current_answered_record_count)
    OR NOT (n.point_earn_event_count <=> k.point_earn_event_count)
    OR NOT (n.point_spend_event_count <=> k.point_spend_event_count)
    OR NOT (n.db_payment_success_count <=> k.db_payment_success_count)
    OR NOT (n.db_payment_fail_count <=> k.db_payment_fail_count)
    OR NOT (n.promo_receipt_count <=> k.promo_receipt_count)
    OR NOT (n.hackle_event_count <=> k.hackle_event_count)
    OR NOT (n.hackle_visit_count <=> k.hackle_visit_count)
    OR NOT (n.hackle_question_start_count <=> k.hackle_question_start_count)
    OR NOT (n.hackle_question_complete_count <=> k.hackle_question_complete_count)
    OR NOT (n.hackle_shop_event_count <=> k.hackle_shop_event_count)
),0)
INTO @activity_metric_mismatch_count
FROM votes_mart.mart_user_activity_daily_v2_resume_after5_new AS n
JOIN votes_mart._wrk_user_activity_accum_v2 AS k
  ON k.user_id=n.user_id AND k.activity_date=n.activity_date;

SELECT
    @activity_final_accum_row_count AS accumulator_rows,
    @activity_new_row_count AS projected_rows,
    @activity_metric_mismatch_count AS metric_mismatch_rows;

SET @activity_publish_guard_sql = IF(
    @activity_final_accum_row_count=@activity_new_row_count
    AND @activity_metric_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RESUME_PROJECTION_INVALID__'
);
PREPARE stmt_activity_publish_guard FROM @activity_publish_guard_sql;
EXECUTE stmt_activity_publish_guard;
DEALLOCATE PREPARE stmt_activity_publish_guard;

/* 이전 publish backup은 새 결과 검증 후에만 교체한다. */
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after5_old;
SET @activity_target_exists = (
    SELECT COUNT(*)
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='mart_user_activity_daily_v2'
      AND TABLE_TYPE='BASE TABLE'
);
SET @activity_publish_sql = IF(
    @activity_target_exists=1,
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2 TO votes_mart.mart_user_activity_daily_v2_resume_after5_old, votes_mart.mart_user_activity_daily_v2_resume_after5_new TO votes_mart.mart_user_activity_daily_v2',
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2_resume_after5_new TO votes_mart.mart_user_activity_daily_v2'
);
PREPARE stmt_activity_publish FROM @activity_publish_sql;
EXECUTE stmt_activity_publish;
DEALLOCATE PREPARE stmt_activity_publish;

SELECT '[activity resume] publish complete; checkpoint retained through final QA'
    AS build_progress;
