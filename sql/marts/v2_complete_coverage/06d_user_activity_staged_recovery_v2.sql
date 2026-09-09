/* ============================================================================
   06-D recovery. 사용자×날짜 sparse activity를 단일 거대 CTE 대신
   clustered-PK 누산기에 원천별로 독립 적재한다.

   동치 근거
   - 누산기 PK (user_id, activity_date)는 기존 all_keys UNION의 set grain이다.
   - 각 원천은 자신이 소유한 metric 컬럼만 대입한다.
   - friend sender/receiver는 서로 다른 컬럼만 대입하므로 기존 UNION ALL 후
     GROUP BY와 같다.
   - ON DUPLICATE KEY UPDATE는 값을 더하지 않고 재계산값으로 교체하므로
     같은 원천 단계를 다시 실행해도 이중 집계되지 않는다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

DROP TABLE IF EXISTS votes_mart._wrk_user_activity_accum_v2;

CREATE TABLE votes_mart._wrk_user_activity_accum_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    signup_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    attendance_raw_element_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    attendance_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    friend_request_sent_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    friend_request_received_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    friend_request_sent_final_a_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    friend_request_sent_final_p_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    friend_request_sent_final_r_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    db_question_set_created_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    db_vote_record_created_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    db_ping_received_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ping_current_read_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ping_current_answered_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    point_earn_event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    point_spend_event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    db_payment_success_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    db_payment_fail_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    promo_receipt_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    hackle_event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    hackle_visit_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    hackle_question_start_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    hackle_question_complete_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    hackle_shop_event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, activity_date)
) ENGINE=InnoDB;

SELECT '[activity recovery 1/9] attendance' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    attendance_raw_element_count, attendance_record_count
)
SELECT
    user_id,
    attendance_date AS activity_date,
    COUNT(*) AS attendance_raw_element_count,
    COUNT(DISTINCT attendance_record_id) AS attendance_record_count
FROM votes_mart.bridge_attendance_day_v2
WHERE attendance_date IS NOT NULL
GROUP BY user_id, attendance_date
ON DUPLICATE KEY UPDATE
    attendance_raw_element_count=VALUES(attendance_raw_element_count),
    attendance_record_count=VALUES(attendance_record_count);

SELECT '[activity recovery 2/9] friend sender' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    friend_request_sent_count,
    friend_request_sent_final_a_count,
    friend_request_sent_final_p_count,
    friend_request_sent_final_r_count
)
SELECT
    send_user_id AS user_id,
    DATE(request_created_at_raw) AS activity_date,
    COUNT(*) AS friend_request_sent_count,
    COALESCE(SUM(final_status_code='A'),0) AS friend_request_sent_final_a_count,
    COALESCE(SUM(final_status_code='P'),0) AS friend_request_sent_final_p_count,
    COALESCE(SUM(final_status_code='R'),0) AS friend_request_sent_final_r_count
FROM votes_mart.mart_friend_request_event_v2
WHERE send_user_id IS NOT NULL
  AND request_created_at_raw IS NOT NULL
GROUP BY send_user_id, DATE(request_created_at_raw)
ON DUPLICATE KEY UPDATE
    friend_request_sent_count=VALUES(friend_request_sent_count),
    friend_request_sent_final_a_count=VALUES(friend_request_sent_final_a_count),
    friend_request_sent_final_p_count=VALUES(friend_request_sent_final_p_count),
    friend_request_sent_final_r_count=VALUES(friend_request_sent_final_r_count);

SELECT '[activity recovery 3/9] friend receiver' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date, friend_request_received_count
)
SELECT
    receive_user_id AS user_id,
    DATE(request_created_at_raw) AS activity_date,
    COUNT(*) AS friend_request_received_count
FROM votes_mart.mart_friend_request_event_v2
WHERE receive_user_id IS NOT NULL
  AND request_created_at_raw IS NOT NULL
GROUP BY receive_user_id, DATE(request_created_at_raw)
ON DUPLICATE KEY UPDATE
    friend_request_received_count=VALUES(friend_request_received_count);

SELECT '[activity recovery 4/9] question set' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date, db_question_set_created_count
)
SELECT
    question_set_owner_user_id AS user_id,
    DATE(question_set_created_at) AS activity_date,
    COUNT(DISTINCT question_set_id) AS db_question_set_created_count
FROM votes_mart.mart_question_set_record_v2
WHERE question_set_owner_user_id IS NOT NULL
  AND question_set_created_at IS NOT NULL
GROUP BY question_set_owner_user_id, DATE(question_set_created_at)
ON DUPLICATE KEY UPDATE
    db_question_set_created_count=VALUES(db_question_set_created_count);

SELECT '[activity recovery 5/9] vote actor' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date, db_vote_record_created_count
)
SELECT
    voter_user_id AS user_id,
    DATE(vote_record_created_at) AS activity_date,
    COUNT(*) AS db_vote_record_created_count
FROM votes_mart.mart_vote_record_v2
WHERE voter_user_id IS NOT NULL
  AND vote_record_created_at IS NOT NULL
GROUP BY voter_user_id, DATE(vote_record_created_at)
ON DUPLICATE KEY UPDATE
    db_vote_record_created_count=VALUES(db_vote_record_created_count);

SELECT '[activity recovery 6/9] ping receiver snapshot' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
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
    COALESCE(SUM(ping_answer_status_current IN ('A','P')),0) AS ping_current_answered_record_count
FROM votes_mart.mart_vote_record_v2
WHERE chosen_user_id IS NOT NULL
  AND vote_record_created_at IS NOT NULL
GROUP BY chosen_user_id, DATE(vote_record_created_at)
ON DUPLICATE KEY UPDATE
    db_ping_received_record_count=VALUES(db_ping_received_record_count),
    ping_current_read_record_count=VALUES(ping_current_read_record_count),
    ping_current_answered_record_count=VALUES(ping_current_answered_record_count);

SELECT '[activity recovery 7/9] DB value event' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date,
    point_earn_event_count, point_spend_event_count,
    db_payment_success_count, db_payment_fail_count, promo_receipt_count
)
SELECT
    service_user_id AS user_id,
    DATE(event_at_raw) AS activity_date,
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_EARN'),0) AS point_earn_event_count,
    COALESCE(SUM(source_table='accounts_pointhistory' AND event_type='POINT_SPEND'),0) AS point_spend_event_count,
    COALESCE(SUM(source_table='accounts_paymenthistory'),0) AS db_payment_success_count,
    COALESCE(SUM(source_table='accounts_failpaymenthistory'),0) AS db_payment_fail_count,
    COALESCE(SUM(source_table='event_receipts'),0) AS promo_receipt_count
FROM votes_mart.mart_value_event_v2
WHERE source_system='DB'
  AND service_user_id IS NOT NULL
  AND event_at_raw IS NOT NULL
GROUP BY service_user_id, DATE(event_at_raw)
ON DUPLICATE KEY UPDATE
    point_earn_event_count=VALUES(point_earn_event_count),
    point_spend_event_count=VALUES(point_spend_event_count),
    db_payment_success_count=VALUES(db_payment_success_count),
    db_payment_fail_count=VALUES(db_payment_fail_count),
    promo_receipt_count=VALUES(promo_receipt_count);

SELECT '[activity recovery 8/9] identified Hackle event' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
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
    COALESCE(SUM(hek.attribute_value_raw='click_question_start'),0) AS hackle_question_start_count,
    COALESCE(SUM(hek.attribute_value_raw='complete_question'),0) AS hackle_question_complete_count,
    COALESCE(SUM(hek.attribute_value_raw IN ('view_shop','click_purchase','complete_purchase')),0) AS hackle_shop_event_count
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
ON DUPLICATE KEY UPDATE
    hackle_event_count=VALUES(hackle_event_count),
    hackle_visit_count=VALUES(hackle_visit_count),
    hackle_question_start_count=VALUES(hackle_question_start_count),
    hackle_question_complete_count=VALUES(hackle_question_complete_count),
    hackle_shop_event_count=VALUES(hackle_shop_event_count);

SELECT '[activity recovery 9/9] signup and final projection' AS build_progress;
INSERT INTO votes_mart._wrk_user_activity_accum_v2 (
    user_id, activity_date, signup_record_count
)
SELECT user_id, DATE(signup_at) AS activity_date, 1 AS signup_record_count
FROM votes_mart.mart_user_acquisition_profile_v2
WHERE signup_at IS NOT NULL
ON DUPLICATE KEY UPDATE
    signup_record_count=VALUES(signup_record_count);

DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_recover_new;

CREATE TABLE votes_mart.mart_user_activity_daily_v2_recover_new (
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
FROM votes_mart._wrk_user_activity_accum_v2 AS k
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id=k.user_id
LEFT JOIN votes_mart.dim_source_observation_calendar_v2 AS c
  ON c.calendar_date=k.activity_date
ORDER BY k.user_id, k.activity_date;

ALTER TABLE votes_mart.mart_user_activity_daily_v2_recover_new
    ADD INDEX idx_activity_day_user (activity_date, user_id),
    ADD INDEX idx_activity_proxy (user_initiated_activity_proxy_flag, activity_date),
    ALGORITHM=INPLACE,
    LOCK=NONE;

/* 새 결과가 누산기의 모든 key/metric을 보존했을 때만 publish한다. */
SET @activity_accum_row_count = (
    SELECT COUNT(*) FROM votes_mart._wrk_user_activity_accum_v2
);
SET @activity_new_row_count = (
    SELECT COUNT(*) FROM votes_mart.mart_user_activity_daily_v2_recover_new
);
SET @activity_metric_mismatch_count = (
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
    FROM votes_mart.mart_user_activity_daily_v2_recover_new AS n
    JOIN votes_mart._wrk_user_activity_accum_v2 AS k
      ON k.user_id=n.user_id AND k.activity_date=n.activity_date
);

SELECT
    @activity_accum_row_count AS accumulator_rows,
    @activity_new_row_count AS projected_rows,
    @activity_metric_mismatch_count AS metric_mismatch_rows;

SET @activity_publish_guard_sql = IF(
    @activity_accum_row_count=@activity_new_row_count
    AND @activity_metric_mismatch_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__ACTIVITY_RECOVERY_VALIDATION_FAILED__'
);
PREPARE stmt_activity_publish_guard FROM @activity_publish_guard_sql;
EXECUTE stmt_activity_publish_guard;
DEALLOCATE PREPARE stmt_activity_publish_guard;

DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_recover_old;
SET @activity_target_exists = (
    SELECT COUNT(*)
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='mart_user_activity_daily_v2'
);
SET @activity_publish_sql = IF(
    @activity_target_exists=1,
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2 TO votes_mart.mart_user_activity_daily_v2_recover_old, votes_mart.mart_user_activity_daily_v2_recover_new TO votes_mart.mart_user_activity_daily_v2',
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2_recover_new TO votes_mart.mart_user_activity_daily_v2'
);
PREPARE stmt_activity_publish FROM @activity_publish_sql;
EXECUTE stmt_activity_publish;
DEALLOCATE PREPARE stmt_activity_publish;

DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_recover_old;

DROP TABLE votes_mart._wrk_user_activity_accum_v2;

SELECT '[activity recovery] publish complete' AS build_progress;
