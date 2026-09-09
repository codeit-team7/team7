/* --------------------------------------------------------------------------
   10. final projection + guarded atomic publish
---------------------------------------------------------------------------- */
SELECT '[activity resume-after-7] final projection' AS build_progress;
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after7_new;

CREATE TABLE votes_mart.mart_user_activity_daily_v2_resume_after7_new (
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

ALTER TABLE votes_mart.mart_user_activity_daily_v2_resume_after7_new
    ADD INDEX idx_activity_day_user (activity_date, user_id),
    ADD INDEX idx_activity_proxy (user_initiated_activity_proxy_flag, activity_date),
    ALGORITHM=INPLACE,
    LOCK=NONE;

SELECT COUNT(*)
INTO @activity_new_row_count
FROM votes_mart.mart_user_activity_daily_v2_resume_after7_new;

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
FROM votes_mart.mart_user_activity_daily_v2_resume_after7_new AS n
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
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2_resume_after7_old;
SET @activity_target_exists = (
    SELECT COUNT(*)
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='mart_user_activity_daily_v2'
      AND TABLE_TYPE='BASE TABLE'
);
SET @activity_publish_sql = IF(
    @activity_target_exists=1,
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2 TO votes_mart.mart_user_activity_daily_v2_resume_after7_old, votes_mart.mart_user_activity_daily_v2_resume_after7_new TO votes_mart.mart_user_activity_daily_v2',
    'RENAME TABLE votes_mart.mart_user_activity_daily_v2_resume_after7_new TO votes_mart.mart_user_activity_daily_v2'
);
PREPARE stmt_activity_publish FROM @activity_publish_sql;
EXECUTE stmt_activity_publish;
DEALLOCATE PREPARE stmt_activity_publish;

SELECT '[activity resume-after-7] publish complete; checkpoint retained through final QA'
    AS build_progress;

