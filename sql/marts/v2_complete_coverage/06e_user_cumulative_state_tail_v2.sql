/* --------------------------------------------------------------------------
   06-E. 기존 '1년 여정 퍼널'을 대체하는 사용자 누적 상태

   순차 인과 퍼널이 아니라, 서로 다른 원천에서 관측된 누적 이력을 한 사용자
   단위로 요약한다. 컬럼명에 history/current/source scope를 명시한다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_user_cumulative_state_1y_v2;

CREATE TABLE votes_mart.mart_user_cumulative_state_1y_v2 AS
WITH friend_agg AS (
    SELECT
        user_id,
        sent_request_count AS friend_request_sent_count,
        received_request_count AS friend_request_received_count
    /* Stage 1에서 원행과 정확히 대사된 사용자 집계를 재사용한다. */
    FROM votes_mart.mart_user_viral_profile_v2
),
qset_agg AS (
    SELECT
        question_set_owner_user_id AS user_id,
        COUNT(*) AS question_set_history_count,
        SUM(question_set_status='F') AS question_set_current_f_status_count
    FROM votes_mart.mart_question_set_record_v2
    GROUP BY question_set_owner_user_id
),
vote_actor_agg AS (
    SELECT
        voter_user_id AS user_id,
        COUNT(*) AS vote_record_created_count
    FROM votes_mart.mart_vote_record_v2
    GROUP BY voter_user_id
),
ping_receiver_agg AS (
    SELECT
        chosen_user_id AS user_id,
        COUNT(*) AS ping_received_record_count,
        SUM(ping_has_read_current=1) AS ping_current_read_record_count,
        SUM(ping_answer_status_current IN ('A','P')) AS ping_current_answered_record_count
    FROM votes_mart.mart_vote_record_v2
    GROUP BY chosen_user_id
),
value_agg AS (
    SELECT
        service_user_id AS user_id,
        SUM(event_type='POINT_EARN') AS point_earn_history_count,
        SUM(event_type='POINT_SPEND') AS point_spend_history_count,
        SUM(event_type='PAYMENT_SUCCESS') AS db_payment_success_history_count,
        SUM(event_type='PAYMENT_FAIL') AS db_payment_fail_history_count,
        SUM(event_type='PROMO_POINT_RECEIPT') AS promo_receipt_history_count
    FROM votes_mart.mart_value_event_v2
    WHERE source_system='DB' AND service_user_id IS NOT NULL
    GROUP BY service_user_id
),
safety_agg AS (
    SELECT
        actor_user_id AS user_id,
        COUNT(*) AS feedback_block_report_record_count,
        SUM(COALESCE(report_count_weight,1)) AS feedback_block_report_weight
    FROM votes_mart.mart_safety_event_v2
    WHERE actor_user_id IS NOT NULL
    GROUP BY actor_user_id
),
hackle_agg AS (
    SELECT
        hu.service_user_id AS user_id,
        SUM(v.visit_event_count_30m) AS hackle_24d_event_count,
        COUNT(*) AS hackle_24d_visit_count,
        SUM(v.question_start_count) AS hackle_24d_question_start_count,
        SUM(v.question_complete_count) AS hackle_24d_question_complete_count
    FROM votes_mart.dim_hackle_visit_30m_v2 AS v
    JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
      ON hs.session_sk=v.original_session_sk
    JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
      ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
    WHERE hu.service_user_id IS NOT NULL
    GROUP BY hu.service_user_id
),
attendance_agg AS (
    SELECT
        user_id,
        COUNT(DISTINCT attendance_date) AS attendance_distinct_day_count,
        COUNT(*) AS attendance_raw_element_count
    FROM votes_mart.bridge_attendance_day_v2
    WHERE attendance_date IS NOT NULL
    GROUP BY user_id
)
SELECT
    p.user_id,
    p.signup_at,
    p.current_school_id,
    p.current_school_type,
    p.current_grade,
    p.current_class_num,
    p.gender,
    p.is_staff,
    p.is_superuser,
    p.current_ban_status,
    p.current_friend_list_length,
    COALESCE(f.friend_request_sent_count,0) AS friend_request_sent_history_count,
    COALESCE(f.friend_request_received_count,0) AS friend_request_received_history_count,
    COALESCE(q.question_set_history_count,0) AS question_set_history_count_top10_scope,
    COALESCE(q.question_set_current_f_status_count,0) AS question_set_current_f_status_count_top10_scope,
    COALESCE(va.vote_record_created_count,0) AS vote_created_by_user_count_top10_scope,
    COALESCE(pr.ping_received_record_count,0) AS ping_received_record_count_top10_scope,
    COALESCE(pr.ping_current_read_record_count,0) AS ping_current_read_record_count_top10_scope,
    COALESCE(pr.ping_current_answered_record_count,0) AS ping_current_answered_record_count_top10_scope,
    COALESCE(val.point_earn_history_count,0) AS point_earn_history_count_top10_scope,
    COALESCE(val.point_spend_history_count,0) AS point_spend_history_count_top10_scope,
    COALESCE(val.db_payment_success_history_count,0) AS db_payment_success_history_count_global_scope,
    COALESCE(val.db_payment_fail_history_count,0) AS db_payment_fail_history_count_incomplete_period,
    COALESCE(val.promo_receipt_history_count,0) AS promo_receipt_history_count_global_scope,
    COALESCE(sa.feedback_block_report_record_count,0) AS safety_or_feedback_actor_record_count_global_scope,
    COALESCE(sa.feedback_block_report_weight,0) AS safety_or_feedback_actor_weight_global_scope,
    COALESCE(h.hackle_24d_event_count,0) AS hackle_24d_event_count,
    COALESCE(h.hackle_24d_visit_count,0) AS hackle_24d_visit_count,
    COALESCE(h.hackle_24d_question_start_count,0) AS hackle_24d_question_start_count,
    COALESCE(h.hackle_24d_question_complete_count,0) AS hackle_24d_question_complete_count,
    COALESCE(a.attendance_distinct_day_count,0) AS attendance_distinct_day_count,
    COALESCE(a.attendance_raw_element_count,0) AS attendance_raw_element_count,
    (q.user_id IS NOT NULL) AS question_top10_source_observed_flag,
    (val.user_id IS NOT NULL) AS value_db_source_observed_flag,
    (h.user_id IS NOT NULL) AS hackle_identified_user_observed_flag,
    (a.user_id IS NOT NULL) AS attendance_source_observed_flag
FROM votes_mart.mart_user_acquisition_profile_v2 AS p
LEFT JOIN friend_agg AS f ON f.user_id=p.user_id
LEFT JOIN qset_agg AS q ON q.user_id=p.user_id
LEFT JOIN vote_actor_agg AS va ON va.user_id=p.user_id
LEFT JOIN ping_receiver_agg AS pr ON pr.user_id=p.user_id
LEFT JOIN value_agg AS val ON val.user_id=p.user_id
LEFT JOIN safety_agg AS sa ON sa.user_id=p.user_id
LEFT JOIN hackle_agg AS h ON h.user_id=p.user_id
LEFT JOIN attendance_agg AS a ON a.user_id=p.user_id;

ALTER TABLE votes_mart.mart_user_cumulative_state_1y_v2
    ADD PRIMARY KEY (user_id),
    ADD INDEX idx_state_school (current_school_id),
    ADD INDEX idx_state_signup (signup_at);
