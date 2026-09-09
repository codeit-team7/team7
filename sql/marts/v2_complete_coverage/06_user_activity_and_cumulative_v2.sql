/* ============================================================================
   06. 출석 원행 보존 + 사용자 일별 활동 v2 + 누적 상태 v2

   핵심 원칙
   - activity daily는 사용자×날짜가 있는 날만 저장하는 sparse fact다.
   - 행이 없다는 사실을 곧바로 '비활성'으로 해석하지 않는다.
   - 원천마다 관측기간이 달라 source_*_range_flag를 별도 calendar dim에 둔다.
   - 질문세트 생성, 포인트 적립, 요청 수신은 사용자 행동이 아닐 수 있으므로
     user_initiated_proxy_flag와 context_only를 분리한다.
   - next_active_date/gap은 미래 정보이므로 예측 피처로 만들지 않는다.

   추가 원천 커버리지
   - final.accounts_attendance
   - 앞 스크립트에서 만든 v2 fact/dim 전부
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

/* --------------------------------------------------------------------------
   06-A. 출석 원행. JSON이 깨졌거나 비어 있어도 원행은 남긴다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_attendance_record_v2;

CREATE TABLE votes_mart.mart_attendance_record_v2 AS
SELECT
    a.id AS attendance_record_id,
    a.user_id,
    a.attendance_date_list AS attendance_date_list_json,
    JSON_VALID(a.attendance_date_list) AS attendance_json_valid_flag,
    CASE WHEN JSON_VALID(a.attendance_date_list)=1
         THEN JSON_LENGTH(a.attendance_date_list) END AS attendance_list_length,
    (p.user_id IS NULL) AS orphan_user_flag
FROM final.accounts_attendance AS a
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = a.user_id;

ALTER TABLE votes_mart.mart_attendance_record_v2
    ADD PRIMARY KEY (attendance_record_id),
    ADD INDEX idx_attendance_record_user (user_id);


/* --------------------------------------------------------------------------
   06-B. 출석 JSON 원소 보존. 같은 사용자·같은 날짜 중복도 삭제하지 않는다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.bridge_attendance_day_v2;

CREATE TABLE votes_mart.bridge_attendance_day_v2 AS
WITH expanded AS (
    SELECT
        a.attendance_record_id,
        a.user_id,
        jt.attendance_ordinal,
        jt.attendance_date_text,
        STR_TO_DATE(jt.attendance_date_text, '%Y-%m-%d') AS attendance_date
    FROM votes_mart.mart_attendance_record_v2 AS a
    JOIN JSON_TABLE(
        CASE WHEN a.attendance_json_valid_flag=1
             THEN a.attendance_date_list_json ELSE JSON_ARRAY() END,
        '$[*]' COLUMNS (
            attendance_ordinal FOR ORDINALITY,
            attendance_date_text VARCHAR(32) PATH '$'
        )
    ) AS jt ON TRUE
)
SELECT
    e.*,
    (e.attendance_date IS NULL) AS invalid_attendance_date_flag,
    ROW_NUMBER() OVER (
        PARTITION BY e.user_id, e.attendance_date_text
        ORDER BY e.attendance_record_id, e.attendance_ordinal
    ) AS same_user_date_occurrence_rank,
    COUNT(*) OVER (
        PARTITION BY e.user_id, e.attendance_date_text
    ) AS same_user_date_occurrence_count
FROM expanded AS e;

ALTER TABLE votes_mart.bridge_attendance_day_v2
    ADD INDEX idx_attendance_user_day (user_id, attendance_date),
    ADD INDEX idx_attendance_record_ordinal (attendance_record_id, attendance_ordinal);


/* --------------------------------------------------------------------------
   06-C. 데이터 원천별 달력상 관측범위

   *_range_flag는 '그 날짜가 원천의 최소~최대 시각 안'이라는 뜻일 뿐,
   그 날 모든 행동이 완전하게 수집됐다는 보장은 아니다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.dim_source_observation_calendar_v2;

CREATE TABLE votes_mart.dim_source_observation_calendar_v2 AS
WITH RECURSIVE limits AS (
    SELECT
        LEAST(
            (SELECT DATE(MIN(signup_at)) FROM votes_mart.mart_user_acquisition_profile_v2),
            (SELECT DATE(MIN(request_created_at_raw)) FROM votes_mart.mart_friend_request_event_v2),
            (SELECT MIN(attendance_date) FROM votes_mart.bridge_attendance_day_v2
             WHERE attendance_date IS NOT NULL),
            (SELECT DATE(MIN(event_at_raw)) FROM votes_mart.mart_value_event_v2),
            (SELECT DATE(MIN(event_datetime_raw)) FROM votes_mart.fact_hackle_event_24d_v2)
        ) AS min_date,
        GREATEST(
            (SELECT DATE(MAX(signup_at)) FROM votes_mart.mart_user_acquisition_profile_v2),
            (SELECT DATE(MAX(request_created_at_raw)) FROM votes_mart.mart_friend_request_event_v2),
            (SELECT MAX(attendance_date) FROM votes_mart.bridge_attendance_day_v2
             WHERE attendance_date IS NOT NULL),
            (SELECT DATE(MAX(event_at_raw)) FROM votes_mart.mart_value_event_v2),
            (SELECT DATE(MAX(event_datetime_raw)) FROM votes_mart.fact_hackle_event_24d_v2)
        ) AS max_date
),
calendar AS (
    SELECT min_date AS calendar_date, max_date FROM limits
    UNION ALL
    SELECT DATE_ADD(calendar_date, INTERVAL 1 DAY), max_date
    FROM calendar
    WHERE calendar_date < max_date
),
ranges AS (
    SELECT
        (SELECT DATE(MIN(signup_at)) FROM votes_mart.mart_user_acquisition_profile_v2) AS signup_min,
        (SELECT DATE(MAX(signup_at)) FROM votes_mart.mart_user_acquisition_profile_v2) AS signup_max,
        (SELECT DATE(MIN(request_created_at_raw)) FROM votes_mart.mart_friend_request_event_v2) AS friend_min,
        (SELECT DATE(MAX(request_created_at_raw)) FROM votes_mart.mart_friend_request_event_v2) AS friend_max,
        (SELECT MIN(attendance_date) FROM votes_mart.bridge_attendance_day_v2 WHERE attendance_date IS NOT NULL) AS attendance_min,
        (SELECT MAX(attendance_date) FROM votes_mart.bridge_attendance_day_v2 WHERE attendance_date IS NOT NULL) AS attendance_max,
        (SELECT DATE(MIN(question_set_created_at)) FROM votes_mart.mart_question_set_record_v2) AS question_db_min,
        (SELECT DATE(MAX(question_set_created_at)) FROM votes_mart.mart_question_set_record_v2) AS question_db_max,
        (SELECT DATE(MIN(event_at_raw)) FROM votes_mart.mart_value_event_v2 WHERE source_system='DB') AS value_db_min,
        (SELECT DATE(MAX(event_at_raw)) FROM votes_mart.mart_value_event_v2 WHERE source_system='DB') AS value_db_max,
        (SELECT DATE(MIN(event_datetime_raw)) FROM votes_mart.fact_hackle_event_24d_v2) AS hackle_min,
        (SELECT DATE(MAX(event_datetime_raw)) FROM votes_mart.fact_hackle_event_24d_v2) AS hackle_max
)
SELECT
    c.calendar_date,
    (c.calendar_date BETWEEN r.signup_min AND r.signup_max) AS signup_source_range_flag,
    (c.calendar_date BETWEEN r.friend_min AND r.friend_max) AS friend_request_source_range_flag,
    (c.calendar_date BETWEEN r.attendance_min AND r.attendance_max) AS attendance_source_range_flag,
    (c.calendar_date BETWEEN r.question_db_min AND r.question_db_max) AS question_db_source_range_flag,
    (c.calendar_date BETWEEN r.value_db_min AND r.value_db_max) AS value_db_source_range_flag,
    (c.calendar_date BETWEEN r.hackle_min AND r.hackle_max) AS hackle_24d_source_range_flag,
    r.signup_min, r.signup_max,
    r.friend_min, r.friend_max,
    r.attendance_min, r.attendance_max,
    r.question_db_min, r.question_db_max,
    r.value_db_min, r.value_db_max,
    r.hackle_min, r.hackle_max,
    'RANGE_ONLY_NOT_COMPLETENESS_GUARANTEE' AS observation_flag_meaning
FROM calendar AS c
CROSS JOIN ranges AS r;

ALTER TABLE votes_mart.dim_source_observation_calendar_v2
    ADD PRIMARY KEY (calendar_date);


/* --------------------------------------------------------------------------
   06-D. 사용자×날짜 sparse 활동 마트
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_user_activity_daily_v2;

/*
   가장 큰 결과 테이블의 PRIMARY KEY를 CTAS 이후 ALTER로 붙이면 InnoDB가
   테이블 전체를 다시 만들 수 있다. user_id/activity_date만 미리 선언해
   적재와 동시에 clustered PK 및 날짜 보조 인덱스를 만들고, 나머지 SELECT
   컬럼은 MySQL CTAS가 기존 순서대로 추가하도록 한다.
*/
CREATE TABLE votes_mart.mart_user_activity_daily_v2 (
    user_id BIGINT NOT NULL,
    activity_date DATE NOT NULL,
    PRIMARY KEY (user_id, activity_date),
    KEY idx_activity_day_user (activity_date, user_id)
) AS
WITH attendance_day AS (
    SELECT
        user_id, attendance_date AS activity_date,
        COUNT(*) AS attendance_raw_element_count,
        COUNT(DISTINCT attendance_record_id) AS attendance_record_count
    FROM votes_mart.bridge_attendance_day_v2
    WHERE attendance_date IS NOT NULL
    GROUP BY user_id, attendance_date
),
friend_day AS (
    SELECT
        user_id, activity_date,
        SUM(sent_count) AS friend_request_sent_count,
        SUM(received_count) AS friend_request_received_count,
        SUM(final_a_count) AS friend_request_final_a_count,
        SUM(final_p_count) AS friend_request_final_p_count,
        SUM(final_r_count) AS friend_request_final_r_count
    FROM (
        SELECT
            send_user_id AS user_id,
            DATE(request_created_at_raw) AS activity_date,
            COUNT(*) AS sent_count, 0 AS received_count,
            SUM(final_status_code='A') AS final_a_count,
            SUM(final_status_code='P') AS final_p_count,
            SUM(final_status_code='R') AS final_r_count
        FROM votes_mart.mart_friend_request_event_v2
        GROUP BY send_user_id, DATE(request_created_at_raw)
        UNION ALL
        SELECT
            receive_user_id AS user_id,
            DATE(request_created_at_raw) AS activity_date,
            0, COUNT(*), 0, 0, 0
        FROM votes_mart.mart_friend_request_event_v2
        GROUP BY receive_user_id, DATE(request_created_at_raw)
    ) AS x
    WHERE user_id IS NOT NULL AND activity_date IS NOT NULL
    GROUP BY user_id, activity_date
),
question_set_day AS (
    SELECT
        question_set_owner_user_id AS user_id,
        DATE(question_set_created_at) AS activity_date,
        COUNT(DISTINCT question_set_id) AS db_question_set_created_count
    FROM votes_mart.mart_question_set_record_v2
    WHERE question_set_owner_user_id IS NOT NULL
      AND question_set_created_at IS NOT NULL
    GROUP BY question_set_owner_user_id, DATE(question_set_created_at)
),
vote_day AS (
    SELECT
        voter_user_id AS user_id, DATE(vote_record_created_at) AS activity_date,
        COUNT(*) AS db_vote_record_created_count
    FROM votes_mart.mart_vote_record_v2
    WHERE voter_user_id IS NOT NULL AND vote_record_created_at IS NOT NULL
    GROUP BY voter_user_id, DATE(vote_record_created_at)
),
ping_snapshot_day AS (
    SELECT
        chosen_user_id AS user_id, DATE(vote_record_created_at) AS activity_date,
        COUNT(*) AS db_ping_received_record_count,
        SUM(ping_has_read_current=1) AS ping_current_read_record_count,
        SUM(ping_answer_status_current IN ('A','P')) AS ping_current_answered_record_count
    FROM votes_mart.mart_vote_record_v2
    WHERE chosen_user_id IS NOT NULL AND vote_record_created_at IS NOT NULL
    GROUP BY chosen_user_id, DATE(vote_record_created_at)
),
value_day AS (
    SELECT
        service_user_id AS user_id, DATE(event_at_raw) AS activity_date,
        SUM(source_table='accounts_pointhistory' AND event_type='POINT_EARN') AS point_earn_event_count,
        SUM(source_table='accounts_pointhistory' AND event_type='POINT_SPEND') AS point_spend_event_count,
        SUM(source_table='accounts_paymenthistory') AS db_payment_success_count,
        SUM(source_table='accounts_failpaymenthistory') AS db_payment_fail_count,
        SUM(source_table='event_receipts') AS promo_receipt_count
    FROM votes_mart.mart_value_event_v2
    WHERE source_system='DB'
      AND service_user_id IS NOT NULL
      AND event_at_raw IS NOT NULL
    GROUP BY service_user_id, DATE(event_at_raw)
),
hackle_day AS (
    SELECT
        hu.service_user_id AS user_id, DATE(h.event_datetime_raw) AS activity_date,
        COUNT(*) AS hackle_event_count,
        COUNT(DISTINCT hva.derived_visit_id_30m) AS hackle_visit_count,
        SUM(hek.attribute_value_raw='click_question_start') AS hackle_question_start_count,
        SUM(hek.attribute_value_raw='complete_question') AS hackle_question_complete_count,
        SUM(hek.attribute_value_raw IN ('view_shop','click_purchase','complete_purchase')) AS hackle_shop_event_count
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
),
signup_day AS (
    SELECT
        user_id, DATE(signup_at) AS activity_date,
        1 AS signup_record_count
    FROM votes_mart.mart_user_acquisition_profile_v2
    WHERE signup_at IS NOT NULL
),
all_keys AS (
    SELECT user_id, activity_date FROM attendance_day
    UNION
    SELECT user_id, activity_date FROM friend_day
    UNION
    SELECT user_id, activity_date FROM question_set_day
    UNION
    SELECT user_id, activity_date FROM vote_day
    UNION
    SELECT user_id, activity_date FROM ping_snapshot_day
    UNION
    SELECT user_id, activity_date FROM value_day
    UNION
    SELECT user_id, activity_date FROM hackle_day
    UNION
    SELECT user_id, activity_date FROM signup_day
)
SELECT
    k.user_id,
    k.activity_date,
    DATEDIFF(k.activity_date, DATE(p.signup_at)) AS days_since_signup,
    COALESCE(s.signup_record_count,0) AS signup_record_count,
    COALESCE(a.attendance_raw_element_count,0) AS attendance_raw_element_count,
    COALESCE(a.attendance_record_count,0) AS attendance_record_count,
    COALESCE(f.friend_request_sent_count,0) AS friend_request_sent_count,
    COALESCE(f.friend_request_received_count,0) AS friend_request_received_count,
    COALESCE(f.friend_request_final_a_count,0) AS friend_request_sent_final_a_count,
    COALESCE(f.friend_request_final_p_count,0) AS friend_request_sent_final_p_count,
    COALESCE(f.friend_request_final_r_count,0) AS friend_request_sent_final_r_count,
    COALESCE(q.db_question_set_created_count,0) AS db_question_set_created_count,
    COALESCE(v.db_vote_record_created_count,0) AS db_vote_record_created_count,
    COALESCE(pg.db_ping_received_record_count,0) AS db_ping_received_record_count,
    COALESCE(pg.ping_current_read_record_count,0) AS ping_current_read_record_count,
    COALESCE(pg.ping_current_answered_record_count,0) AS ping_current_answered_record_count,
    COALESCE(val.point_earn_event_count,0) AS point_earn_event_count,
    COALESCE(val.point_spend_event_count,0) AS point_spend_event_count,
    COALESCE(val.db_payment_success_count,0) AS db_payment_success_count,
    COALESCE(val.db_payment_fail_count,0) AS db_payment_fail_count,
    COALESCE(val.promo_receipt_count,0) AS promo_receipt_count,
    COALESCE(h.hackle_event_count,0) AS hackle_event_count,
    COALESCE(h.hackle_visit_count,0) AS hackle_visit_count,
    COALESCE(h.hackle_question_start_count,0) AS hackle_question_start_count,
    COALESCE(h.hackle_question_complete_count,0) AS hackle_question_complete_count,
    COALESCE(h.hackle_shop_event_count,0) AS hackle_shop_event_count,
    c.signup_source_range_flag,
    c.friend_request_source_range_flag,
    c.attendance_source_range_flag,
    c.question_db_source_range_flag,
    c.value_db_source_range_flag,
    c.hackle_24d_source_range_flag,
    (
        COALESCE(a.attendance_raw_element_count,0)>0 OR
        COALESCE(f.friend_request_sent_count,0)>0 OR
        COALESCE(v.db_vote_record_created_count,0)>0 OR
        COALESCE(val.db_payment_success_count,0)>0 OR
        COALESCE(val.db_payment_fail_count,0)>0 OR
        COALESCE(h.hackle_event_count,0)>0
    ) AS user_initiated_activity_proxy_flag,
    (
        COALESCE(s.signup_record_count,0)+
        COALESCE(a.attendance_raw_element_count,0)+
        COALESCE(f.friend_request_sent_count,0)+
        COALESCE(f.friend_request_received_count,0)+
        COALESCE(q.db_question_set_created_count,0)+
        COALESCE(v.db_vote_record_created_count,0)+
        COALESCE(pg.db_ping_received_record_count,0)+
        COALESCE(val.point_earn_event_count,0)+
        COALESCE(val.point_spend_event_count,0)+
        COALESCE(val.db_payment_success_count,0)+
        COALESCE(val.db_payment_fail_count,0)+
        COALESCE(val.promo_receipt_count,0)+
        COALESCE(h.hackle_event_count,0)
    )>0 AS any_record_observed_flag,
    (
        COALESCE(s.signup_record_count,0)+
        COALESCE(f.friend_request_received_count,0)+
        COALESCE(q.db_question_set_created_count,0)+
        COALESCE(pg.db_ping_received_record_count,0)+
        COALESCE(val.point_earn_event_count,0)>0
        AND
        COALESCE(a.attendance_raw_element_count,0)+
        COALESCE(f.friend_request_sent_count,0)+
        COALESCE(v.db_vote_record_created_count,0)+
        COALESCE(val.db_payment_success_count,0)+
        COALESCE(val.db_payment_fail_count,0)+
        COALESCE(h.hackle_event_count,0)=0
    ) AS context_or_system_only_day_flag,
    (p.user_id IS NULL) AS user_dimension_missing_flag
FROM all_keys AS k
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id=k.user_id
LEFT JOIN signup_day AS s
  ON s.user_id=k.user_id AND s.activity_date=k.activity_date
LEFT JOIN attendance_day AS a
  ON a.user_id=k.user_id AND a.activity_date=k.activity_date
LEFT JOIN friend_day AS f
  ON f.user_id=k.user_id AND f.activity_date=k.activity_date
LEFT JOIN question_set_day AS q
  ON q.user_id=k.user_id AND q.activity_date=k.activity_date
LEFT JOIN vote_day AS v
  ON v.user_id=k.user_id AND v.activity_date=k.activity_date
LEFT JOIN ping_snapshot_day AS pg
  ON pg.user_id=k.user_id AND pg.activity_date=k.activity_date
LEFT JOIN value_day AS val
  ON val.user_id=k.user_id AND val.activity_date=k.activity_date
LEFT JOIN hackle_day AS h
  ON h.user_id=k.user_id AND h.activity_date=k.activity_date
LEFT JOIN votes_mart.dim_source_observation_calendar_v2 AS c
  ON c.calendar_date=k.activity_date;

/* 보조 인덱스만 INPLACE로 추가해 CTAS 결과 전체 재작성 가능성을 피한다. */
ALTER TABLE votes_mart.mart_user_activity_daily_v2
    ADD INDEX idx_activity_proxy (user_initiated_activity_proxy_flag, activity_date),
    ALGORITHM=INPLACE,
    LOCK=NONE;


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
