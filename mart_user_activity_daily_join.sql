-- ==============================================================================
-- [SQL] mart_user_activity_daily 구축 및 조인 쿼리
-- (반영 사항: Hackle 이벤트 유지 + accounts_friendrequest 순수 발송(send_user_id)만 집계)
-- ==============================================================================

CREATE OR REPLACE TABLE `mart_user_activity_daily` AS

WITH 
-- 1. Hackle 세션-유저 1:1 고유 매핑
valid_sessions AS (
    SELECT 
        session_id,
        CAST(user_id AS INT64) AS mapped_user_id
    FROM `hackle_properties`
    WHERE REGEXP_CONTAINS(user_id, r'^\d+$')
    GROUP BY session_id, user_id
    QUALIFY COUNT(*) OVER(PARTITION BY session_id) = 1
),

-- 2. 출석체크 JSON 언패킹 (일별)
attendance_daily AS (
    SELECT 
        user_id,
        DATE(att_date) AS activity_date,
        1 AS attendance_flag
    FROM `accounts_attendance`,
         UNNEST(JSON_VALUE_ARRAY(attendance_date_list)) AS att_date
),

-- 3. Hackle 앱 실행 및 행동 집계 (일별)
hackle_daily AS (
    SELECT
        vs.mapped_user_id AS user_id,
        DATE(he.event_datetime) AS activity_date,
        COUNTIF(he.event_key = '$session_start')       AS launch_app_count,
        COUNT(DISTINCT he.session_id)                  AS session_count,
        COUNTIF(he.event_key = 'click_question_start') AS question_start_count,
        COUNTIF(he.event_key = 'complete_question')    AS question_complete_count,
        COUNTIF(he.event_key = 'skip_question')        AS skip_count,
        COUNTIF(he.event_key = 'click_question_open')  AS ping_open_count
    FROM `hackle_events` he
    JOIN valid_sessions vs ON he.session_id = vs.session_id
    GROUP BY user_id, activity_date
),

-- 4. 질문 세트 시작 및 완주 집계 (DB 원장)
questionset_daily AS (
    SELECT
        user_id,
        DATE(created_at) AS activity_date,
        COUNT(*) AS question_start_count,
        COUNTIF(status = 'F') AS question_complete_count
    FROM `polls_questionset`
    GROUP BY user_id, activity_date
),

-- 5. 친구 요청 발송 집계 (※ 핵심: send_user_id만 집계하여 수신으로 인한 유령 행 차단!)
friend_daily AS (
    SELECT 
        send_user_id AS user_id, 
        DATE(created_at) AS activity_date, 
        COUNT(*) AS friend_action_count
    FROM `accounts_friendrequest`
    WHERE send_user_id IS NOT NULL
    GROUP BY send_user_id, DATE(created_at)
),

-- 6. 포인트 거래 집계
point_daily AS (
    SELECT 
        user_id,
        DATE(created_at) AS activity_date,
        SUM(CASE WHEN delta_point > 0 THEN delta_point ELSE 0 END) AS point_earn,
        SUM(CASE WHEN delta_point < 0 THEN ABS(delta_point) ELSE 0 END) AS point_spend
    FROM `accounts_pointhistory`
    GROUP BY user_id, activity_date
),

-- 7. 결제 성공 집계
payment_daily AS (
    SELECT 
        user_id,
        DATE(created_at) AS activity_date,
        COUNT(*) AS payment_count
    FROM `accounts_paymenthistory`
    GROUP BY user_id, activity_date
),

-- 8. 모든 능동적 활동 유저-날짜 기준 Base 생성 (Full Outer Join 효과)
all_user_dates AS (
    SELECT user_id, activity_date FROM attendance_daily
    UNION DISTINCT
    SELECT user_id, activity_date FROM hackle_daily
    UNION DISTINCT
    SELECT user_id, activity_date FROM questionset_daily
    UNION DISTINCT
    SELECT user_id, activity_date FROM friend_daily
    UNION DISTINCT
    SELECT user_id, activity_date FROM point_daily
    UNION DISTINCT
    SELECT user_id, activity_date FROM payment_daily
),

-- 9. 전체 지표 병합
daily_combined AS (
    SELECT
        base.user_id,
        base.activity_date,
        u.created_at AS user_signup_at,
        COALESCE(att.attendance_flag, 0)                  AS attendance_flag,
        COALESCE(h.launch_app_count, 0)                   AS launch_app_count,
        COALESCE(h.session_count, 0)                      AS session_count,
        GREATEST(COALESCE(h.question_start_count, 0), COALESCE(qs.question_start_count, 0)) AS question_start_count,
        GREATEST(COALESCE(h.question_complete_count, 0), COALESCE(qs.question_complete_count, 0)) AS question_complete_count,
        COALESCE(h.skip_count, 0)                         AS skip_count,
        COALESCE(h.ping_open_count, 0)                    AS ping_open_count,
        COALESCE(f.friend_action_count, 0)                AS friend_action_count,
        COALESCE(p.point_earn, 0)                         AS point_earn,
        COALESCE(p.point_spend, 0)                        AS point_spend,
        COALESCE(pay.payment_count, 0)                    AS payment_count
    FROM all_user_dates base
    LEFT JOIN `accounts_user` u        ON base.user_id = u.id
    LEFT JOIN attendance_daily att     ON base.user_id = att.user_id AND base.activity_date = att.activity_date
    LEFT JOIN hackle_daily h           ON base.user_id = h.user_id   AND base.activity_date = h.activity_date
    LEFT JOIN questionset_daily qs     ON base.user_id = qs.user_id  AND base.activity_date = qs.activity_date
    LEFT JOIN friend_daily f           ON base.user_id = f.user_id   AND base.activity_date = f.activity_date
    LEFT JOIN point_daily p            ON base.user_id = p.user_id   AND base.activity_date = p.activity_date
    LEFT JOIN payment_daily pay        ON base.user_id = pay.user_id AND base.activity_date = pay.activity_date
)

-- 10. 시계열 윈도우 함수 및 활성 판별 컬럼 최종 산출
SELECT
    user_id,
    activity_date,
    CAST(attendance_flag AS BOOL) AS attendance_flag,
    launch_app_count,
    session_count,
    question_start_count,
    question_complete_count,
    skip_count,
    ping_open_count,
    friend_action_count,
    point_earn,
    point_spend,
    payment_count,
    DATE_DIFF(activity_date, DATE(user_signup_at), DAY) AS days_since_first_active,
    DATE_DIFF(activity_date, LAG(activity_date) OVER(PARTITION BY user_id ORDER BY activity_date), DAY) AS days_since_previous_active,
    LEAD(activity_date) OVER(PARTITION BY user_id ORDER BY activity_date) AS next_active_date,
    DATE_DIFF(LEAD(activity_date) OVER(PARTITION BY user_id ORDER BY activity_date), activity_date, DAY) AS gap_to_next_activity,
    (attendance_flag = 1 OR session_count > 0 OR question_start_count > 0 OR skip_count > 0 OR ping_open_count > 0 OR friend_action_count > 0 OR point_spend > 0 OR payment_count > 0) AS is_active
FROM daily_combined
ORDER BY user_id, activity_date;
