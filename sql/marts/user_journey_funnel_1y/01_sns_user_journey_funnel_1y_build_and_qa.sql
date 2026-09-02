/* ============================================================================
   SNS 1-YEAR USER JOURNEY FUNNEL: BUILD + QA
   MySQL 8.0 / source schema: final / mart schema: votes_mart
   Prepared: 2026-09-02

   PURPOSE
   - 약 1년 동안 가입한 사용자가 DB 기록상 어떤 서비스 단계에 도달했는지
     사용자 1명당 1행으로 구축한다.
   - 첫 24시간 분석은 포함하지 않는다.
   - 기존 24일 Hackle 세션 퍼널과 별개로 유지한다.

   FINAL GRAIN
   - user_id 1행

   COHORT
   - signup_at >= 2023-04-28 00:00:00 KST
   - signup_at <  2024-05-07 00:00:00 KST (2024-05-06 포함)
   - is_staff=0, is_superuser=0
   - ban_status는 제거하지 않고 세그먼트로 보존
   - 일반 테스트 계정은 명세서에 식별 플래그·ID 명단이 없어 자동 제외하지 못함

   CUMULATIVE STATE FUNNEL
   1. SIGNUP_COMPLETED
   2. QUESTION_SET_STARTED
   3. QUESTION_SET_FINISHED_STATE
   4. PING_RECEIVED_AFTER_QUESTION_START
   5. PING_READ_FINAL_STATE
   6. PING_ANSWERED_FINAL_STATE
   7. PAYMENT_SUCCESS_AFTER_PING_RECEIPT

   INTERPRETATION GUARDRAILS
   - QUESTION_SET_FINISHED_STATE는 polls_questionset.status='F'인 현재 상태다.
     완료 발생 시각은 알 수 없다.
   - PING_READ_FINAL_STATE는 has_read=1인 현재 상태다. 열람 시각은 알 수 없다.
   - PING_ANSWERED_FINAL_STATE는 answer_status IN ('A','P')인 현재 상태다.
     answer_updated_at은 미답변에도 값이 있어 답변 시각으로 사용하지 않는다.
   - stage 4~6은 최초 질문 세트 생성 이후 생성된 수신 Ping만 사용한다.
   - stage 7은 Ping 수신 이후 기록된 실제 PAYMENT_SUCCESS만 사용한다.
     답변 시각이 없으므로 '답변 후 결제'의 엄밀한 순서는 확정할 수 없다.
   - 결제 성공 원장은 2023-05-13부터 관찰된다. 그 이전 가입자는 초기 결제가
     누락됐을 수 있어 payment_full_cohort_coverage_flag로 구분한다.
   - 신규 가입자마다 관찰 가능 기간이 다르므로 followup_days_available을 보존하고,
     분석 시 가입 월별 결과와 충분한 관찰기간을 가진 사용자 결과를 함께 확인한다.
   - 원시 시각은 KST로 간주하고 CONVERT_TZ를 적용하지 않는다.

   SAFE RELEASE PROCESS
   1) 이 파일로 _build 후보 테이블 생성
   2) 마지막 release_gate_status 확인
   3) PASS일 때만 publish_after_pass 파일 실행
============================================================================ */
SET NAMES utf8mb4;
SET SESSION group_concat_max_len = 1024 * 1024;

SET @cohort_start_at = TIMESTAMP('2023-04-28 00:00:00');
SET @feature_end_exclusive = TIMESTAMP('2024-05-07 00:00:00');
SET @payment_source_start_at = TIMESTAMP('2023-05-13 00:00:00');
SET @mart_built_at = CURRENT_TIMESTAMP(6);

/* ============================================================================
   0. PREFLIGHT: SOURCE EXISTENCE / PERIOD / NULL CHECK
============================================================================ */

SELECT
    table_name,
    table_rows AS optimizer_estimated_rows
FROM information_schema.tables
WHERE table_schema = 'final'
  AND table_name IN (
      'accounts_user',
      'accounts_group',
      'accounts_school',
      'polls_questionset',
      'accounts_userquestionrecord',
      'accounts_paymenthistory',
      'accounts_failpaymenthistory'
  )
ORDER BY table_name;

SELECT
    'accounts_user' AS source_name,
    COUNT(*) AS row_count,
    MIN(created_at) AS min_at,
    MAX(created_at) AS max_at,
    SUM(created_at IS NULL) AS null_time_count
FROM final.accounts_user

UNION ALL

SELECT
    'polls_questionset', COUNT(*), MIN(created_at), MAX(created_at),
    SUM(created_at IS NULL)
FROM final.polls_questionset

UNION ALL

SELECT
    'accounts_userquestionrecord', COUNT(*), MIN(created_at), MAX(created_at),
    SUM(created_at IS NULL)
FROM final.accounts_userquestionrecord

UNION ALL

SELECT
    'accounts_paymenthistory', COUNT(*), MIN(created_at), MAX(created_at),
    SUM(created_at IS NULL)
FROM final.accounts_paymenthistory

UNION ALL

SELECT
    'accounts_failpaymenthistory', COUNT(*), MIN(created_at), MAX(created_at),
    SUM(created_at IS NULL)
FROM final.accounts_failpaymenthistory;

SELECT
    COUNT(*) AS eligible_signup_user_count,
    COUNT(DISTINCT id) AS unique_user_count,
    MIN(created_at) AS cohort_min_signup_at,
    MAX(created_at) AS cohort_max_signup_at
FROM final.accounts_user
WHERE created_at >= @cohort_start_at
  AND created_at < @feature_end_exclusive
  AND is_staff = 0
  AND is_superuser = 0;

/* ============================================================================
   1. BUILD CANDIDATE TABLE
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_user_journey_funnel_1y_build;

CREATE TABLE votes_mart.mart_user_journey_funnel_1y_build AS
WITH
user_cohort AS (
    SELECT
        u.id AS user_id,
        u.created_at AS signup_at,
        DATE(u.created_at) AS signup_date,
        u.gender,
        u.ban_status,
        u.is_push_on,
        u.group_id,
        g.grade,
        g.class_num,
        g.school_id,
        s.school_type
    FROM final.accounts_user u
    LEFT JOIN final.accounts_group g
      ON g.id = u.group_id
    LEFT JOIN final.accounts_school s
      ON s.id = g.school_id
    WHERE u.created_at >= @cohort_start_at
      AND u.created_at < @feature_end_exclusive
      AND u.is_staff = 0
      AND u.is_superuser = 0
),

question_set_agg AS (
    SELECT
        c.user_id,
        COUNT(*) AS question_set_count,
        SUM(qs.status = 'F') AS finished_question_set_count,
        SUM(qs.status = 'O') AS opened_question_set_count,
        SUM(qs.status = 'C') AS closed_question_set_count,
        MIN(qs.created_at) AS first_question_set_at,
        MIN(CASE WHEN qs.status = 'F' THEN qs.created_at END)
            AS first_finished_state_question_set_created_at,
        MAX(qs.created_at) AS last_question_set_at
    FROM user_cohort c
    JOIN final.polls_questionset qs
      ON qs.user_id = c.user_id
     AND qs.created_at >= c.signup_at
     AND qs.created_at < @feature_end_exclusive
    GROUP BY c.user_id
),

ping_agg AS (
    SELECT
        c.user_id,
        COUNT(*) AS ping_received_count,
        SUM(r.has_read = 1) AS ping_read_final_count,
        SUM(r.answer_status IN ('A', 'P')) AS ping_answered_final_count,
        SUM(r.status = 'I') AS ping_initial_hint_state_count,
        SUM(r.status = 'B') AS ping_blocked_state_count,
        SUM(r.report_count > 0) AS ping_with_report_count,

        SUM(q.first_question_set_at IS NOT NULL
            AND r.created_at >= q.first_question_set_at)
            AS ping_received_after_question_start_count,
        SUM(q.first_question_set_at IS NOT NULL
            AND r.created_at >= q.first_question_set_at
            AND r.has_read = 1)
            AS ping_read_final_after_question_start_count,
        SUM(q.first_question_set_at IS NOT NULL
            AND r.created_at >= q.first_question_set_at
            AND r.answer_status IN ('A', 'P'))
            AS ping_answered_final_after_question_start_count,

        MIN(r.created_at) AS first_ping_received_at,
        MIN(CASE
                WHEN q.first_question_set_at IS NOT NULL
                 AND r.created_at >= q.first_question_set_at
                THEN r.created_at
            END) AS first_ping_received_after_question_start_at,
        MIN(CASE
                WHEN q.first_question_set_at IS NOT NULL
                 AND r.created_at >= q.first_question_set_at
                 AND r.has_read = 1
                THEN r.created_at
            END) AS first_currently_read_ping_created_at,
        MIN(CASE
                WHEN q.first_question_set_at IS NOT NULL
                 AND r.created_at >= q.first_question_set_at
                 AND r.answer_status IN ('A', 'P')
                THEN r.created_at
            END) AS first_currently_answered_ping_created_at
    FROM user_cohort c
    JOIN final.accounts_userquestionrecord r
      ON r.chosen_user_id = c.user_id
     AND r.created_at >= c.signup_at
     AND r.created_at < @feature_end_exclusive
    LEFT JOIN question_set_agg q
      ON q.user_id = c.user_id
    GROUP BY c.user_id, q.first_question_set_at
),

payment_event AS (
    SELECT
        user_id,
        created_at,
        'SUCCESS' AS payment_result
    FROM final.accounts_paymenthistory
    WHERE created_at >= @payment_source_start_at
      AND created_at < @feature_end_exclusive

    UNION ALL

    SELECT
        user_id,
        created_at,
        'FAIL' AS payment_result
    FROM final.accounts_failpaymenthistory
    WHERE created_at >= @payment_source_start_at
      AND created_at < @feature_end_exclusive
),

payment_agg AS (
    SELECT
        c.user_id,
        COUNT(*) AS payment_attempt_count,
        SUM(p.payment_result = 'SUCCESS') AS payment_success_count,
        SUM(p.payment_result = 'FAIL') AS payment_fail_count,
        SUM(ping.first_ping_received_after_question_start_at IS NOT NULL
            AND p.created_at >= ping.first_ping_received_after_question_start_at
            AND p.payment_result = 'SUCCESS')
            AS payment_success_after_ping_count,
        MIN(CASE WHEN p.payment_result = 'SUCCESS'
                 THEN p.created_at END)
            AS first_payment_success_at,
        MIN(CASE
                WHEN ping.first_ping_received_after_question_start_at IS NOT NULL
                 AND p.created_at >= ping.first_ping_received_after_question_start_at
                 AND p.payment_result = 'SUCCESS'
                THEN p.created_at
            END) AS first_payment_success_after_ping_at
    FROM user_cohort c
    JOIN payment_event p
      ON p.user_id = c.user_id
     AND p.created_at >= GREATEST(c.signup_at, @payment_source_start_at)
     AND p.created_at < @feature_end_exclusive
    LEFT JOIN ping_agg ping
      ON ping.user_id = c.user_id
    GROUP BY c.user_id, ping.first_ping_received_after_question_start_at
),

base_features AS (
    SELECT
        c.*,
        TIMESTAMPDIFF(DAY, c.signup_at, @feature_end_exclusive)
            AS followup_days_available,
        CAST(c.signup_at >= @payment_source_start_at AS UNSIGNED)
            AS payment_full_cohort_coverage_flag,

        COALESCE(q.question_set_count, 0) AS question_set_count,
        COALESCE(q.finished_question_set_count, 0)
            AS finished_question_set_count,
        COALESCE(q.opened_question_set_count, 0)
            AS opened_question_set_count,
        COALESCE(q.closed_question_set_count, 0)
            AS closed_question_set_count,
        q.first_question_set_at,
        q.first_finished_state_question_set_created_at,
        q.last_question_set_at,

        COALESCE(ping.ping_received_count, 0) AS ping_received_count,
        COALESCE(ping.ping_read_final_count, 0) AS ping_read_final_count,
        COALESCE(ping.ping_answered_final_count, 0)
            AS ping_answered_final_count,
        COALESCE(ping.ping_initial_hint_state_count, 0)
            AS ping_initial_hint_state_count,
        COALESCE(ping.ping_blocked_state_count, 0)
            AS ping_blocked_state_count,
        COALESCE(ping.ping_with_report_count, 0) AS ping_with_report_count,
        COALESCE(ping.ping_received_after_question_start_count, 0)
            AS ping_received_after_question_start_count,
        COALESCE(ping.ping_read_final_after_question_start_count, 0)
            AS ping_read_final_after_question_start_count,
        COALESCE(ping.ping_answered_final_after_question_start_count, 0)
            AS ping_answered_final_after_question_start_count,
        ping.first_ping_received_at,
        ping.first_ping_received_after_question_start_at,
        ping.first_currently_read_ping_created_at,
        ping.first_currently_answered_ping_created_at,

        COALESCE(pay.payment_attempt_count, 0) AS payment_attempt_count,
        COALESCE(pay.payment_success_count, 0) AS payment_success_count,
        COALESCE(pay.payment_fail_count, 0) AS payment_fail_count,
        COALESCE(pay.payment_success_after_ping_count, 0)
            AS payment_success_after_ping_count,
        pay.first_payment_success_at,
        pay.first_payment_success_after_ping_at
    FROM user_cohort c
    LEFT JOIN question_set_agg q
      ON q.user_id = c.user_id
    LEFT JOIN ping_agg ping
      ON ping.user_id = c.user_id
    LEFT JOIN payment_agg pay
      ON pay.user_id = c.user_id
),

state_flags AS (
    SELECT
        b.*,
        1 AS signup_completed_flag,
        CAST(b.question_set_count > 0 AS UNSIGNED)
            AS question_started_flag,
        CAST(b.finished_question_set_count > 0 AS UNSIGNED)
            AS question_finished_state_flag,
        CAST(b.ping_received_after_question_start_count > 0 AS UNSIGNED)
            AS ping_received_after_question_start_flag,
        CAST(b.ping_read_final_after_question_start_count > 0 AS UNSIGNED)
            AS ping_read_final_after_question_start_flag,
        CAST(b.ping_answered_final_after_question_start_count > 0 AS UNSIGNED)
            AS ping_answered_final_after_question_start_flag,
        CAST(b.payment_success_after_ping_count > 0 AS UNSIGNED)
            AS payment_success_after_ping_flag
    FROM base_features b
),

cumulative_flags AS (
    SELECT
        f.*,
        1 AS cumulative_stage_1_flag,
        f.question_started_flag AS cumulative_stage_2_flag,
        CAST(f.question_started_flag = 1
             AND f.question_finished_state_flag = 1 AS UNSIGNED)
            AS cumulative_stage_3_flag,
        CAST(f.question_started_flag = 1
             AND f.question_finished_state_flag = 1
             AND f.ping_received_after_question_start_flag = 1 AS UNSIGNED)
            AS cumulative_stage_4_flag,
        CAST(f.question_started_flag = 1
             AND f.question_finished_state_flag = 1
             AND f.ping_received_after_question_start_flag = 1
             AND f.ping_read_final_after_question_start_flag = 1 AS UNSIGNED)
            AS cumulative_stage_5_flag,
        CAST(f.question_started_flag = 1
             AND f.question_finished_state_flag = 1
             AND f.ping_received_after_question_start_flag = 1
             AND f.ping_read_final_after_question_start_flag = 1
             AND f.ping_answered_final_after_question_start_flag = 1 AS UNSIGNED)
            AS cumulative_stage_6_flag,
        CAST(f.question_started_flag = 1
             AND f.question_finished_state_flag = 1
             AND f.ping_received_after_question_start_flag = 1
             AND f.ping_read_final_after_question_start_flag = 1
             AND f.ping_answered_final_after_question_start_flag = 1
             AND f.payment_success_after_ping_flag = 1 AS UNSIGNED)
            AS cumulative_stage_7_flag
    FROM state_flags f
)

SELECT
    @mart_built_at AS mart_built_at,
    DATE(@cohort_start_at) AS cohort_start_date,
    DATE(@feature_end_exclusive - INTERVAL 1 SECOND) AS feature_end_date,
    DATE(@payment_source_start_at) AS payment_source_start_date,
    c.*,
    CASE
        WHEN c.cumulative_stage_7_flag = 1 THEN 7
        WHEN c.cumulative_stage_6_flag = 1 THEN 6
        WHEN c.cumulative_stage_5_flag = 1 THEN 5
        WHEN c.cumulative_stage_4_flag = 1 THEN 4
        WHEN c.cumulative_stage_3_flag = 1 THEN 3
        WHEN c.cumulative_stage_2_flag = 1 THEN 2
        ELSE 1
    END AS max_cumulative_stage
FROM cumulative_flags c;

/* ============================================================================
   2. INDEXES
============================================================================ */

ALTER TABLE votes_mart.mart_user_journey_funnel_1y_build
    ADD PRIMARY KEY (user_id),
    ADD INDEX idx_journey_signup_date (signup_date),
    ADD INDEX idx_journey_school (school_id),
    ADD INDEX idx_journey_grade (grade),
    ADD INDEX idx_journey_ban_status (ban_status),
    ADD INDEX idx_journey_stage (max_cumulative_stage),
    ADD INDEX idx_journey_payment (payment_success_count);

/* ============================================================================
   3. QA
============================================================================ */

-- 3-A. Grain / cohort boundary.
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT user_id) AS unique_user_count,
    SUM(user_id IS NULL) AS null_user_id_count,
    COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_user_count,
    MIN(signup_at) AS min_signup_at,
    MAX(signup_at) AS max_signup_at,
    MIN(followup_days_available) AS min_followup_days,
    MAX(followup_days_available) AS max_followup_days
FROM votes_mart.mart_user_journey_funnel_1y_build;

-- 3-B. Count reconciliation. 모든 오류가 0이어야 한다.
SELECT
    SUM(finished_question_set_count > question_set_count)
        AS invalid_question_count_users,
    SUM(ping_read_final_count > ping_received_count)
        AS invalid_ping_read_count_users,
    SUM(ping_answered_final_count > ping_received_count)
        AS invalid_ping_answer_count_users,
    SUM(ping_read_final_after_question_start_count
        > ping_received_after_question_start_count)
        AS invalid_ping_after_start_read_users,
    SUM(ping_answered_final_after_question_start_count
        > ping_received_after_question_start_count)
        AS invalid_ping_after_start_answer_users,
    SUM(payment_success_count + payment_fail_count <> payment_attempt_count)
        AS invalid_payment_reconciliation_users,
    SUM(payment_success_after_ping_count > payment_success_count)
        AS invalid_payment_after_ping_users
FROM votes_mart.mart_user_journey_funnel_1y_build;

-- 3-C. Timestamp sanity. 모든 오류가 0이어야 한다.
SELECT
    SUM(first_question_set_at < signup_at)
        AS question_before_signup_users,
    SUM(first_ping_received_at < signup_at)
        AS ping_before_signup_users,
    SUM(first_ping_received_after_question_start_at < first_question_set_at)
        AS ping_before_question_start_users,
    SUM(first_payment_success_at < signup_at)
        AS payment_before_signup_users,
    SUM(first_payment_success_after_ping_at
        < first_ping_received_after_question_start_at)
        AS payment_before_ping_users,
    SUM(first_question_set_at >= TIMESTAMP('2024-05-07 00:00:00'))
        AS question_after_window_users,
    SUM(first_ping_received_at >= TIMESTAMP('2024-05-07 00:00:00'))
        AS ping_after_window_users,
    SUM(first_payment_success_at >= TIMESTAMP('2024-05-07 00:00:00'))
        AS payment_after_window_users
FROM votes_mart.mart_user_journey_funnel_1y_build;

-- 3-D. Cumulative stage monotonicity. 모든 오류가 0이어야 한다.
SELECT
    SUM(cumulative_stage_2_flag > cumulative_stage_1_flag)
        AS stage_2_violation,
    SUM(cumulative_stage_3_flag > cumulative_stage_2_flag)
        AS stage_3_violation,
    SUM(cumulative_stage_4_flag > cumulative_stage_3_flag)
        AS stage_4_violation,
    SUM(cumulative_stage_5_flag > cumulative_stage_4_flag)
        AS stage_5_violation,
    SUM(cumulative_stage_6_flag > cumulative_stage_5_flag)
        AS stage_6_violation,
    SUM(cumulative_stage_7_flag > cumulative_stage_6_flag)
        AS stage_7_violation
FROM votes_mart.mart_user_journey_funnel_1y_build;

-- 3-E. 약 1년 누적 사용자 퍼널.
WITH stage_counts AS (
    SELECT 1 AS stage_no, 'SIGNUP_COMPLETED' AS stage_name,
           COUNT(*) AS reached_user_count
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 2, 'QUESTION_SET_STARTED', SUM(cumulative_stage_2_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 3, 'QUESTION_SET_FINISHED_STATE', SUM(cumulative_stage_3_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 4, 'PING_RECEIVED_AFTER_QUESTION_START', SUM(cumulative_stage_4_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 5, 'PING_READ_FINAL_STATE', SUM(cumulative_stage_5_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 6, 'PING_ANSWERED_FINAL_STATE', SUM(cumulative_stage_6_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
    UNION ALL
    SELECT 7, 'PAYMENT_SUCCESS_AFTER_PING_RECEIPT', SUM(cumulative_stage_7_flag)
    FROM votes_mart.mart_user_journey_funnel_1y_build
),
with_previous AS (
    SELECT
        stage_no,
        stage_name,
        reached_user_count,
        LAG(reached_user_count) OVER (ORDER BY stage_no)
            AS previous_stage_user_count,
        FIRST_VALUE(reached_user_count) OVER (ORDER BY stage_no)
            AS base_user_count
    FROM stage_counts
)
SELECT
    stage_no,
    stage_name,
    reached_user_count,
    previous_stage_user_count,
    ROUND(100.0 * reached_user_count / NULLIF(base_user_count, 0), 2)
        AS conversion_from_signup_pct,
    CASE
        WHEN stage_no = 1 THEN 100.00
        ELSE ROUND(100.0 * reached_user_count
                   / NULLIF(previous_stage_user_count, 0), 2)
    END AS conversion_from_previous_stage_pct
FROM with_previous
ORDER BY stage_no;

-- 3-F. 가입 월별 관찰기간과 단계 도달률.
-- 신규 가입자일수록 관찰기간이 짧다는 우측 절단 문제를 확인하기 위한 표다.
SELECT
    DATE_FORMAT(signup_date, '%Y-%m') AS signup_month,
    COUNT(*) AS signup_user_count,
    ROUND(AVG(followup_days_available), 1) AS avg_followup_days,
    ROUND(100.0 * SUM(cumulative_stage_2_flag) / COUNT(*), 2)
        AS question_started_rate_pct,
    ROUND(100.0 * SUM(cumulative_stage_3_flag) / COUNT(*), 2)
        AS question_finished_state_rate_pct,
    ROUND(100.0 * SUM(cumulative_stage_4_flag) / COUNT(*), 2)
        AS ping_received_rate_pct,
    ROUND(100.0 * SUM(cumulative_stage_5_flag) / COUNT(*), 2)
        AS ping_read_final_rate_pct,
    ROUND(100.0 * SUM(cumulative_stage_6_flag) / COUNT(*), 2)
        AS ping_answered_final_rate_pct,
    ROUND(100.0 * SUM(cumulative_stage_7_flag) / COUNT(*), 2)
        AS payment_after_ping_rate_pct
FROM votes_mart.mart_user_journey_funnel_1y_build
GROUP BY DATE_FORMAT(signup_date, '%Y-%m')
ORDER BY signup_month;

-- 3-G. 완료·열람·답변 시각을 오해하지 않기 위한 원천 재확인.
SELECT
    COUNT(*) AS uqr_count,
    SUM(answer_status = 'N') AS unanswered_count,
    SUM(answer_status = 'N' AND answer_updated_at IS NOT NULL)
        AS unanswered_with_update_time_count,
    SUM(answer_updated_at < created_at) AS updated_before_created_count
FROM final.accounts_userquestionrecord;

-- 3-H. 질문 세트 JSON 보존 상태.
SELECT
    COUNT(*) AS question_set_count,
    SUM(JSON_VALID(question_piece_id_list) = 0) AS invalid_json_count,
    MIN(JSON_LENGTH(question_piece_id_list)) AS min_piece_count,
    MAX(JSON_LENGTH(question_piece_id_list)) AS max_piece_count
FROM final.polls_questionset
WHERE created_at >= @cohort_start_at
  AND created_at < @feature_end_exclusive;

-- 3-I. FINAL RELEASE GATE. release_gate_status가 PASS여야 한다.
WITH qa AS (
    SELECT
        COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_user_count,
        SUM(user_id IS NULL) AS null_user_count,
        SUM(signup_at < TIMESTAMP('2023-04-28 00:00:00')
            OR signup_at >= TIMESTAMP('2024-05-07 00:00:00'))
            AS out_of_cohort_user_count,
        SUM(finished_question_set_count > question_set_count)
            AS invalid_question_count,
        SUM(ping_read_final_count > ping_received_count
            OR ping_answered_final_count > ping_received_count)
            AS invalid_ping_count,
        SUM(payment_success_count + payment_fail_count <> payment_attempt_count)
            AS invalid_payment_count,
        SUM(first_question_set_at < signup_at
            OR first_ping_received_at < signup_at
            OR first_payment_success_at < signup_at)
            AS invalid_time_count,
        SUM(cumulative_stage_2_flag > cumulative_stage_1_flag
            OR cumulative_stage_3_flag > cumulative_stage_2_flag
            OR cumulative_stage_4_flag > cumulative_stage_3_flag
            OR cumulative_stage_5_flag > cumulative_stage_4_flag
            OR cumulative_stage_6_flag > cumulative_stage_5_flag
            OR cumulative_stage_7_flag > cumulative_stage_6_flag)
            AS cumulative_stage_violation_count
    FROM votes_mart.mart_user_journey_funnel_1y_build
)
SELECT
    qa.*,
    CASE
        WHEN duplicate_user_count = 0
         AND null_user_count = 0
         AND out_of_cohort_user_count = 0
         AND invalid_question_count = 0
         AND invalid_ping_count = 0
         AND invalid_payment_count = 0
         AND invalid_time_count = 0
         AND cumulative_stage_violation_count = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS release_gate_status
FROM qa;

/* ============================================================================
   PUBLISH는 별도 sns_user_journey_funnel_1y_publish_after_pass.sql에서 수행한다.
============================================================================ */
