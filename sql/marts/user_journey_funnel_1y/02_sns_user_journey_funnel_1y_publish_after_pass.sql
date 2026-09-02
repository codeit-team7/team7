/* ============================================================================
   SNS 1-YEAR USER JOURNEY FUNNEL: PUBLISH AFTER QA PASS
   build_and_qa의 release_gate_status가 PASS일 때만 실행한다.
============================================================================ */

SET NAMES utf8mb4;

-- 후보 테이블 grain을 마지막으로 재확인한다.
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT user_id) AS unique_user_count,
    SUM(user_id IS NULL) AS null_user_count,
    COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_user_count
FROM votes_mart.mart_user_journey_funnel_1y_build;

-- 기존 공개 테이블이 있다면 후보 테이블로 교체한다.
DROP TABLE IF EXISTS votes_mart.mart_user_journey_funnel_1y;

RENAME TABLE votes_mart.mart_user_journey_funnel_1y_build
    TO votes_mart.mart_user_journey_funnel_1y;

-- 약 1년 누적 사용자 퍼널 요약표.
DROP TABLE IF EXISTS votes_mart.mart_user_journey_funnel_1y_summary;

CREATE TABLE votes_mart.mart_user_journey_funnel_1y_summary AS
WITH stage_counts AS (
    SELECT 1 AS stage_no, 'SIGNUP_COMPLETED' AS stage_name,
           COUNT(*) AS reached_user_count
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 2, 'QUESTION_SET_STARTED', SUM(cumulative_stage_2_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 3, 'QUESTION_SET_FINISHED_STATE', SUM(cumulative_stage_3_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 4, 'PING_RECEIVED_AFTER_QUESTION_START', SUM(cumulative_stage_4_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 5, 'PING_READ_FINAL_STATE', SUM(cumulative_stage_5_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 6, 'PING_ANSWERED_FINAL_STATE', SUM(cumulative_stage_6_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
    UNION ALL
    SELECT 7, 'PAYMENT_SUCCESS_AFTER_PING_RECEIPT', SUM(cumulative_stage_7_flag)
    FROM votes_mart.mart_user_journey_funnel_1y
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
FROM with_previous;

ALTER TABLE votes_mart.mart_user_journey_funnel_1y_summary
    ADD PRIMARY KEY (stage_no);

-- 공개 후 최종 확인.
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT user_id) AS unique_user_count,
    COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_user_count,
    MIN(signup_date) AS min_signup_date,
    MAX(signup_date) AS max_signup_date
FROM votes_mart.mart_user_journey_funnel_1y;

SELECT *
FROM votes_mart.mart_user_journey_funnel_1y_summary
ORDER BY stage_no;
